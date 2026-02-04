package com.raftdb.raft;

import com.raftdb.config.Config;
import com.raftdb.proto.*;
import com.raftdb.rpc.RaftClient;
import com.raftdb.storage.KVStore;
import com.raftdb.storage.PersistentState;
import com.raftdb.util.Logger;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.locks.ReentrantLock;

public class RaftNode {
    private final Config config;
    private final KVStore kvStore;
    private final RaftClient raftClient;
    private final Logger logger;
    public final ReentrantLock lock = new ReentrantLock();

    // Persistent state
    private final PersistentState persistentState;

    // Volatile state
    private long commitIndex = 0;
    private long lastApplied = 0;
    private RaftState state = RaftState.FOLLOWER;
    private String leaderId = null;

    // Client Pending Requests (Index -> Future)
    private final Map<Long, CompletableFuture<Boolean>> pendingRequests = new ConcurrentHashMap<>();

    // Components
    private final Election election;
    private final Replication replication;

    // Timers
    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(2);
    private ScheduledFuture<?> electionTimer;
    private ScheduledFuture<?> heartbeatTimer;

    public RaftNode(Config config, KVStore kvStore, RaftClient raftClient) {
        this.config = config;
        this.kvStore = kvStore;
        this.raftClient = raftClient;
        this.logger = new Logger(RaftNode.class, config.getNodeId());
        this.persistentState = new PersistentState();
        
        this.election = new Election(this);
        this.replication = new Replication(this);
    }

    public void start() {
        logger.info("Starting RaftNode...");
        resetElectionTimer();
    }

    // --- State Accessors ---
    public long getCurrentTerm() { return persistentState.getCurrentTerm(); }
    public void setCurrentTerm(long term) { persistentState.setCurrentTerm(term); }
    public String getVotedFor() { return persistentState.getVotedFor(); }
    public RaftState getState() { return state; }
    public String getLeaderId() { return leaderId; }
    public Config getConfig() { return config; }
    public RaftClient getRaftClient() { return raftClient; }
    public Logger getLogger() { return logger; }
    public KVStore getKvStore() { return kvStore; }

    // --- Log Access ---
    public LogEntry getLogEntry(int index) {
        synchronized (persistentState) {
            if (index < 0 || index >= persistentState.getLog().size()) return null;
            return persistentState.getLog().get(index);
        }
    }

    public List<LogEntry> getLogEntries(int fromIndex) {
        synchronized (persistentState) {
            if (fromIndex >= persistentState.getLog().size()) return Collections.emptyList();
            return new ArrayList<>(persistentState.getLog().subList(fromIndex, persistentState.getLog().size()));
        }
    }

    public long getLastLogIndex() { return persistentState.getLastLogIndex(); }
    public long getLastLogTerm() { return persistentState.getLastLogTerm(); }
    public long getCommitIndex() { return commitIndex; }

    public void setCommitIndex(long index) {
        this.commitIndex = index;
        applyLog();
    }

    // --- RPC Handlers ---

    public VoteResponse handleRequestVote(VoteRequest request) {
        lock.lock();
        try {
            if (request.getTerm() > getCurrentTerm()) {
                convertToFollower(request.getTerm());
            }

            boolean voteGranted = false;
            if (request.getTerm() >= getCurrentTerm()) {
                String currentVotedFor = getVotedFor();
                if ((currentVotedFor == null || currentVotedFor.equals(request.getCandidateId())) &&
                    isLogUpToDate(request.getLastLogIndex(), request.getLastLogTerm())) {
                    voteGranted = true;
                    persistentState.setVotedFor(request.getCandidateId());
                    resetElectionTimer();
                }
            }
            
            logger.info("Vote request from " + request.getCandidateId() + " term " + request.getTerm() + 
                        " (myTerm: " + getCurrentTerm() + ") -> " + (voteGranted ? "GRANTED" : "DENIED"));

            return VoteResponse.newBuilder()
                    .setTerm(getCurrentTerm())
                    .setVoteGranted(voteGranted)
                    .build();
        } finally {
            lock.unlock();
        }
    }

    private boolean isLogUpToDate(long candidateLastIdx, long candidateLastTerm) {
        long myLastTerm = getLastLogTerm();
        long myLastIdx = getLastLogIndex();
        
        if (candidateLastTerm != myLastTerm) {
            return candidateLastTerm > myLastTerm;
        }
        return candidateLastIdx >= myLastIdx;
    }

    public AppendEntriesResponse handleAppendEntries(AppendEntriesRequest request) {
        lock.lock();
        try {
            if (request.getTerm() > getCurrentTerm()) {
                convertToFollower(request.getTerm());
            }

            AppendEntriesResponse.Builder response = AppendEntriesResponse.newBuilder();
            response.setTerm(currentTerm);

            if (request.getTerm() < getCurrentTerm()) {
                return response.setSuccess(false).build();
            }

            // Valid leader found
            if (state != RaftState.FOLLOWER) {
                convertToFollower(request.getTerm());
            }
            leaderId = request.getLeaderId();
            resetElectionTimer();

            // Log consistency check
            LogEntry prevEntry = getLogEntry((int)request.getPrevLogIndex());
            if (prevEntry == null || prevEntry.getTerm() != request.getPrevLogTerm()) {
                return response.setSuccess(false).setMatchIndex(getLastLogIndex()).build(); 
            }

            // Append new entries
            synchronized (persistentState) {
                List<LogEntry> currentLog = persistentState.getLog();
                int index = (int)request.getPrevLogIndex();
                for (LogEntry entry : request.getEntriesList()) {
                    index++;
                    if (index < currentLog.size()) {
                        if (currentLog.get(index).getTerm() != entry.getTerm()) {
                            // Conflict, remove this and all following
                            while (currentLog.size() > index) {
                                currentLog.remove(currentLog.size() - 1);
                            }
                            currentLog.add(entry);
                        }
                    } else {
                        currentLog.add(entry);
                    }
                }
            }

            // Update commit index
            if (request.getLeaderCommit() > commitIndex) {
                setCommitIndex(Math.min(request.getLeaderCommit(), getLastLogIndex()));
            }

            return response.setSuccess(true).setMatchIndex(getLastLogIndex()).build();

        } finally {
            lock.unlock();
        }
    }

    // --- Client Operations ---

    public CompletableFuture<Boolean> propose(Command command) {
        CompletableFuture<Boolean> future = new CompletableFuture<>();
        lock.lock();
        try {
            if (state != RaftState.LEADER) {
                future.completeExceptionally(new RuntimeException("Not Leader"));
                return future;
            }

            long index = getLastLogIndex() + 1;
            LogEntry entry = LogEntry.newBuilder()
                    .setTerm(getCurrentTerm())
                    .setIndex(index)
                    .setCommand(command)
                    .build();
            
            synchronized (persistentState) {
                persistentState.getLog().add(entry);
            }
            
            pendingRequests.put(index, future);
        } finally {
            lock.unlock();
        }
        return future;
    }

    // --- State Transitions ---

    public void convertToFollower(long term) {
        long oldTerm = getCurrentTerm();
        state = RaftState.FOLLOWER;
        setCurrentTerm(term);
        persistentState.setVotedFor(null);
        leaderId = null;
        if (oldTerm != term) logger.info("Converting to FOLLOWER. Term: " + oldTerm + " -> " + term);
        
        if (heartbeatTimer != null) heartbeatTimer.cancel(false);
        resetElectionTimer();
    }

    public void convertToCandidate() {
        lock.lock();
        try {
            state = RaftState.CANDIDATE;
            setCurrentTerm(getCurrentTerm() + 1);
            persistentState.setVotedFor(config.getNodeId());
            leaderId = null;
            logger.info("Converting to CANDIDATE. New Term: " + getCurrentTerm());
            
            election.startElection();
            resetElectionTimer();
        } finally {
            lock.unlock();
        }
    }

    public void convertToLeader() {
        if (state != RaftState.CANDIDATE) return;
        
        state = RaftState.LEADER;
        leaderId = config.getNodeId();
        logger.info("Became LEADER for term " + getCurrentTerm());
        
        if (electionTimer != null) electionTimer.cancel(false);
        
        replication.startLeader();
        startHeartbeatTimer();
    }

    // --- Timers ---

    public void resetElectionTimer() {
        if (state == RaftState.LEADER) return;

        if (electionTimer != null && !electionTimer.isDone()) {
            electionTimer.cancel(false);
        }
        
        long timeout = 500 + (long)(Math.random() * 500); 
        electionTimer = scheduler.schedule(this::handleElectionTimeout, timeout, TimeUnit.MILLISECONDS);
    }
    
    private void handleElectionTimeout() {
        lock.lock();
        try {
            if (state != RaftState.LEADER) {
                logger.info("Election timeout. Starting new election.");
                convertToCandidate();
            }
        } finally {
            lock.unlock();
        }
    }

    private void startHeartbeatTimer() {
        if (heartbeatTimer != null && !heartbeatTimer.isDone()) {
            heartbeatTimer.cancel(false);
        }
        heartbeatTimer = scheduler.scheduleAtFixedRate(() -> {
            lock.lock();
            try {
                if (state == RaftState.LEADER) {
                    replication.sendHeartbeats();
                } else {
                    heartbeatTimer.cancel(false);
                }
            } finally {
                lock.unlock();
            }
        }, 0, 50, TimeUnit.MILLISECONDS);
    }

    private void applyLog() {
        while (commitIndex > lastApplied) {
            lastApplied++;
            LogEntry entry = getLogEntry((int)lastApplied);
            if (entry == null) continue;
            
            Command cmd = entry.getCommand();
            if (cmd.getOperation() != Command.Operation.GET) {
                if (cmd.getOperation() == Command.Operation.SET) {
                    kvStore.set(cmd.getKey(), cmd.getValue());
                } else if (cmd.getOperation() == Command.Operation.DELETE) {
                    kvStore.delete(cmd.getKey());
                }
                logger.info("Applied index " + lastApplied + ": " + cmd.getOperation() + " " + cmd.getKey());
            }

            CompletableFuture<Boolean> future = pendingRequests.remove(lastApplied);
            if (future != null) {
                future.complete(true);
            }
        }
    }

    public void stop() {
        scheduler.shutdownNow();
    }
}
