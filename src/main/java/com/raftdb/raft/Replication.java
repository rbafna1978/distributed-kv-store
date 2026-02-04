package com.raftdb.raft;

import com.raftdb.proto.AppendEntriesRequest;
import com.raftdb.proto.AppendEntriesResponse;
import com.raftdb.proto.LogEntry;
import io.grpc.stub.StreamObserver;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Replication {
    private final RaftNode node;
    private final Map<String, Long> nextIndex = new HashMap<>();
    private final Map<String, Long> matchIndex = new HashMap<>();

    public Replication(RaftNode node) {
        this.node = node;
    }

    public void startLeader() {
        // Assume lock held
        long lastLogIndex = node.getLastLogIndex();
        for (String peerId : node.getConfig().getPeerIds()) {
            nextIndex.put(peerId, lastLogIndex + 1);
            matchIndex.put(peerId, 0L);
        }
    }

    public void sendHeartbeats() {
        // Send to all peers
        for (String peerId : node.getConfig().getPeerIds()) {
            sendAppendEntries(peerId);
        }
    }

    private void sendAppendEntries(String peerId) {
        // Must be called with lock or handle concurrency. 
        // Since this is called from scheduled executor inside lock in RaftNode, it's safe.
        // Wait, the StreamObserver callback is async, so it needs to lock.
        
        long term = node.getCurrentTerm();
        String leaderId = node.getConfig().getNodeId();
        
        long prevLogIndex = nextIndex.getOrDefault(peerId, 1L) - 1;
        // Safety check
        if (prevLogIndex < 0) prevLogIndex = 0;
        
        LogEntry prevEntry = node.getLogEntry((int)prevLogIndex);
        long prevLogTerm = (prevEntry != null) ? prevEntry.getTerm() : 0;
        
        List<LogEntry> entries;
        long lastIndex = node.getLastLogIndex();
        
        if (lastIndex >= nextIndex.get(peerId)) {
            entries = node.getLogEntries((int)(long)nextIndex.get(peerId));
        } else {
            entries = Collections.emptyList();
        }

        long leaderCommit = node.getCommitIndex();

        AppendEntriesRequest request = AppendEntriesRequest.newBuilder()
                .setTerm(term)
                .setLeaderId(leaderId)
                .setPrevLogIndex(prevLogIndex)
                .setPrevLogTerm(prevLogTerm)
                .addAllEntries(entries)
                .setLeaderCommit(leaderCommit)
                .build();

        node.getRaftClient().appendEntries(peerId, request, new StreamObserver<AppendEntriesResponse>() {
            @Override
            public void onNext(AppendEntriesResponse response) {
                node.lock.lock();
                try {
                    if (node.getState() != RaftState.LEADER || node.getCurrentTerm() != request.getTerm()) {
                        return;
                    }

                    if (response.getTerm() > node.getCurrentTerm()) {
                        node.convertToFollower(response.getTerm());
                        return;
                    }

                    if (response.getSuccess()) {
                        // Update matchIndex and nextIndex
                        long newMatchIndex = request.getPrevLogIndex() + request.getEntriesCount();
                        matchIndex.put(peerId, newMatchIndex);
                        nextIndex.put(peerId, newMatchIndex + 1);
                        
                        updateCommitIndex();
                    } else {
                        // Failed due to log inconsistency, decrement nextIndex and retry
                        long currentNext = nextIndex.get(peerId);
                        // Optimization: jump back to matchIndex from response (if implemented) or decrement
                        // Using simple decrement for now
                         if (response.getMatchIndex() > 0) {
                             nextIndex.put(peerId, response.getMatchIndex() + 1); // Not standard, but some impls do this
                         } else {
                             nextIndex.put(peerId, Math.max(1, currentNext - 1));
                         }
                         // Retry immediately? Or wait for next heartbeat?
                         // Waiting for next heartbeat is safer to avoid storms.
                    }
                } finally {
                    node.lock.unlock();
                }
            }

            @Override
            public void onError(Throwable t) {
                // Log and ignore
            }

            @Override
            public void onCompleted() {
            }
        });
    }

    private void updateCommitIndex() {
        // If there exists an N such that N > commitIndex, a majority of matchIndex[i] >= N, 
        // and log[N].term == currentTerm: set commitIndex = N
        
        long n = node.getLastLogIndex();
        while (n > node.getCommitIndex()) {
            if (node.getLogEntry((int)n).getTerm() == node.getCurrentTerm()) {
                int count = 1; // Self
                for (Long match : matchIndex.values()) {
                    if (match >= n) count++;
                }
                
                int total = node.getConfig().getPeerIds().size() + 1;
                if (count > total / 2) {
                    node.setCommitIndex(n);
                    break;
                }
            }
            n--;
        }
    }
}
