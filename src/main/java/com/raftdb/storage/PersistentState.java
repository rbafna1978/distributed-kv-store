package com.raftdb.storage;

import com.raftdb.proto.LogEntry;
import java.util.ArrayList;
import java.util.List;

/**
 * According to Raft Figure 2, this state must be persisted to stable storage
 * before responding to RPCs.
 */
public class PersistentState {
    private long currentTerm = 0;
    private String votedFor = null;
    private final List<LogEntry> log = new ArrayList<>();

    public PersistentState() {
        // Initialize log with dummy entry at index 0 as per Raft paper
        log.add(LogEntry.newBuilder().setTerm(0).setIndex(0).build());
    }

    public synchronized long getCurrentTerm() { return currentTerm; }
    public synchronized void setCurrentTerm(long currentTerm) { this.currentTerm = currentTerm; }

    public synchronized String getVotedFor() { return votedFor; }
    public synchronized void setVotedFor(String votedFor) { this.votedFor = votedFor; }

    public synchronized List<LogEntry> getLog() { return log; }
    
    public synchronized long getLastLogIndex() { return log.size() - 1; }
    public synchronized long getLastLogTerm() { return log.get(log.size() - 1).getTerm(); }
}
