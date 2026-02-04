package com.raftdb.raft;

import com.raftdb.proto.LogEntry;

public class LogEntryWrapper {
    private final LogEntry entry;

    public LogEntryWrapper(LogEntry entry) {
        this.entry = entry;
    }

    public LogEntry getEntry() {
        return entry;
    }

    public long getTerm() {
        return entry.getTerm();
    }

    public long getIndex() {
        return entry.getIndex();
    }
}
