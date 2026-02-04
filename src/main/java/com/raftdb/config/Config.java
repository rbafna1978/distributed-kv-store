package com.raftdb.config;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Config {
    // Static configuration for the 5-node cluster demo
    public static final Map<String, Integer> CLUSTER = new HashMap<>();
    static {
        CLUSTER.put("node1", 8001);
        CLUSTER.put("node2", 8002);
        CLUSTER.put("node3", 8003);
        CLUSTER.put("node4", 8004);
        CLUSTER.put("node5", 8005);
    }

    private final String nodeId;
    private final int port;
    private final List<String> peerIds;

    public Config(String nodeId) {
        if (!CLUSTER.containsKey(nodeId)) {
            throw new IllegalArgumentException("Unknown node: " + nodeId);
        }
        this.nodeId = nodeId;
        this.port = CLUSTER.get(nodeId);
        this.peerIds = new ArrayList<>(CLUSTER.keySet());
        this.peerIds.remove(nodeId);
    }

    public String getNodeId() {
        return nodeId;
    }

    public int getPort() {
        return port;
    }

    public List<String> getPeerIds() {
        return peerIds;
    }

    public int getPeerPort(String peerId) {
        return CLUSTER.get(peerId);
    }
}
