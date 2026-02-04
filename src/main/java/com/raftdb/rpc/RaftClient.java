package com.raftdb.rpc;

import com.raftdb.config.Config;
import com.raftdb.proto.RaftServiceGrpc;
import com.raftdb.proto.VoteRequest;
import com.raftdb.proto.VoteResponse;
import com.raftdb.proto.AppendEntriesRequest;
import com.raftdb.proto.AppendEntriesResponse;
import com.raftdb.util.Logger;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

public class RaftClient {
    private final Config config;
    private final Logger logger;
    private final Map<String, ManagedChannel> channels = new ConcurrentHashMap<>();
    private final Map<String, RaftServiceGrpc.RaftServiceStub> stubs = new ConcurrentHashMap<>();

    public RaftClient(Config config) {
        this.config = config;
        this.logger = new Logger(RaftClient.class, config.getNodeId());
        initializeChannels();
    }

    private void initializeChannels() {
        for (String peerId : config.getPeerIds()) {
            int port = config.getPeerPort(peerId);
            String hostname = peerId;
            try {
                java.net.InetAddress.getByName(hostname);
            } catch (java.net.UnknownHostException e) {
                hostname = "127.0.0.1";
            }
            logger.info("Creating channel to " + peerId + " at " + hostname + ":" + port);
            ManagedChannel channel = io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder
                    .forAddress(new java.net.InetSocketAddress(hostname, port))
                    .usePlaintext()
                    .build();
            channels.put(peerId, channel);
            stubs.put(peerId, RaftServiceGrpc.newStub(channel));
        }
    }

    public void requestVote(String peerId, VoteRequest request, StreamObserver<VoteResponse> observer) {
        RaftServiceGrpc.RaftServiceStub stub = stubs.get(peerId);
        if (stub != null) {
            stub.requestVote(request, observer);
        } else {
            logger.error("No connection to peer: " + peerId);
            observer.onError(new RuntimeException("No connection to peer"));
        }
    }

    public void appendEntries(String peerId, AppendEntriesRequest request, StreamObserver<AppendEntriesResponse> observer) {
        RaftServiceGrpc.RaftServiceStub stub = stubs.get(peerId);
        if (stub != null) {
            stub.appendEntries(request, observer);
        } else {
            logger.error("No connection to peer: " + peerId);
            observer.onError(new RuntimeException("No connection to peer"));
        }
    }

    public void shutdown() {
        for (ManagedChannel channel : channels.values()) {
            try {
                channel.shutdown().awaitTermination(1, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }
}
