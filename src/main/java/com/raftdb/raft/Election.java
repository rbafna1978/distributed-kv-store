package com.raftdb.raft;

import com.raftdb.proto.VoteRequest;
import com.raftdb.proto.VoteResponse;
import io.grpc.stub.StreamObserver;

import java.util.concurrent.atomic.AtomicInteger;

public class Election {
    private final RaftNode node;

    public Election(RaftNode node) {
        this.node = node;
    }

    public void startElection() {
        // Assume lock is held by caller (RaftNode.convertToCandidate)
        
        long term = node.getCurrentTerm();
        String candidateId = node.getConfig().getNodeId();
        long lastLogIndex = node.getLastLogIndex();
        long lastLogTerm = node.getLastLogTerm();

        VoteRequest request = VoteRequest.newBuilder()
                .setTerm(term)
                .setCandidateId(candidateId)
                .setLastLogIndex(lastLogIndex)
                .setLastLogTerm(lastLogTerm)
                .build();

        AtomicInteger votes = new AtomicInteger(1); // Vote for self
        int majority = node.getConfig().getPeerIds().size() / 2 + 1; // Peers + self / 2 + 1? No. Total nodes / 2 + 1.
        // Peers count is Total - 1. So Total = Peers.size() + 1.
        int totalNodes = node.getConfig().getPeerIds().size() + 1;
        int requiredVotes = totalNodes / 2 + 1;

        for (String peerId : node.getConfig().getPeerIds()) {
            node.getRaftClient().requestVote(peerId, request, new StreamObserver<VoteResponse>() {
                @Override
                public void onNext(VoteResponse response) {
                    node.lock.lock();
                    try {
                        if (node.getState() != RaftState.CANDIDATE || node.getCurrentTerm() != request.getTerm()) {
                            return;
                        }

                        if (response.getTerm() > node.getCurrentTerm()) {
                            node.convertToFollower(response.getTerm());
                            return;
                        }

                        if (response.getVoteGranted()) {
                            int currentVotes = votes.incrementAndGet();
                            if (currentVotes >= requiredVotes && node.getState() == RaftState.CANDIDATE) {
                                node.convertToLeader();
                            }
                        }
                    } finally {
                        node.lock.unlock();
                    }
                }

                @Override
                public void onError(Throwable t) {
                    // Log failure, but continue
                    node.getLogger().warn("Failed to request vote from " + peerId + ": " + t.toString() + 
                                          (t.getCause() != null ? " Cause: " + t.getCause().toString() : ""));
                }

                @Override
                public void onCompleted() {
                }
            });
        }
    }
}
