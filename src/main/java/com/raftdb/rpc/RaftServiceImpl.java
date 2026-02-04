package com.raftdb.rpc;

import com.raftdb.proto.AppendEntriesRequest;
import com.raftdb.proto.AppendEntriesResponse;
import com.raftdb.proto.RaftServiceGrpc;
import com.raftdb.proto.VoteRequest;
import com.raftdb.proto.VoteResponse;
import com.raftdb.raft.RaftNode;
import io.grpc.stub.StreamObserver;

public class RaftServiceImpl extends RaftServiceGrpc.RaftServiceImplBase {
    private final RaftNode raftNode;

    public RaftServiceImpl(RaftNode raftNode) {
        this.raftNode = raftNode;
    }

    @Override
    public void requestVote(VoteRequest request, StreamObserver<VoteResponse> responseObserver) {
        raftNode.getLogger().info("Received RequestVote from " + request.getCandidateId() + " term " + request.getTerm());
        try {
            VoteResponse response = raftNode.handleRequestVote(request);
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (Exception e) {
            raftNode.getLogger().error("Error handling requestVote: " + e.getMessage(), e);
            responseObserver.onError(io.grpc.Status.INTERNAL.withDescription(e.getMessage()).asRuntimeException());
        }
    }

    @Override
    public void appendEntries(AppendEntriesRequest request, StreamObserver<AppendEntriesResponse> responseObserver) {
        try {
            AppendEntriesResponse response = raftNode.handleAppendEntries(request);
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (Exception e) {
            raftNode.getLogger().error("Error handling appendEntries: " + e.getMessage(), e);
            responseObserver.onError(io.grpc.Status.INTERNAL.withDescription(e.getMessage()).asRuntimeException());
        }
    }
}
