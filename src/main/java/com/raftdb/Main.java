package com.raftdb;

import com.raftdb.config.Config;
import com.raftdb.proto.Command;
import com.raftdb.proto.LogEntry;
import com.raftdb.raft.RaftNode;
import com.raftdb.rpc.ClientServiceImpl;
import com.raftdb.rpc.RaftClient;
import com.raftdb.rpc.RaftServiceImpl;
import com.raftdb.storage.KVStore;
import com.raftdb.util.Logger;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import io.grpc.Server;
import io.grpc.ServerBuilder;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.util.concurrent.CompletableFuture;

public class Main {
    private static Logger logger;

    public static void main(String[] args) throws IOException, InterruptedException {
        if (args.length < 2) {
            System.err.println("Usage: java -jar raft-kv-store.jar <nodeId> <port>");
            System.exit(1);
        }

        String nodeId = args[0];
        // Port arg is ignored in favor of Config.java for peer communication, 
        // but we verify it matches or use it for HTTP.
        // Actually Config has hardcoded ports for cluster members.
        // We will trust Config for gRPC ports.
        
        Config config = new Config(nodeId);
        logger = new Logger(Main.class, nodeId);

        KVStore kvStore = new KVStore();
        RaftClient raftClient = new RaftClient(config);
        RaftNode raftNode = new RaftNode(config, kvStore, raftClient);

        // Start gRPC Server
        Server grpcServer = io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder.forAddress(new java.net.InetSocketAddress("127.0.0.1", config.getPort()))
                .addService(new RaftServiceImpl(raftNode))
                .addService(new ClientServiceImpl(raftNode))
                .build()
                .start();

        logger.info("gRPC Server started on port " + config.getPort());

        // Start Raft Node
        raftNode.start();

        // Start HTTP Server for demo/curl
        // Use port + 1000 for HTTP to avoid conflict with gRPC
        int httpPort = config.getPort() + 1000;
        HttpServer httpServer = HttpServer.create(new InetSocketAddress(httpPort), 0);
        httpServer.createContext("/api/set", new SetHandler(raftNode));
        httpServer.createContext("/api/get", new GetHandler(raftNode));
        httpServer.createContext("/api/delete", new DeleteHandler(raftNode));
        httpServer.setExecutor(java.util.concurrent.Executors.newCachedThreadPool());
        httpServer.start();
        
        logger.info("HTTP API started on port " + httpPort);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Shutting down...");
            raftNode.stop();
            raftClient.shutdown();
            grpcServer.shutdown();
            httpServer.stop(0);
        }));

        grpcServer.awaitTermination();
    }

    static class SetHandler implements HttpHandler {
        private final RaftNode raftNode;
        public SetHandler(RaftNode raftNode) { this.raftNode = raftNode; }

        @Override
        public void handle(HttpExchange exchange) throws IOException {
            if ("POST".equals(exchange.getRequestMethod())) {
                // Parse JSON simply (assuming {"key":"k","value":"v"})
                String body = new String(exchange.getRequestBody().readAllBytes());
                String key = extractJson(body, "key");
                String value = extractJson(body, "value");

                Command cmd = Command.newBuilder()
                        .setOperation(Command.Operation.SET)
                        .setKey(key)
                        .setValue(value)
                        .build();

                handleCommand(exchange, raftNode, cmd);
            } else {
                sendResponse(exchange, 405, "Method Not Allowed");
            }
        }
    }

    static class GetHandler implements HttpHandler {
        private final RaftNode raftNode;
        public GetHandler(RaftNode raftNode) { this.raftNode = raftNode; }

        @Override
        public void handle(HttpExchange exchange) throws IOException {
            if ("GET".equals(exchange.getRequestMethod())) {
                String query = exchange.getRequestURI().getQuery();
                String key = query.split("=")[1];
                
                String value = raftNode.getKvStore().get(key);
                String json = "{\"value\":\"" + (value == null ? "" : value) + "\"}";
                sendResponse(exchange, 200, json);
            } else {
                sendResponse(exchange, 405, "Method Not Allowed");
            }
        }
    }

    static class DeleteHandler implements HttpHandler {
        private final RaftNode raftNode;
        public DeleteHandler(RaftNode raftNode) { this.raftNode = raftNode; }

        @Override
        public void handle(HttpExchange exchange) throws IOException {
            if ("DELETE".equals(exchange.getRequestMethod())) {
                String query = exchange.getRequestURI().getQuery();
                String key = query.split("=")[1];

                Command cmd = Command.newBuilder()
                        .setOperation(Command.Operation.DELETE)
                        .setKey(key)
                        .build();

                handleCommand(exchange, raftNode, cmd);
            } else {
                sendResponse(exchange, 405, "Method Not Allowed");
            }
        }
    }

    private static void handleCommand(HttpExchange exchange, RaftNode raftNode, Command cmd) throws IOException {
        CompletableFuture<Boolean> future = raftNode.propose(cmd);
        future.whenComplete((success, ex) -> {
            try {
                if (ex != null) {
                     // If redirect needed
                     if (ex.getMessage().contains("Not Leader")) {
                         String leader = raftNode.getLeaderId();
                         String msg = "{\"error\":\"Not Leader\", \"leader\":\"" + leader + "\"}";
                         sendResponse(exchange, 503, msg);
                     } else {
                         sendResponse(exchange, 500, "{\"error\":\"" + ex.getMessage() + "\"}");
                     }
                } else {
                    sendResponse(exchange, 200, "{\"status\":\"ok\"}");
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
    }

    private static void sendResponse(HttpExchange exchange, int code, String response) throws IOException {
        exchange.sendResponseHeaders(code, response.length());
        OutputStream os = exchange.getResponseBody();
        os.write(response.getBytes());
        os.close();
    }

    // Very basic JSON extractor
    private static String extractJson(String json, String key) {
        int keyIdx = json.indexOf("\"" + key + "\"");
        if (keyIdx == -1) return "";
        int colonIdx = json.indexOf(":", keyIdx);
        int startQuote = json.indexOf("\"", colonIdx);
        int endQuote = json.indexOf("\"", startQuote + 1);
        return json.substring(startQuote + 1, endQuote);
    }
}
