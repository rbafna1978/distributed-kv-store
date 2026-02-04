package com.raftdb.util;

import org.slf4j.LoggerFactory;

public class Logger {
    private final org.slf4j.Logger logger;
    private final String nodeId;

    public Logger(Class<?> clazz, String nodeId) {
        this.logger = LoggerFactory.getLogger(clazz);
        this.nodeId = nodeId;
    }

    public void info(String format, Object... args) {
        logger.info("[" + nodeId + "] " + format, args);
    }

    public void warn(String format, Object... args) {
        logger.warn("[" + nodeId + "] " + format, args);
    }

    public void error(String format, Object... args) {
        logger.error("[" + nodeId + "] " + format, args);
    }

    public void debug(String format, Object... args) {
        logger.debug("[" + nodeId + "] " + format, args);
    }
}
