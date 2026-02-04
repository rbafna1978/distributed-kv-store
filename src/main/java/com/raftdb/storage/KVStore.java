package com.raftdb.storage;

import java.util.concurrent.ConcurrentHashMap;

public class KVStore {
    private final ConcurrentHashMap<String, String> map = new ConcurrentHashMap<>();

    public void set(String key, String value) {
        map.put(key, value);
    }

    public String get(String key) {
        return map.get(key);
    }

    public void delete(String key) {
        map.remove(key);
    }
}
