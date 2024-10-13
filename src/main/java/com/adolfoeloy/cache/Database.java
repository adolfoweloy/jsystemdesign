package com.adolfoeloy.cache;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Since writing and reading from the database are slower than from cache, a delay is added to each operation.
 */
public class Database {
    private final Map<String, String> map = new ConcurrentHashMap<>();

    public void write(String key, String value) {
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        map.put(key, value);
    }

    public String read(String key) {
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        return map.get(key);
    }
}
