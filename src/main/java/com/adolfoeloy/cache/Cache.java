package com.adolfoeloy.cache;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class Cache {
    private final Map<String, String> map = new ConcurrentHashMap<>();

    public void write(String key, String value) {
        map.put(key, value);
    }

    public String read(String key) {
        return map.get(key);
    }
}
