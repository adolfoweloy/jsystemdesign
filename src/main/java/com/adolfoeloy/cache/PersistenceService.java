package com.adolfoeloy.cache;

public class PersistenceService {

    private final Cache caching = new Cache();
    private final Database database = new Database();

    /**
     * Write to both the cache and to the database.
     * Here I'm not worried about failing to write in the database while maintaining invalid entries in the cache.
     * In real life this should be properly addressed.
     */
    public void writeThrough(String key, String value) {
        caching.write(key, value);
        database.write(key, value);
    }

    public String read(String key) {
        // lazy loading from the cache in case of cache-misses
        var result = caching.read(key);

        if (result == null) {
            caching.write(key, database.read(key));
        }

        return caching.read(key);
    }
}
