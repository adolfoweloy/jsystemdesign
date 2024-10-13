package com.adolfoeloy.cache;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class PersistenceService {

    private final Cache caching = new Cache();
    private final Database database = new Database();
    private final ExecutorService executorService = Executors.newSingleThreadExecutor();

    /**
     * Write to both the cache and to the database.
     * Here I'm not worried about failing to write in the database while maintaining invalid entries in the cache.
     * In real life this should be properly addressed.
     */
    public void writeThrough(String key, String value) {
        caching.write(key, value);
        database.write(key, value);
    }

    /**
     * Write directly and only to the database. When using this strategy, the cache is usually updated
     * on cache-misses when reading from the cache.
     * @see this.read
     */
    public void writeAround(String key, String value) {
        database.write(key, value);
    }

    /**
     * Write first to the cache and asynchronously to the database.
     */
    public void writeBack(String key, String value) {
        caching.write(key, value);

        // writes to the database is performed asynchronously unblocking the write operation as a whole
        executorService.submit(() -> {
            database.write(key, value);
        });
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
