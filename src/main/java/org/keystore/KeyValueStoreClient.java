package org.keystore;

/**
 * Ideally the client of this key-value store should rely on put and get methods.
 * This interface expresses what is exposed for clients, however support for client creation would be required.
 */
public interface KeyValueStoreClient {

    void put(String key, String value);

    String get(String key);

}
