package org.keystore;

public interface KeyValueStoreClient {

    void put(String key, String value);

    String get(String key);

}
