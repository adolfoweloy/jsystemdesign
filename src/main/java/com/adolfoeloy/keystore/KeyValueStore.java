package com.adolfoeloy.keystore;

import com.adolfoeloy.keystore.consistenthash.ConsistentHashingKeyValueStore;
import com.adolfoeloy.keystore.rehashing.RehashingKeyValueStore;

public interface KeyValueStore extends KeyValueStoreClient, KeyValueStoreManager {

    static KeyValueStore rehashing(String...nodes) {
        return new RehashingKeyValueStore(nodes);
    }

    static KeyValueStore consistentHashing(String...nodes) {
        return new ConsistentHashingKeyValueStore(nodes);
    }
}
