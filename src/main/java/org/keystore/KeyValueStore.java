package org.keystore;

import org.keystore.consistenthash.ConsistentHashingKeyValueStore;
import org.keystore.rehashing.RehashingKeyValueStore;

public interface KeyValueStore extends KeyValueStoreClient, KeyValueStoreManager {

    static KeyValueStore rehashing(String...nodes) {
        return new RehashingKeyValueStore(nodes);
    }

    static KeyValueStore constentHashing(String...nodes) {
        return new ConsistentHashingKeyValueStore(nodes);
    }
}
