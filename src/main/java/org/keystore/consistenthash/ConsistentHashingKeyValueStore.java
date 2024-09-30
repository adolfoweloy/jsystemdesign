package org.keystore.consistenthash;

import org.keystore.KeyValueStore;
import org.keystore.NodeServer;

import java.util.List;

public class ConsistentHashingKeyValueStore implements KeyValueStore {
    public ConsistentHashingKeyValueStore(String... nodes) {
    }

    @Override
    public void put(String key, String value) {

    }

    @Override
    public String get(String key) {
        return "";
    }

    @Override
    public void addNode(String newNode) {

    }

    @Override
    public void removeNode(String nodeToRemove) {

    }

    @Override
    public List<NodeServer> nodeServers() {
        return List.of();
    }
}
