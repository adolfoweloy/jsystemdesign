package org.keystore.rehashing;

import org.keystore.HashNumber;
import org.keystore.KeyValueStore;
import org.keystore.NodeServerWithData;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class RehashingKeyValueStore implements KeyValueStore {
    private final Map<Integer, NodeServerWithData> nodes = new HashMap<>();

    public RehashingKeyValueStore(String... nodes) {
        for (String node : nodes) {
            this.nodes.put(
                this.nodes.size(),
                new NodeServerWithData(node, new HashMap<>())
            );
        }
    }

    @Override
    public void put(String key, String value) {
        var server = getServer(getHash(key));
        server.data().put(key, value);
    }

    @Override
    public String get(String key) {
        var server = getServer(getHash(key));
        return server.data().get(key);
    }

    @Override
    public List<NodeServerWithData> nodeServers() {
        return List.copyOf(nodes.values());
    }

    private HashNumber getHash(String key) {
        return new HashNumber(key.hashCode());
    }

    private NodeServerWithData getServer(HashNumber hash) {
        int index = hash.hash() % nodes.size();
        return nodes.get(index);
    }

    @Override
    public void addNode(String newNode) {
        var allServers = new ArrayList<NodeServerWithData>(nodes.size() + 1);
        var currentServers = nodes.values().stream()
                .map(server -> new NodeServerWithData(server.name(), new HashMap<>()))
                .toList();
        allServers.addAll(currentServers);
        allServers.add(new NodeServerWithData(newNode, new HashMap<>()));

        rehash(allServers);
    }

    @Override
    public void removeNode(String nodeToRemove) {
        var allServers = nodes.values().stream()
                .filter(server -> !server.name().equals(nodeToRemove))
                .map(server -> new NodeServerWithData(server.name(), new HashMap<>()))
                .toList();

        rehash(allServers);
    }

    /**
     * Rehashing affects all key-value pairs in the stores proving to be an inefficient way to
     * distribute data among nodes in a distributed key-value storage system.
     */
    private void rehash(List<NodeServerWithData> allServers) {
        // get all key-value pairs
        var keyValues = getCurrentKeyValues();

        // reset nodes
        nodes.clear();
        for (NodeServerWithData server : allServers) {
            nodes.put(nodes.size(), server);
        }

        // rehash all data
        for (Map.Entry<String, String> entry : keyValues.entrySet()) {
            put(entry.getKey(), entry.getValue());
        }
    }

    private HashMap<String, String> getCurrentKeyValues() {
        var keyValues = new HashMap<String, String>();
        for (NodeServerWithData node:  nodes.values()) {
            Map<String, String> values = node.data();
            keyValues.putAll(values);
        }
        return keyValues;
    }
}
