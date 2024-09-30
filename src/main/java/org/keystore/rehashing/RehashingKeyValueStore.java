package org.keystore.rehashing;

import org.keystore.HashNumber;
import org.keystore.KeyValueStore;
import org.keystore.NodeServer;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class RehashingKeyValueStore implements KeyValueStore {
    private final Map<Integer, NodeServer> nodes = new HashMap<>();

    public RehashingKeyValueStore(String... nodes) {
        for (String node : nodes) {
            this.nodes.put(
                this.nodes.size(),
                new NodeServer(node, new HashMap<>())
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
    public List<NodeServer> nodeServers() {
        return List.copyOf(nodes.values());
    }

    private HashNumber getHash(String key) {
        return new HashNumber(key.hashCode());
    }

    private NodeServer getServer(HashNumber hash) {
        // determine the server to store the value
        int index = hash.hash() % nodes.size();
        return nodes.get(index);
    }

    @Override
    public void addNode(String newNode) {
        var allServers = new ArrayList<NodeServer>(nodes.size() + 1);
        var currentServers = nodes.values().stream()
                .map(server -> new NodeServer(server.name(), new HashMap<>()))
                .toList();
        allServers.addAll(currentServers);
        allServers.add(new NodeServer(newNode, new HashMap<>()));

        // get all key-value pairs
        rehash(allServers);
    }

    @Override
    public void removeNode(String nodeToRemove) {
        var allServers = nodes.values().stream()
                .filter(server -> !server.name().equals(nodeToRemove))
                .map(server -> new NodeServer(server.name(), new HashMap<>()))
                .toList();

        rehash(allServers);
    }

    private void rehash(List<NodeServer> allServers) {
        // get all key-value pairs
        var keyValues = getCurrentKeyValues();

        // reset nodes
        nodes.clear();
        for (NodeServer server : allServers) {
            nodes.put(nodes.size(), server);
        }

        // rehash all data
        for (Map.Entry<String, String> entry : keyValues.entrySet()) {
            put(entry.getKey(), entry.getValue());
        }
    }

    private HashMap<String, String> getCurrentKeyValues() {
        var keyValues = new HashMap<String, String>();
        for (NodeServer node:  nodes.values()) {
            Map<String, String> values = node.data();
            keyValues.putAll(values);
        }
        return keyValues;
    }
}
