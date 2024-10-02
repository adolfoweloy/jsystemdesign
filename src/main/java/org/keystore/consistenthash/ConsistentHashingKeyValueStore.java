package org.keystore.consistenthash;

import com.google.common.hash.Hashing;
import org.keystore.KeyValueStore;
import org.keystore.NodeServerWithData;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import static java.nio.charset.StandardCharsets.UTF_8;

public class ConsistentHashingKeyValueStore implements KeyValueStore {
    private final TreeMap<String, NodeServer> ring = new TreeMap<>();
    private final Map<NodeServer, Map<String, String>> values = new HashMap<>();

    public ConsistentHashingKeyValueStore(String... nodes) {
        // build the ring
        for (String node : nodes) {
            var nodeServer = new NodeServer(node);
            var hash = getHash(node);
            ring.put(hash, nodeServer);
            values.put(nodeServer, new HashMap<>());
        }
    }

    @Override
    public void put(String key, String value) {
        var hash = getHash(key);
        var entry = ring.tailMap(hash).firstEntry();
        if (entry == null) {
            entry = ring.firstEntry();
        }

        var nodeServer = entry.getValue();
        values.get(nodeServer).put(key, value);
    }

    @Override
    public String get(String key) {
        var hash = getHash(key);
        var entry = ring.tailMap(hash).firstEntry();
        if (entry == null) {
            entry = ring.firstEntry();
        }

        var nodeServer = entry.getValue();
        return values.get(nodeServer).get(key);
    }

    @Override
    public void addNode(String newNode) {
        var newNodeServer = new NodeServer(newNode);
        var hash = getHash(newNode);

        // get current node server and its values
        var currentNodeEntry = ring.tailMap(hash).firstEntry();
        if (currentNodeEntry == null) {
            currentNodeEntry = ring.firstEntry();
        }
        var previousNodeServer = currentNodeEntry.getValue();
        var previousNodeValues = values.get(previousNodeServer);

        // adding the new node server
        ring.put(hash, newNodeServer);

        values.put(newNodeServer, new HashMap<>(previousNodeValues));
        values.put(previousNodeServer, new HashMap<>());
    }

    @Override
    public void removeNode(String nodeToRemove) {
        var hash = getHash(nodeToRemove);
        ring.remove(hash);
        values.remove(new NodeServer(nodeToRemove));
    }

    @Override
    public List<NodeServerWithData> nodeServers() {
        return ring.values().stream()
            .map(nodeServer -> new NodeServerWithData(nodeServer.name(), values.get(nodeServer)))
            .toList();
    }

    private String getHash(String key) {
        return Hashing
            .sha256()
            .hashString(key, UTF_8)
            .toString();
    }

}
