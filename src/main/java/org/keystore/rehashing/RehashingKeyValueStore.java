package org.keystore.rehashing;

import org.keystore.KeyValueStore;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class RehashingKeyValueStore implements KeyValueStore {
    private final Map<Integer, Map<String, String>> nodes = new HashMap<>();
    private final Map<String, Integer> nodeIndexes = new HashMap<>();

    public RehashingKeyValueStore(String... nodeNames) {
        for (String nodeName : nodeNames) {
            var index = nodeIndexes.size();
            this.nodeIndexes.put(nodeName, index);
            this.nodes.put(index, new HashMap<>());
        }
    }

    @Override
    public void put(String key, String value) {
        var nodeIndex = lookupNodeWithKey(getHash(key));
        nodes.get(nodeIndex).put(key, value);
    }

    @Override
    public String get(String key) {
        var nodeIndex = lookupNodeWithKey(getHash(key));
        return nodes.get(nodeIndex).get(key);
    }

    @Override
    public List<Integer> nodesSizes() {
        return nodes.values().stream()
                .map(Map::size)
                .toList();
    }

    private int getHash(String value) {
        return value.hashCode();
    }

    private int lookupNodeWithKey(Integer hash) {
        return hash % nodeIndexes.size();
    }

    @Override
    public void addNode(String newNodeName) {
        var index = Math.max(
            nodeIndexes.size(),
            nodeIndexes.values().stream().max(Integer::compareTo).orElse(0)
        );
        nodeIndexes.put(newNodeName, index);
        nodes.put(index, new HashMap<>());

        rehash(new HashSet<>(nodeIndexes.values()));
    }

    @Override
    public void removeNode(String nodeToRemove) {
        var keyValues = getCurrentValues();

        var index = nodeIndexes.get(nodeToRemove);
        nodes.remove(index);
        nodeIndexes.remove(nodeToRemove);

        for (Map.Entry<String, String> entry : keyValues.entrySet()) {
            put(entry.getKey(), entry.getValue());
        }
    }

    /**
     * Rehashing affects all key-value pairs in the stores proving to be an inefficient way to
     * distribute data among nodes in a distributed key-value storage system.
     */
    private void rehash(Set<Integer> nodeIndexes) {
        // save all key-value pairs before resetting nodes
        var keyValues = getCurrentValues();

        // reset nodes
        nodes.clear();
        for (Integer index : nodeIndexes) {
            nodes.put(index, new HashMap<>());
        }

        // rehash all data
        for (Map.Entry<String, String> entry : keyValues.entrySet()) {
            put(entry.getKey(), entry.getValue());
        }
    }

    private Map<String, String> getCurrentValues() {
        return nodes.values().stream()
                .flatMap(map -> map.entrySet().stream())
                .collect(Collectors.toMap(
                        Map.Entry::getKey,
                        Map.Entry::getValue
                ));
    }
}
