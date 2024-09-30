package org.keystore.rehashing;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;
import org.keystore.KeyValueStore;

import static org.assertj.core.api.Assertions.assertThat;

class RehashingKeyValueStoreTest {

    private final Map<String, String> data = new HashMap<>();

    @BeforeEach
    void setUp() {
        data.clear();
        for (int i = 0; i < 100; i++) {
            data.put("key" + i, "value" + i);
        }
    }

    @Test
    void putAndGetShouldBeConsistentWithAStaticNumberOfNodes() {
        var store = KeyValueStore.rehashing(
            "node1", "node2", "node3", "node4"
        );

        addDataToStore(store);

        assertThat(allKeyValuePairsFrom(store))
            .containsAllEntriesOf(data);
    }

    @Test
    void addingNewNodeShouldNotChangeDataDistribution() {
        var store = KeyValueStore.rehashing(
            "node1", "node2", "node3", "node4"
        );

        addDataToStore(store);

        var dataBeforeAddingNode = allKeyValuePairsFrom(store);

        store.addNode("node5");

        assertThat(allKeyValuePairsFrom(store))
            .containsAllEntriesOf(dataBeforeAddingNode);
    }

    @Test
    void removingNodeShouldNotChangeDataDistribution() {
        var store = KeyValueStore.rehashing(
            "node1", "node2", "node3", "node4"
        );

        addDataToStore(store);

        var dataBeforeRemovingNode = allKeyValuePairsFrom(store);

        store.removeNode("node4");

        assertThat(allKeyValuePairsFrom(store))
            .containsAllEntriesOf(dataBeforeRemovingNode);
    }

    @Test
    void dataShouldBeDistributedEvenlyAmongNodes() {
        var store = KeyValueStore.rehashing(
            "node1", "node2", "node3", "node4"
        );

        addDataToStore(store);

        var nodes = store.nodeServers();
        var expectedDataPerNode = data.size() / nodes.size();

        for (var node : nodes) {
            var actualDataSize = node.data().size();
            assertThat(actualDataSize)
                .isBetween(
                    expectedDataPerNode - 1,
                    expectedDataPerNode + 1
                );
        }
    }

    private void addDataToStore(KeyValueStore store) {
        for (var entry : data.entrySet()) {
            store.put(entry.getKey(), entry.getValue());
        }
    }

    private Map<String, String> allKeyValuePairsFrom(KeyValueStore store) {
        var result = new HashMap<String, String>();
        for (var entry : data.entrySet()) {
            var value = store.get(entry.getKey());
            result.put(entry.getKey(), value);
        }
        return result;
    }

}