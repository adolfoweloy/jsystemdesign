package org.keystore.consistenthash;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.keystore.KeyValueStore;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.keystore.KeyStoreTestUtils.addDataToStore;
import static org.keystore.KeyStoreTestUtils.allKeyValuePairsFrom;

class ConsistentHashingKeyValueStoreTest {
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
        var store = KeyValueStore.constentHashing(
                "node1", "node2", "node3", "node4"
        );

        addDataToStore(data, store);

        assertThat(allKeyValuePairsFrom(data, store))
                .containsAllEntriesOf(data);
    }

    @Test
    void addingNewNodeShouldNotChangeDataDistribution() {
        var store = KeyValueStore.constentHashing(
                "node1", "node2", "node3", "node4"
        );

        addDataToStore(data, store);

        var dataBeforeAddingNode = allKeyValuePairsFrom(data, store);

        store.addNode("node5");

        assertThat(allKeyValuePairsFrom(data, store))
                .containsAllEntriesOf(dataBeforeAddingNode);
    }

    @Test
    void removingNodeShouldNotChangeDataDistribution() {
        var store = KeyValueStore.constentHashing(
                "node1", "node2", "node3", "node4"
        );

        addDataToStore(data, store);

        var dataBeforeRemovingNode = allKeyValuePairsFrom(data, store);

        store.removeNode("node4");

        assertThat(allKeyValuePairsFrom(data, store))
                .containsAllEntriesOf(dataBeforeRemovingNode);
    }

    @Test
    void dataShouldBeDistributedEvenlyAmongNodes() {
        var store = KeyValueStore.constentHashing(
                "node1", "node2", "node3", "node4"
        );

        addDataToStore(data, store);

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


}