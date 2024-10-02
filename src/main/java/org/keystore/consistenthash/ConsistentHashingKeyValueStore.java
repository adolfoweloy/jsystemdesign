package org.keystore.consistenthash;

import com.google.common.hash.Hashing;
import org.keystore.KeyValueStore;
import org.keystore.NodeServer;
import org.keystore.NodeServerWithData;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import static java.nio.charset.StandardCharsets.UTF_8;

public class ConsistentHashingKeyValueStore implements KeyValueStore {
    private final TreeMap<String, TreeMap<String, String>> ring = new TreeMap<>(); // virtual node hash -> key values
    private final Map<String, List<VirtualNode>> vtnMap = new HashMap<>(); // node server name -> list of VirtualNodes

    private static final int replicas = 100;

    public ConsistentHashingKeyValueStore(String... nodes) {
        for (String node : nodes) {
            var nodeServer = new NodeServer(node);

            for (int i = 0; i < replicas; i++) {
                var hash = getHash(node + i);
                var virtualNode = new VirtualNode(hash, nodeServer, i);
                ring.put(virtualNode.hash(), new TreeMap<>());
                vtnMap.computeIfAbsent(nodeServer.name(), k -> new ArrayList<>()).add(virtualNode);
            }

        }
    }

    @Override
    public void put(String key, String value) {
        var hash = getHash(key);
        var entry = ring.tailMap(hash).firstEntry();
        if (entry == null) {
            entry = ring.firstEntry();
        }

        var map = entry.getValue();
        map.put(getHash(key), value);
    }

    @Override
    public String get(String key) {
        var hash = getHash(key);
        var entry = ring.tailMap(hash).firstEntry();
        if (entry == null) {
            entry = ring.firstEntry();
        }

        var map = entry.getValue();
        return map.get(getHash(key));
    }

    @Override
    public void addNode(String newNodeName) {
        var newNode = new NodeServer(newNodeName);
        vtnMap.put(newNodeName, new ArrayList<>());

        for (int i=0; i < replicas; i++) {
            var hash = getHash(newNodeName + i);
            var virtualNode = new VirtualNode(hash, newNode, i);

            // before adding to the ring, find the current server and its values
            var first = ring.tailMap(hash).firstEntry(); // this is the hash of the new VTN
            if (first == null) {
                first = ring.firstEntry();
            }

            var currentVirtualNodeHash = first.getKey();
            var currentVirtualNodeValues = first.getValue();


            // remap values to the new VTN before adding it to the ring
            // get keys that are smaller than current VTN hash within currentVirtualNodeValues
            var valuesToRemap = currentVirtualNodeValues.headMap(hash);
            var valuesToKeep = currentVirtualNodeValues.tailMap(hash);


            ring.put(hash, new TreeMap<>(valuesToRemap));
            ring.put(currentVirtualNodeHash, new TreeMap<>(valuesToKeep));

            vtnMap.get(newNode.name()).add(virtualNode);
        }
    }

    @Override
    public void removeNode(String nodeToRemove) {
        var virtualNodes = vtnMap.get(nodeToRemove);
        for (VirtualNode vn : virtualNodes) {
            var next = ring.tailMap(vn.hash(), false).firstEntry();
            if (next == null) {
                next = ring.firstEntry();
            }

            ring.get(next.getKey()).putAll(ring.get(vn.hash()));
            ring.remove(vn.hash());
        }
    }

    @Override
    public List<NodeServerWithData> nodeServers() {
        return vtnMap.values().stream()
            .flatMap(Collection::stream)
            .map(virtualNode -> new NodeServerWithData(virtualNode.name(), ring.get(virtualNode.hash())))
            .toList();
    }

    private String getHash(String key) {
        return Hashing
            .sha1()
            .hashString(key, UTF_8)
            .toString();
    }

}
