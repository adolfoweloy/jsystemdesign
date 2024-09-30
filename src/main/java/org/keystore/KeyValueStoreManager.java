package org.keystore;

import java.util.List;

public interface KeyValueStoreManager {

    void addNode(String newNode);

    void removeNode(String nodeToRemove);

    List<NodeServer> nodeServers();
}
