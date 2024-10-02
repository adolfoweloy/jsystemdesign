package org.keystore;

import java.util.List;

/**
 * This interface expresses the ideal API for whichever entity maintains the nodes creation/deletion.
 * The current implementation does not support creation of manager separately from the client.
 */
public interface KeyValueStoreManager {

    void addNode(String newNode);

    void removeNode(String nodeToRemove);

    List<Integer> nodesSizes();
}
