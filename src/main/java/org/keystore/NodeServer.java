package org.keystore;

/**
 * Represents a node to which key-value entries will be stored.
 * @param name
 */
public record NodeServer(String name) implements Comparable<NodeServer> {
    @Override
    public int compareTo(NodeServer other) {
        return name.compareTo(other.name);
    }
}