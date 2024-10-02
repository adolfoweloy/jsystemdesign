package org.keystore.consistenthash;

public record NodeServer(String name) implements Comparable<NodeServer> {
    @Override
    public int compareTo(NodeServer other) {
        return name.compareTo(other.name);
    }
}