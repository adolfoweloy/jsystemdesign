package org.keystore.consistenthash;

public record VirtualNode(NodeServer nodeServer, int replica) {
    public String name() {
        return nodeServer.name() + " - " + replica;
    }
}
