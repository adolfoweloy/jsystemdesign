package org.keystore.consistenthash;

import org.keystore.NodeServer;

public record VirtualNode(String hash, NodeServer nodeServer, int replica)
        implements Comparable<VirtualNode>
{
    public String name() {
        return nodeServer.name() + " - " + replica;
    }

    @Override
    public int compareTo(VirtualNode o) {
        return hash.compareTo(o.hash);
    }
}
