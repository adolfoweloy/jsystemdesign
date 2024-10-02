package org.keystore;

import org.keystore.consistenthash.NodeServer;

import java.util.Map;

/**
 * This doesn't make sense. Only {@code NodeServer} is enough.
 * @see NodeServer
 */
public record NodeServerWithData(String name, Map<String, String> data) {
}
