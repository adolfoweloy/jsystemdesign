package org.keystore;

import java.util.Map;

public record NodeServerWithData(String name, Map<String, String> data) {
}
