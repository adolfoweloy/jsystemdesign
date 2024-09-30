package org.keystore;

import java.util.HashMap;
import java.util.Map;

public class KeyStoreTestUtils {

    public static void addDataToStore(Map<String, String> data, KeyValueStore store) {
        for (var entry : data.entrySet()) {
            store.put(entry.getKey(), entry.getValue());
        }
    }

    public static Map<String, String> allKeyValuePairsFrom(Map<String, String> data, KeyValueStore store) {
        var result = new HashMap<String, String>();
        for (var entry : data.entrySet()) {
            var value = store.get(entry.getKey());
            result.put(entry.getKey(), value);
        }
        return result;
    }
}
