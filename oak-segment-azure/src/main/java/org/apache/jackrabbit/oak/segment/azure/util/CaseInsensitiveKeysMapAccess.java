package org.apache.jackrabbit.oak.segment.azure.util;

import java.util.Collections;
import java.util.Map;
import java.util.TreeMap;

/**
 * Wrapper around the map that allows accessing the map with case-insensitive keys.
 * For example, the keys 'hello' and 'Hello' access the same value.
 * <p>
 * If there is a conflicting key, any one of the keys and any one of the values is used. Because of
 * the nature of  Hashmaps, the result is not deterministic.
 */
public class CaseInsensitiveKeysMapAccess {

    /**
     * Wrapper around the map that allows accessing the map with case-insensitive keys.
     * <p>
     * Return an unmodifiable map to make it clear that changes are not reflected to the original map.
     *
     * @param map the map to convert
     * @return an unmodifiable map with case-insensitive key access
     */
    public static Map<String, String> convert(Map<String, String> map) {
        Map<String, String> caseInsensitiveMap = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        if (map != null) {
            caseInsensitiveMap.putAll(map);
        }
        // return an unmodifiable map to make it clear that changes are not reflected in the original map.
        return Collections.unmodifiableMap(caseInsensitiveMap);
    }

}
