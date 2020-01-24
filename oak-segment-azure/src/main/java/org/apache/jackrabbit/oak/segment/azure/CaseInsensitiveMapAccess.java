package org.apache.jackrabbit.oak.segment.azure;

import java.util.Collections;
import java.util.Map;
import java.util.TreeMap;

/**
 * Wrapper around the map that allows accessing the map in a case-insensitive manner.
 * For example, the keys 'hello' and 'Hello' access the same value.
 */
public class CaseInsensitiveMapAccess {
    /**
     * Wrap a map that allows accessing the map in a case-insensitive manner.
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
