package org.apache.jackrabbit.oak.segment.azure;

import java.util.Collections;
import java.util.Map;
import java.util.TreeMap;

/**
 * Wrapper around the Map that allows accessing the map in a case-insensitive manner. The keys 'hello' and 'Hello'
 * refer to the same value.
 */
public class CaseInsensitiveMapAccess {
    public static Map<String, String> convert(Map<String, String> map) {
        Map<String, String> caseInsensitiveMap = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        if (map != null) {
            caseInsensitiveMap.putAll(map);
        }
        // return an unmodifiable map to make it clear that changes are not reflected in the original map.
        return Collections.unmodifiableMap(caseInsensitiveMap);
    }

}
