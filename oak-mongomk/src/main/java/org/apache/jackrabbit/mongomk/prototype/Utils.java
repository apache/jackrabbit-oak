package org.apache.jackrabbit.mongomk.prototype;

import java.util.Map;
import java.util.TreeMap;

public class Utils {
    
    static int pathDepth(String path) {
        return path.equals("/") ? 0 : path.replaceAll("[^/]", "").length();
    }
    
    static <K, V> Map<K, V> newMap() {
        return new TreeMap<K, V>();
    }
    
}
