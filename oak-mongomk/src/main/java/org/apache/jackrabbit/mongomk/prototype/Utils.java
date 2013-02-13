package org.apache.jackrabbit.mongomk.prototype;

import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicInteger;

public class Utils {
    
    static long startTime = System.currentTimeMillis();
    static AtomicInteger counter = new AtomicInteger();
    
    static int pathDepth(String path) {
        return path.equals("/") ? 0 : path.replaceAll("[^/]", "").length();
    }
    
    static <K, V> Map<K, V> newMap() {
        return new TreeMap<K, V>();
    }
    
    /**
     * Create a simple revision id. It consists of 3 hex characters for the
     * seconds since startup, 3 characters for the cluster id, and 3 characters
     * for the counter. The format is similar to MongoDB ObjectId.
     * 
     * @param clusterId the unique machineId + processId
     * @return the unique revision id
     */
    static String createRevision(int clusterId) {
        int seconds = (int) ((System.currentTimeMillis() - startTime) / 1000);
        int count = counter.getAndIncrement();
        StringBuilder buff = new StringBuilder("r");
        buff.append(Integer.toHexString(0x1000 + seconds).substring(1));
        buff.append(Integer.toHexString(0x1000 + clusterId).substring(1));
        buff.append(Integer.toHexString(0x1000 + count).substring(1));
        return buff.toString();
    }
    
    /**
     * Get the age of a revision in seconds.
     * 
     * @param rev the revision
     * @return the age in seconds
     */
    int getRevisionAge(String rev) {
        String s = rev.substring(0, 3);
        return Integer.parseInt(s, 16);
    }
    
}
