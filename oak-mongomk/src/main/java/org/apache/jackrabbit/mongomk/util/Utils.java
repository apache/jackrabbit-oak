/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.mongomk.util;

import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;

import org.bson.types.ObjectId;

/**
 * Utility methods.
 */
public class Utils {
    
    public static int pathDepth(String path) {
        if (path.equals("/")) {
            return 0;
        }
        int depth = 0;
        for (int i = 0; i < path.length(); i++) {
            if (path.charAt(i) == '/') {
                depth++;
            }
        }
        return depth;
    }
    
    public static <K, V> Map<K, V> newMap() {
        return new TreeMap<K, V>();
    }

    public static <E> Set<E> newSet() {
        return new HashSet<E>();
    }

    @SuppressWarnings("unchecked")
    public static int estimateMemoryUsage(Map<String, Object> map) {
        if (map == null) {
            return 0;
        }
        int size = 0;
        for (Entry<String, Object> e : map.entrySet()) {
            size += e.getKey().length();
            Object o = e.getValue();
            if (o instanceof String) {
                size += o.toString().length();
            } else if (o instanceof Long) {
                size += 8;
            } else if (o instanceof Map) {
                size += 8 + estimateMemoryUsage((Map<String, Object>) o);
            }
        }
        return size;
    }

    /**
     * Generate a unique cluster id, similar to the machine id field in MongoDB ObjectId objects.
     * 
     * @return the unique machine id
     */
    public static int getUniqueClusterId() {
        ObjectId objId = new ObjectId();
        return objId._machine();
    }

    public static String escapePropertyName(String propertyName) {
        int len = propertyName.length();
        if (len == 0) {
            return "_";
        }
        // avoid creating a buffer if escaping is not needed
        StringBuilder buff = null;
        char c = propertyName.charAt(0);
        int i = 0;
        if (c == '_' || c == '$') {
            buff = new StringBuilder(len + 1);
            buff.append('_').append(c);
            i++;
        }
        for (; i < len; i++) {
            c = propertyName.charAt(i);
            char rep;
            switch (c) {
            case '.':
                rep = 'd';
                break;
            case '\\':
                rep = '\\';
                break;
            default:
                rep = 0;
            }
            if (rep != 0) {
                if (buff == null) {
                    buff = new StringBuilder(propertyName.substring(0, i));
                }
                buff.append('\\').append(rep);
            } else if (buff != null) {
                buff.append(c);
            }
        }
        return buff == null ? propertyName : buff.toString();
    }
    
    public static String unescapePropertyName(String key) {
        int len = key.length();
        if (key.startsWith("_")
                && (key.startsWith("__") || key.startsWith("_$") || len == 1)) {
            key = key.substring(1);
            len--;
        }
        // avoid creating a buffer if escaping is not needed
        StringBuilder buff = null;
        for (int i = 0; i < len; i++) {
            char c = key.charAt(i);
            if (c == '\\') {
                if (buff == null) {
                    buff = new StringBuilder(key.substring(0, i));
                }
                c = key.charAt(++i);
                if (c == '\\') {
                    // ok
                } else if (c == 'd') {
                    c = '.';
                }
                buff.append(c);
            } else if (buff != null) {
                buff.append(c);
            }
        }
        return buff == null ? key : buff.toString();
    }
    
    public static boolean isPropertyName(String key) {
        return !key.startsWith("_") || key.startsWith("__") || key.startsWith("_$");
    }

    public static String getIdFromPath(String path) {
        int depth = Utils.pathDepth(path);
        return depth + ":" + path;
    }
    
    public static String getPathFromId(String id) {
        int index = id.indexOf(':');
        return id.substring(index + 1);
    }

    /**
     * Deep copy of a map that may contain map values.
     * 
     * @param source the source map
     * @param target the target map
     */
    public static <K> void deepCopyMap(Map<K, Object> source, Map<K, Object> target) {
        for (Entry<K, Object> e : source.entrySet()) {
            Object value = e.getValue();
            if (value instanceof Map<?, ?>) {
                @SuppressWarnings("unchecked")
                Map<Object, Object> old = (Map<Object, Object>) value;
                Map<Object, Object> c = newMap();
                deepCopyMap(old, c);
                value = c;
            }
            target.put(e.getKey(), value);
        }
    }
    
    /**
     * Formats a MongoDB document for use in a log message.
     * 
     * @param document the MongoDB document.
     * @return
     */
    public static String formatDocument(Map<String, Object> document) {
    	return document.toString().replaceAll(", _", ",\n_").replaceAll("}, ", "},\n");
    }

}
