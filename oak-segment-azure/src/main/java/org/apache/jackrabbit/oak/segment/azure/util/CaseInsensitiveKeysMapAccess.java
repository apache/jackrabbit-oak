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
