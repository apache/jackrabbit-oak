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
package org.apache.jackrabbit.oak.plugins.document.persistentCache;

import java.util.Map;

import org.h2.mvstore.MVMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class MapFactory {
    
    static final Logger LOG = LoggerFactory.getLogger(MapFactory.class);

    private int openCount = 1;

    /**
     * Ensure the store is open, re-opening it if needed. The store is first
     * closed if the old open count if the old open count matches the current
     * open count.
     *
     * @param oldOpenCount the old open count
     * @return the new open count
     */
    synchronized int reopenStoreIfNeeded(int oldOpenCount) {
        if (oldOpenCount == openCount) {
            closeStore();
            openCount++;
            openStore();
        }
        return openCount;
    }
    
    public int getOpenCount() {
        return openCount;
    }
    
    /**
     * Open the store.
     */
    abstract void openStore();
    
    /**
     * Close the store.
     */
    abstract void closeStore();
    
    /**
     * Open or get the given map.
     * 
     * @param <K> the key type
     * @param <V> the value type
     * @param name the map name
     * @param builder the map builder
     * @return
     */
    abstract <K, V> Map<K, V> openMap(String name, MVMap.Builder<K, V> builder);
    
    /**
     * Get the file size in bytes.
     * 
     * @return the file size
     */
    abstract long getFileSize();

}
