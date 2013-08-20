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
package org.apache.jackrabbit.oak.plugins.mongomk;

import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.annotation.CheckForNull;

import org.apache.jackrabbit.oak.cache.CacheValue;
import org.apache.jackrabbit.oak.plugins.mongomk.util.Utils;

/**
 * A document corresponds to a node stored in the MongoMK. A document contains
 * all the revisions of a node stored in the {@link DocumentStore}.
 */
public class Document implements CacheValue {

    /**
     * The node id, which contains the depth of the path
     * (0 for root, 1 for children of the root), and then the path.
     */
    static final String ID = "_id";

    /**
     * The data of this document.
     */
    protected Map<String, Object> data = new TreeMap<String, Object>();

    /**
     * Whether this document is sealed (immutable data).
     */
    private AtomicBoolean sealed = new AtomicBoolean(false);

    /**
     * @return the id of this document or <code>null</code> if none is set.
     */
    @CheckForNull
    public String getId() {
        return (String) get(ID);
    }

    /**
     * Gets the data for the given <code>key</code>.
     *
     * @param key the key.
     * @return the data or <code>null</code>.
     */
    @CheckForNull
    public Object get(String key) {
        return data.get(key);
    }

    /**
     * Sets the data for the given <code>key</code>.
     *
     * @param key the key.
     * @param value the value to set.
     * @return the previous value or <code>null</code> if there was none.
     */
    @CheckForNull
    public Object put(String key, Object value) {
        return data.put(key, value);
    }

    /**
     * @return a Set view of the keys contained in this document.
     */
    public Set<String> keySet() {
        return data.keySet();
    }

    /**
     * Seals this document and turns it into an immutable object. Any attempt
     * to modify this document afterwards will result in an
     * {@link UnsupportedOperationException}.
     */
    public void seal() {
        if (!sealed.getAndSet(true)) {
            data = seal(data);
        }
    }

    /**
     * Performs a deep copy of the data within this document to the given target.
     *
     * @param target the target document.
     */
    public void deepCopy(Document target) {
        Utils.deepCopyMap(data, target.data);
    }

    /**
     * Formats this document for use in a log message.
     *
     * @return the formatted string
     */
    public String format() {
        return data.toString().replaceAll(", _", ",\n_").replaceAll("}, ", "},\n");
    }

    //-----------------------------< CacheValue >-------------------------------

    @Override
    public int getMemory() {
        return Utils.estimateMemoryUsage(this.data);
    }

    //------------------------------< internal >--------------------------------

    private static Map<String, Object> seal(Map<String, Object> map) {
        for (Map.Entry<String, Object> entry : map.entrySet()) {
            Object value = entry.getValue();
            if (value instanceof Map) {
                @SuppressWarnings("unchecked")
                Map<String, Object> childMap = (Map<String, Object>) value;
                entry.setValue(seal(childMap));
            }
        }
        return Collections.unmodifiableMap(map);
    }
}
