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
package org.apache.jackrabbit.oak.plugins.document;

import java.util.Collections;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.jackrabbit.oak.cache.CacheValue;
import org.apache.jackrabbit.oak.plugins.document.util.Utils;

import com.google.common.collect.Maps;

/**
 * A document corresponds to a node stored in the DocumentMK. A document contains
 * all the revisions of a node stored in the {@link DocumentStore}.
 */
public class Document implements CacheValue {

    /**
     * The node id, which contains the depth of the path
     * (0 for root, 1 for children of the root), and then the path.
     */
    public static final String ID = "_id";

    /**
     * The modification count on the document. This is an long value
     * incremented on every modification.
     */
    public static final String MOD_COUNT = "_modCount";

    /**
     * The data of this document.
     */
    protected Map<String, Object> data = Maps.newHashMap();

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
     * @return the modification count of this document or <code>null</code> if
     *         none is set.
     */
    @CheckForNull
    public Number getModCount() {
        return (Number) get(MOD_COUNT);
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
            for (Map.Entry<String, Object> entry : data.entrySet()) {
                if (entry.getValue() instanceof Map) {
                    @SuppressWarnings("unchecked")
                    Map<Object, Object> map = (Map<Object, Object>) entry.getValue();
                    entry.setValue(transformAndSeal(map, entry.getKey(), 1));
                }
            }
            data = Collections.unmodifiableMap(data);
        }
    }

    /**
     * Determines if this document is sealed or not
     * @return true if document is sealed.
     */
    public boolean isSealed(){
        return sealed.get();
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

    /**
     * Transform and seal the data of this document. That is, the data becomes
     * immutable and transformation may be performed on the data.
     *
     * @param map the map to transform.
     * @param key the key for the given map or <code>null</code> if the map
     *            is the top level data map.
     * @param level the level. Zero for the top level map, one for an entry in
     *              the top level map, etc.
     * @return the transformed and sealed map.
     */
    @Nonnull
    protected Map<?, ?> transformAndSeal(@Nonnull Map<Object, Object> map,
                                         @Nullable String key,
                                         int level) {
        for (Map.Entry<Object, Object> entry : map.entrySet()) {
            Object value = entry.getValue();
            if (value instanceof Map) {
                @SuppressWarnings("unchecked")
                Map<Object, Object> childMap = (Map<Object, Object>) value;
                entry.setValue(transformAndSeal(
                        childMap, entry.getKey().toString(), level + 1));
            }
        }
        if (map instanceof NavigableMap) {
            return Maps.unmodifiableNavigableMap((NavigableMap<Object, Object>) map);
        } else {
            return Collections.unmodifiableMap(map);
        }
    }

    @Override
    public String toString() {
        return getId();
    }
}
