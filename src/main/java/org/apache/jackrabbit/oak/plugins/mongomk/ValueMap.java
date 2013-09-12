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

import java.util.AbstractMap;
import java.util.AbstractSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import javax.annotation.Nonnull;

import com.google.common.collect.Iterators;

/**
 * A value map contains the versioned values of a property. The key into this
 * map is the revision when the value was set.
 */
class ValueMap {

    @Nonnull
    static Map<String, String> create(final @Nonnull NodeDocument doc,
                                      final @Nonnull String property) {
        final Map<String, String> map = doc.getLocalMap(property);
        if (doc.getPreviousRanges().isEmpty()) {
            return map;
        }
        final Set<Map.Entry<String, String>> values
                = new AbstractSet<Map.Entry<String, String>>() {

            @Override
            @Nonnull
            public Iterator<Map.Entry<String, String>> iterator() {
                return Iterators.concat(map.entrySet().iterator(), Iterators.concat(new Iterator<Iterator<Map.Entry<String, String>>>() {
                    private final Iterator<NodeDocument> previous = doc.getPreviousDocs(null).iterator();

                    @Override
                    public boolean hasNext() {
                        return previous.hasNext();
                    }

                    @Override
                    public Iterator<Map.Entry<String, String>> next() {
                        return previous.next().getLocalMap(property).entrySet().iterator();
                    }

                    @Override
                    public void remove() {
                        throw new UnsupportedOperationException();
                    }
                }));
            }

            @Override
            public int size() {
                int size = map.size();
                for (NodeDocument prev : doc.getPreviousDocs(null)) {
                    size += prev.getLocalMap(property).size();
                }
                return size;
            }
        };
        return new AbstractMap<String, String>() {

            private final Map<String, String> map = doc.getLocalMap(property);

            @Override
            @Nonnull
            public Set<Entry<String, String>> entrySet() {
                return values;
            }

            @Override
            public String get(Object key) {
                // first check values map of this document
                String value = map.get(key);
                if (value != null) {
                    return value;
                }
                Revision r = Revision.fromString(key.toString());
                for (NodeDocument prev : doc.getPreviousDocs(r)) {
                    value = prev.getLocalMap(property).get(key);
                    if (value != null) {
                        return value;
                    }
                }
                // not found
                return null;
            }

            @Override
            public boolean containsKey(Object key) {
                // can use get()
                // the values map does not have null values
                return get(key) != null;
            }
        };
    }
}
