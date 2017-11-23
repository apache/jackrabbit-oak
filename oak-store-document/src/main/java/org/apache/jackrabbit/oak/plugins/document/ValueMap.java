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

import java.util.AbstractMap;
import java.util.AbstractSet;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

import javax.annotation.Nonnull;

import org.apache.jackrabbit.oak.plugins.document.util.MergeSortedIterators;

import com.google.common.base.Objects;
import com.google.common.collect.Iterators;

/**
 * A value map contains the versioned values of a property. The key into this
 * map is the revision when the value was set.
 */
class ValueMap {

    static final SortedMap<Revision, String> EMPTY = Collections.unmodifiableSortedMap(
            new TreeMap<Revision, String>(StableRevisionComparator.REVERSE));

    @Nonnull
    static Map<Revision, String> create(@Nonnull final NodeDocument doc,
                                        @Nonnull final String property) {
        final SortedMap<Revision, String> map = doc.getLocalMap(property);
        if (doc.getPreviousRanges().isEmpty()) {
            return map;
        }
        final Set<Map.Entry<Revision, String>> entrySet
                = new AbstractSet<Map.Entry<Revision, String>>() {

            @Override
            @Nonnull
            public Iterator<Map.Entry<Revision, String>> iterator() {

                final Comparator<? super Revision> c = map.comparator();
                final Iterator<NodeDocument> docs;
                if (map.isEmpty()) {
                    docs = doc.getPreviousDocs(property, null).iterator();
                } else {
                    // merge sort local map into maps of previous documents
                    List<Iterator<NodeDocument>> iterators = 
                            new ArrayList<Iterator<NodeDocument>>(2);
                    iterators.add(Iterators.singletonIterator(doc));
                    iterators.add(doc.getPreviousDocs(property, null).iterator());                            
                    docs = Iterators.mergeSorted(iterators, new Comparator<NodeDocument>() {
                                @Override
                                public int compare(NodeDocument o1,
                                                   NodeDocument o2) {
                                    Revision r1 = getFirstRevision(o1);
                                    Revision r2 = getFirstRevision(o2);
                                    return c.compare(r1, r2);
                                }
                            
                                private Revision getFirstRevision(NodeDocument d) {
                                    Map<Revision, String> values;
                                    if (Objects.equal(d.getId(), doc.getId())) {
                                        // return local map for main document
                                        values = d.getLocalMap(property);
                                    } else {
                                        values = d.getValueMap(property);
                                    }
                                    return values.keySet().iterator().next();
                                }
                        
                            });
                }

                return new MergeSortedIterators<Map.Entry<Revision, String>>(
                        new Comparator<Map.Entry<Revision, String>>() {
                            @Override
                            public int compare(Map.Entry<Revision, String> o1,
                                               Map.Entry<Revision, String> o2) {
                                return c.compare(o1.getKey(), o2.getKey());
                            }
                        }
                ) {
                    @Override
                    public Iterator<Map.Entry<Revision, String>> nextIterator() {
                        NodeDocument d = docs.hasNext() ? docs.next() : null;
                        if (d == null) {
                            return null;
                        }
                        Map<Revision, String> values;
                        if (Objects.equal(d.getId(), doc.getId())) {
                            // return local map for main document
                            values = d.getLocalMap(property);
                        } else {
                            values = d.getValueMap(property);
                        }
                        return values.entrySet().iterator();
                    }

                    @Override
                    public String description() {
                        return "Revisioned values for property " + doc.getId() + "/" + property + ":";
                    }
                };
            }

            @Override
            public int size() {
                int size = map.size();
                for (NodeDocument prev : doc.getPreviousDocs(property, null)) {
                    size += prev.getValueMap(property).size();
                }
                return size;
            }
        };

        return new AbstractMap<Revision, String>() {

            private final Map<Revision, String> map = doc.getLocalMap(property);

            @Override
            @Nonnull
            public Set<Entry<Revision, String>> entrySet() {
                return entrySet;
            }

            @Override
            public String get(Object key) {
                Revision r = (Revision) key;
                // first check values map of this document
                if (map.containsKey(r)) {
                    return map.get(r);
                }
                for (NodeDocument prev : doc.getPreviousDocs(property, r)) {
                    String value = prev.getValueMap(property).get(r);
                    if (value != null) {
                        return value;
                    }
                }
                // not found or null
                return null;
            }

            @Override
            public boolean containsKey(Object key) {
                // check local map first
                if (map.containsKey(key)) {
                    return true;
                }
                Revision r = (Revision) key;
                for (NodeDocument prev : doc.getPreviousDocs(property, r)) {
                    if (prev.getValueMap(property).containsKey(key)) {
                        return true;
                    }
                }
                return false;
            }
        };
    }
}
