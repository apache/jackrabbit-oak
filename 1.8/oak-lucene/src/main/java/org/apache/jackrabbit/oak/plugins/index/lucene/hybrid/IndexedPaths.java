/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.jackrabbit.oak.plugins.index.lucene.hybrid;

import java.util.Collection;
import java.util.Iterator;
import java.util.Map;

import com.google.common.base.Function;
import com.google.common.collect.Iterators;
import com.google.common.collect.Multimap;
import org.apache.jackrabbit.oak.plugins.document.spi.JournalProperty;

import static com.google.common.base.Preconditions.checkNotNull;

class IndexedPaths implements JournalProperty, Iterable<IndexedPathInfo> {
    private final Multimap<String, String> indexedPaths;

    public IndexedPaths(Multimap<String, String> indexedPaths) {
        this.indexedPaths = checkNotNull(indexedPaths);
    }

    @Override
    public Iterator<IndexedPathInfo> iterator() {
        return Iterators.transform(indexedPaths.asMap().entrySet().iterator(),
                new Function<Map.Entry<String, Collection<String>>, IndexedPathInfo>() {
            @Override
            public IndexedPathInfo apply(final Map.Entry<String, Collection<String>> input) {
                return new IndexedPathInfo() {
                    @Override
                    public String getPath() {
                        return input.getKey();
                    }

                    @Override
                    public Iterable<String> getIndexPaths() {
                        return input.getValue();
                    }
                };
            }
        });
    }

    @Override
    public String toString() {
        return indexedPaths.toString();
    }

    public boolean isEmpty(){
        return indexedPaths.isEmpty();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        IndexedPaths that = (IndexedPaths) o;

        return indexedPaths.equals(that.indexedPaths);
    }

    @Override
    public int hashCode() {
        return 0;
    }
}
