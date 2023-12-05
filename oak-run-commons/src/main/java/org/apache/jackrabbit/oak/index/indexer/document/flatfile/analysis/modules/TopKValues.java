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
package org.apache.jackrabbit.oak.index.indexer.document.flatfile.analysis.modules;

import java.util.ArrayList;
import java.util.HashMap;

import org.apache.jackrabbit.oak.commons.json.JsopBuilder;

/**
 * Contains and implements the logic to maintain the top-K values of a property.
 * Internally, it uses a priority queue that is updated each time a value is
 * added or modified. In fact, whenever an existing value is updated, we check
 * if it should be added to the priority queue.
 */
public class TopKValues {

    private final int k;
    private final CountMinSketch sketch = new CountMinSketch(31, 32);
    private final ArrayList<TopEntry> list = new ArrayList<>();
    private final HashMap<String, TopEntry> map = new HashMap<>();

    TopKValues(int k) {
        this.k = k;
    }

    void add(String value) {
        long hash = Hash.hash64(value.hashCode());
        value = shortenIfNeeded(value);
        sketch.add(hash);
        long est = sketch.estimate(hash);
        TopEntry existing = map.get(value);
        if (existing != null) {
            existing.count = est;
            bumpIfNeeded(existing.index);
            return;
        }
        if (list.size() >= k && est < list.get(0).count) {
            return;
        }
        TopEntry e = new TopEntry(value, est);
        e.index = list.size();
        list.add(e);
        bumpIfNeeded(e.index);
        map.put(value, e);
        if (list.size() > k) {
            TopEntry smallest = list.remove(list.size() - 1);
            map.remove(smallest.value);
        }
    }
    
    void bumpIfNeeded(int index) {
        while (index > 0) {
            TopEntry existing = list.get(index);
            TopEntry next = list.get(index - 1);
            if (existing.count <= next.count) {
                break;
            }
            list.set(index, next);
            next.index = index;
            list.set(index - 1, existing);
            existing.index = index - 1;
            index--;
        }
    }

    static String shortenIfNeeded(String value) {
        if (value.length() > 100) {
            value = value.substring(0, 100) + "...";
        }
        return value;
    }

    public String toString() {
        JsopBuilder builder = new JsopBuilder();
        builder.object();
        for (int i = 0; i < list.size(); i++) {
            TopEntry x = list.get(i);
            builder.key(x.value).value(x.count);
        }
        builder.endObject();
        return builder.toString();
    }

    static class TopEntry implements Comparable<TopEntry> {
        long count;
        final String value;
        int index;

        TopEntry(String value, long count) {
            this.value = value;
            this.count = count;
        }

        @Override
        public int compareTo(TopEntry o) {
            return Long.compare(count, o.count);
        }
        
        public String toString() {
            return "[" + index + "]: " + count + " " + value;
        }
    }

}