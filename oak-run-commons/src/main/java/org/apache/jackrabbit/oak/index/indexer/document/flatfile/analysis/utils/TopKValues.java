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
package org.apache.jackrabbit.oak.index.indexer.document.flatfile.analysis.utils;

import java.util.ArrayList;
import java.util.HashMap;

import org.apache.jackrabbit.oak.commons.json.JsopBuilder;

/**
 * A class that remembers the top k entries.
 *
 * Internally, the top entries are kept in a list, as well as in a hash map.
 * Also, a count-min sketch data structure is used for an approximate count per
 * entry.
 */
public class TopKValues {

    // we shorten values longer than this size
    // to save memory
    private static final int MAX_VALUE_LENGTH = 100;

    // after counting an entry, we skip this many,
    // to speed up processing
    private static final int SKIP = 10;

    private final int k;
    private final CountMinSketch sketch = new CountMinSketch(5, 16);
    private final ArrayList<TopEntry> list = new ArrayList<>();
    private final HashMap<String, TopEntry> map = new HashMap<>();
    private long min;
    private long notSkewedCount;
    private long skipRemaining;
    private long skippedCount;
    private long countedCount;

    public TopKValues(int k) {
        this.k = k;
    }

    public boolean isNotSkewed() {
        if (list.size() < k) {
            return false;
        }
        TopEntry first = list.get(0);
        TopEntry last = list.get(list.size() - 1);
        return last.count * 2 > first.count;
    }

    public long getCount() {
        return countedCount;
    }

    public long getTopCount() {
        if (list.isEmpty()) {
            return 0;
        }
        return list.get(0).count;
    }

    public long getSecondCount() {
        if (list.size() < 2) {
            return getTopCount();
        }
        return list.get(1).count;
    }

    public void add(String value) {
        if (skipRemaining > 0) {
            skippedCount++;
            skipRemaining--;
            return;
        }
        countedCount++;
        if (countedCount > 1000) {
            skipRemaining = SKIP;
        }
        long hash = Hash.hash64(value.hashCode());
        long est = sketch.addAndEstimate(hash);
        if (est < min) {
            return;
        }
        if (value.length() > MAX_VALUE_LENGTH) {
            value = value.substring(0, MAX_VALUE_LENGTH) + "..." + value.hashCode();
        }
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
        min = list.get(list.size() - 1).count;
        if (isNotSkewed()) {
            notSkewedCount++;
            skipRemaining = SKIP + Math.min(10000, notSkewedCount * notSkewedCount);
        }
    }

    private void bumpIfNeeded(int index) {
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

    public String toString() {
        JsopBuilder builder = new JsopBuilder();
        builder.object();
        if (notSkewedCount > 0) {
            builder.key("notSkewed").value(notSkewedCount);
        }
        builder.key("skipped").value(skippedCount);
        builder.key("counted").value(countedCount);
        for (int i = 0; i < list.size(); i++) {
            TopEntry x = list.get(i);
            builder.key(x.value).value(x.count);
        }
        builder.endObject();
        return builder.toString();
    }

    static class TopEntry {
        long count;
        final String value;
        int index;

        TopEntry(String value, long count) {
            this.value = value;
            this.count = count;
        }

    }

}