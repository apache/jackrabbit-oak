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
import java.util.List;
import java.util.Map.Entry;
import java.util.stream.Collectors;

import org.apache.jackrabbit.oak.index.indexer.document.flatfile.analysis.stream.NodeData;
import org.apache.jackrabbit.oak.plugins.index.counter.SipHash;

/**
 * A collector for approximate node counts.
 */
public class NodeCount implements StatsCollector {

    private final long seed;
    private final long bitMask;
    private final Storage storage = new Storage();

    public NodeCount(int resolution, long seed) {
        this.bitMask = (Long.highestOneBit(resolution) * 2) - 1;
        this.seed = seed;
    }

    public void add(NodeData node) {
        List<String> pathElements = node.getPathElements();
        SipHash hash = new SipHash(seed);
        ArrayList<String> parents = new ArrayList<>();
        for(String pe : pathElements) {
            hash = new SipHash(hash, pe.hashCode());
            parents.add(pe);
        }
        // with bitMask=1024: with a probability of 1:1024,
        if ((hash.hashCode() & bitMask) == 0) {
            // add 1024
            long offset = bitMask + 1;
            storage.add("/", offset);
            StringBuilder buff = new StringBuilder();
            for(String p : parents) {
                buff.append('/').append(p);
                String pa = buff.toString();
                storage.add(pa, offset);
            }
        }
    }

    public List<String> getRecords() {
        List<String> result = new ArrayList<>();
        ArrayList<Entry<String, Long>> entryList = new ArrayList<>(storage.entrySet());
        // this would sort by value:
        // Collections.sort(entryList, (o1, o2) -> o2.getValue().compareTo(o1.getValue()));
        // but it's unclear if that's better or not
        for(Entry<String, Long> e : entryList) {
            if (e.getValue() > 1_000_000) {
                result.add(e.getKey() + ": " + (e.getValue() / 1_000_000));
            }
        }
        return result;
    }

    public String toString() {
        StringBuilder buff = new StringBuilder();
        buff.append("NodeCount in million\n");
        buff.append(getRecords().stream().map(s -> s + "\n").collect(Collectors.joining()));
        buff.append(storage);
        return buff.toString();
    }

}
