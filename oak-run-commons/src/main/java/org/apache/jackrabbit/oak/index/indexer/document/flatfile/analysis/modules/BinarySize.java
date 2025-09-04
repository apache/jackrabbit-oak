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

import java.util.Map.Entry;
import java.util.stream.Collectors;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.jackrabbit.oak.index.indexer.document.flatfile.analysis.stream.NodeData;
import org.apache.jackrabbit.oak.index.indexer.document.flatfile.analysis.stream.NodeProperty;
import org.apache.jackrabbit.oak.index.indexer.document.flatfile.analysis.stream.NodeProperty.ValueType;

/**
 * Collects the total binary size (references to the datastore) per path.
 */
public class BinarySize implements StatsCollector {

    private final Storage storage = new Storage();
    private final int resolution;
    private final boolean embedded;
    private final String unit;
    private final int divideBy;
    private final Random random;

    public BinarySize(boolean embedded, long seed) {
        this.embedded = embedded;
        if (embedded) {
            unit = "MB";
            divideBy = 1_000_000;
        } else {
            unit = "GB";
            divideBy = 1_000_000_000;
        }
        this.resolution = divideBy / 10;
        this.random = new Random(seed); //NOSONAR
    }

    public void add(NodeData node) {
        long size = 0;
        for(NodeProperty p : node.getProperties()) {
            if (p.getType() == ValueType.BINARY) {
                for (String v : p.getValues()) {
                    if (!v.startsWith(":blobId:")) {
                        continue;
                    }
                    v = v.substring(":blobId:".length());
                    if (v.startsWith("0x")) {
                        // embedded
                        if (embedded) {
                            int hashIndex = v.lastIndexOf('#');
                            if (hashIndex >= 0) {
                                v = v.substring(0, hashIndex);
                            }
                            size = (v.length() - 2) / 2;
                        }
                    } else {
                        // reference
                        if (!embedded) {
                            int hashIndex = v.lastIndexOf('#');
                            String length = v.substring(hashIndex + 1);
                            size += Long.parseLong(length);
                        }
                    }
                }
            }
        }
        if (size == 0) {
            return;
        }
        storage.add("/", size);
        StringBuilder buff = new StringBuilder();
        for (int i = 0; i < node.getPathElements().size(); i++) {
            String pe = node.getPathElements().get(i);
            buff.append('/').append(pe);
            String key = buff.toString();
            if (pe.equals("jcr:content")) {
                break;
            }
            if (i < 2) {
                storage.add(key, size);
            } else {
                long s2 = size / resolution * resolution;
                if (s2 > 0) {
                    storage.add(key, size);
                } else {
                    if (random.nextInt(resolution) < size) {
                        storage.add(key, resolution);
                    }
                }
            }
        }
    }

    public List<String> getRecords() {
        List<String> result = new ArrayList<>();
        for(Entry<String, Long> e : storage.entrySet()) {
            long v = e.getValue();
            if (v > divideBy) {
                result.add(e.getKey() + ": " + (v / divideBy));
            }
        }
        return result;
    }

    public String toString() {
        StringBuilder buff = new StringBuilder();
        buff.append("BinarySize");
        buff.append(embedded ? " embedded" : " references");
        buff.append(" in " + unit);
        buff.append(" (resolution: " + resolution + ")\n");
        buff.append(getRecords().stream().map(s -> s + "\n").collect(Collectors.joining()));
        buff.append(storage);
        return buff.toString();
    }

}
