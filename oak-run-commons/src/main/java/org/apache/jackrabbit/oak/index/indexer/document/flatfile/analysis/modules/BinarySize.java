package org.apache.jackrabbit.oak.index.indexer.document.flatfile.analysis.modules;

import java.util.List;
import java.util.Map.Entry;
import java.util.Random;

import org.apache.jackrabbit.oak.index.indexer.document.flatfile.analysis.NodeData;
import org.apache.jackrabbit.oak.index.indexer.document.flatfile.analysis.Property;
import org.apache.jackrabbit.oak.index.indexer.document.flatfile.analysis.StatsCollector;
import org.apache.jackrabbit.oak.index.indexer.document.flatfile.analysis.Storage;
import org.apache.jackrabbit.oak.index.indexer.document.flatfile.analysis.Property.ValueType;

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
public class BinarySize implements StatsCollector {
    
    Storage storage;
    int resolution;
    Random random = new Random(1);
    
    public BinarySize(int resolution) {
        this.resolution = resolution;
    }
    
    @Override
    public void setStorage(Storage storage) {
        this.storage = storage;
    }

    public void add(NodeData node) {
        List<Property> properties = node.properties;
        List<String> pathElements = node.pathElements;
        long size = 0;
        for(Property p : properties) {
            if (p.getType() == ValueType.STRING) {
                for (String v : p.getValues()) {
                    if (!v.startsWith(":blobId:")) {
                        continue;
                    }
                    v = v.substring(":blobId:".length());
                    if (v.startsWith("0x")) {
                        // embedded
                    } else {
                        // reference
                        int hashIndex = v.indexOf('#');
                        String length = v.substring(hashIndex + 1);
                        size += Long.parseLong(length);
                    }
                }
            }
        }
        if (size == 0) {
            return;
        }
        storage.add("/", size);
        StringBuilder buff = new StringBuilder();
        for (int i = 0; i < pathElements.size(); i++) {
            String pe = pathElements.get(i);
            buff.append('/').append(pe);
            String key = buff.toString();
            if (pe.equals("jcr:content")) {
                break;
            }
            if (i < 3) {
                storage.add(key, size);
            } else {
                long s2 = size / resolution * resolution;
                if (s2 > 0) {
                    storage.add(key, size);
                } else {
                    if (random.nextInt(resolution) < size) {
                        storage.add(key, (long) resolution);
                    }
                }
            }
        }
    }
    
    public String toString() {
        StringBuilder buff = new StringBuilder();
        buff.append("BinarySize GB (resolution: " + resolution + ")\n");
        for(Entry<String, Long> e : storage.entrySet()) {
            long v = e.getValue();
            if (v > 1_000_000_000) {
                buff.append(e.getKey() + ": " + (v / 1_000_000_000)).append('\n');
            }
        }
        buff.append(storage);
        return buff.toString();
    }

    @Override
    public void end() {
    }

}
