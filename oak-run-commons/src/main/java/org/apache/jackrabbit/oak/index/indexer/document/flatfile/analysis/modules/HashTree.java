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

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.Map.Entry;

import org.apache.jackrabbit.oak.index.indexer.document.flatfile.analysis.NodeData;
import org.apache.jackrabbit.oak.index.indexer.document.flatfile.analysis.Property;
import org.apache.jackrabbit.oak.index.indexer.document.flatfile.analysis.StatsCollector;
import org.apache.jackrabbit.oak.index.indexer.document.flatfile.analysis.Storage;

public class HashTree implements StatsCollector {
    
    private static final int MIN_LEVELS = 2;
    private static final int MAX_LEVELS = 3;
    private static final boolean CRYPTOGRAPHICALLY_SAFE = true;
    Storage storage;

    @Override
    public void setStorage(Storage storage) {
        this.storage = storage;
    }

    @Override
    public void add(NodeData node) {
        if (node.getPathElements().size() < MIN_LEVELS) {
            return;
        }
        long nodeHash = 0;
        if (CRYPTOGRAPHICALLY_SAFE) {
            try {
                // SHA-256: 54 seconds
                // SHA3-256: 56 seconds
                MessageDigest md = MessageDigest.getInstance("SHA3-256");
                for(Property p : node.getProperties()) {
                    md.update(p.getName().getBytes(StandardCharsets.UTF_8));
                    md.update((byte) p.getType().getOrdinal());
                    for(String s : p.getValues()) {
                        md.update(s.getBytes(StandardCharsets.UTF_8));
                    }
                }
                byte[] digest = md.digest();
                nodeHash = Arrays.hashCode(digest);
            } catch (NoSuchAlgorithmException e) {
                throw new IllegalStateException();
            }
        } else {
            for(Property p : node.getProperties()) {
                nodeHash = Hash.hash64(p.getName().hashCode(), nodeHash);
                nodeHash = Hash.hash64(p.getType().getOrdinal(), nodeHash);
                for(String s : p.getValues()) {
                    nodeHash = Hash.hash64(s.hashCode(), nodeHash);
                }
            }
        }
        StringBuilder buff = new StringBuilder();
        int i = 0;
        for (; i < MIN_LEVELS; i++) {
            buff.append('/').append(node.getPathElements().get(i));
        }
        for (; i < MAX_LEVELS && i < node.getPathElements().size(); i++) {
            buff.append('/').append(node.getPathElements().get(i));
            String path = buff.toString();
            Long hashObj = storage.get(path);
            long hash = hashObj == null ? 0 : hashObj;
            SipHash sipHash = new SipHash(new SipHash(hash), nodeHash);
            storage.put(path, sipHash.longHashCode());
        }
    }
    
    public String toString() {
        StringBuilder buff = new StringBuilder();
        buff.append("MerkleTree\n");
        for(Entry<String, Long> e : storage.entrySet()) {
            buff.append(e.getKey() + ": " + e.getValue()).append('\n');
        }
        buff.append(storage);
        return buff.toString();
    }   

    @Override
    public void end() {
    }

}
