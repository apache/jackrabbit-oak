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
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.stream.Collectors;

import org.apache.jackrabbit.oak.index.indexer.document.flatfile.analysis.stream.NodeData;
import org.apache.jackrabbit.oak.index.indexer.document.flatfile.analysis.stream.NodeProperty;
import org.apache.jackrabbit.oak.index.indexer.document.flatfile.analysis.stream.NodeProperty.ValueType;
import org.apache.jackrabbit.oak.index.indexer.document.flatfile.analysis.utils.BloomFilter;
import org.apache.jackrabbit.oak.index.indexer.document.flatfile.analysis.utils.Hash;
import org.apache.jackrabbit.oak.index.indexer.document.flatfile.analysis.utils.HyperLogLog;

/**
 * A histogram of distinct binaries. For each size range, we calculate the
 * number of entries and number of distinct entries. The number of distinct
 * entries is calculated using a set if the number of entries is smaller than
 * 1024, or HyperLogLog otherwise.
 */
public class DistinctBinarySize implements StatsCollector {

    private static final double BLOOM_FILTER_FPP = 0.01;

    private final Storage storage = new Storage();
    private final HashSet<BinaryId> largeBinaries = new HashSet<>();
    private final HyperLogLog hll = new HyperLogLog(16 * 1024, 0);
    private final BloomFilter bloomFilter;
    private final long largeBinariesMB;
    private final long bloomFilterMB;
    private long largeBinarySizeThreshold;
    private int largeBinariesCountMax;
    private long bloomFilterMinCount;
    private long bloomFilterMinSize;
    private long bloomFilterIgnoredSize;
    private long referenceCount;
    private long referenceSize;

    public DistinctBinarySize(long largeBinariesMB, long bloomFilterMB) {
        this.largeBinariesMB = largeBinariesMB;
        this.bloomFilterMB = bloomFilterMB;
        // we assume each entry will need 64 bytes
        largeBinariesCountMax = (int) (largeBinariesMB * 1_000_000 / 64);
        long n = BloomFilter.calculateN(bloomFilterMB * 1_000_000 * 8, BLOOM_FILTER_FPP);
        bloomFilter = BloomFilter.construct(n, BLOOM_FILTER_FPP);
    }

    public void add(NodeData node) {
        ArrayList<BinaryId> list = new ArrayList<>();
        for(NodeProperty p : node.getProperties()) {
            if (p.getType() == ValueType.BINARY) {
                for (String v : p.getValues()) {
                    if (!v.startsWith(":blobId:")) {
                        continue;
                    }
                    v = v.substring(":blobId:".length());
                    if (v.startsWith("0x")) {
                        // embedded: ignore
                    } else {
                        // reference
                        list.add(new BinaryId(v));
                    }
                }
            }
        }
        referenceCount += list.size();
        for(BinaryId id : list) {
            referenceSize += id.length;
            if (largeBinariesCountMax > 0 && id.length >= largeBinarySizeThreshold) {
                largeBinaries.add(id);
                truncateLargeBinariesSet();
            } else {
                addToBloomFilter(id);
            }
        }
    }

    private void addToBloomFilter(BinaryId id) {
        hll.add(id.getLongHash());
        if (bloomFilter.mayContain(id.getLongHash())) {
            bloomFilterIgnoredSize += id.length;
        } else {
            bloomFilter.add(id.getLongHash());
            bloomFilterMinCount++;
            bloomFilterMinSize += id.length;
        }
    }

    private void truncateLargeBinariesSet() {
        if (largeBinaries.size() < largeBinariesCountMax * 2) {
            return;
        }
        long[] lengths = new long[largeBinaries.size()];
        int i = 0;
        for(BinaryId id : largeBinaries) {
            lengths[i++] = id.length;
        }
        Arrays.sort(lengths);
        // the new threshold is the median of all the lengths
        largeBinarySizeThreshold = lengths[largeBinariesCountMax];
        for(Iterator<BinaryId> it = largeBinaries.iterator(); it.hasNext();) {
            BinaryId id = it.next();
            if (id.length < largeBinarySizeThreshold) {
                addToBloomFilter(id);
                it.remove();
            }
        }
    }

    public void end() {
        storage.add("configBloomFilterMB", bloomFilterMB);
        storage.add("configLargeBinariesMB", largeBinariesMB);
        storage.add("largeBinariesCount", largeBinaries.size());
        storage.add("largeBinariesCountMax", largeBinariesCountMax * 2);
        storage.add("largeBinariesSizeThreshold", largeBinarySizeThreshold);
        long largeBinariesSize = 0;
        for(BinaryId id : largeBinaries) {
            largeBinariesSize += id.length;
        }
        storage.add("largeBinariesSize", largeBinariesSize);
        storage.add("smallBinariesMinCount", bloomFilterMinCount);
        storage.add("smallBinariesMinSize", bloomFilterMinSize);
        long smallBinariesEstimatedCountHLL = hll.estimate();
        long smallBinariesEstimatedCount = bloomFilter.getEstimatedEntryCount();
        if (smallBinariesEstimatedCount == Long.MAX_VALUE) {
            smallBinariesEstimatedCount = smallBinariesEstimatedCountHLL;
        }
        // We correct for the following:
        // If the Bloom filter did already contain an entry,
        // this could have been a false positive.
        // So we multiply the fpp with the size of ignored entries,
        // and will get a good approximation of the missed size
        double fpp = BloomFilter.calculateFpp(smallBinariesEstimatedCount, bloomFilter.getBitCount(), bloomFilter.getK());
        long bloomFilterEstimatedSize = bloomFilterMinSize;
        bloomFilterEstimatedSize += fpp * bloomFilterIgnoredSize;
        storage.add("smallBinariesEstimatedCount", smallBinariesEstimatedCount);
        storage.add("smallBinariesEstimatedCountHLL", smallBinariesEstimatedCountHLL);
        storage.add("smallBinariesEstimatedSize", bloomFilterEstimatedSize);
        long estimatedCount = largeBinaries.size() + smallBinariesEstimatedCount;
        storage.add("estimatedCount", estimatedCount);
        long estimatedSize = largeBinariesSize + bloomFilterEstimatedSize;
        storage.add("estimatedSize", estimatedSize);
        storage.add("referenceCount", referenceCount);
        storage.add("referenceSize", referenceSize);
    }

    public List<String> getRecords() {
        List<String> result = new ArrayList<>();
        for(Entry<String, Long> e : storage.entrySet()) {
            if (e.getValue() > 0) {
                result.add(e.getKey() + ": " + e.getValue());
            }
        }
        return result;
    }

    public String toString() {
        StringBuilder buff = new StringBuilder();
        buff.append("DistinctBinarySize\n");
        buff.append(getRecords().stream().map(s -> s + "\n").collect(Collectors.joining()));
        buff.append(storage);
        return buff.toString();
    }

    static class BinaryId {
        private final long v0;
        private final long v1;
        private final long v2;
        private final long length;

        BinaryId(String identifier) {
            // we support identifiers of the following format:
            // <hex digits or '-'>#<length>
            // the '-' is ignored
            int hashIndex = identifier.indexOf('#');
            String length = identifier.substring(hashIndex + 1);
            this.length = Long.parseLong(length);
            StringBuilder buff = new StringBuilder(48);
            for (int i = 0; i < hashIndex; i++) {
                char c = identifier.charAt(i);
                if (c != '-') {
                    buff.append(c);
                }
            }
            // we need to hash again because some of the bits are fixed
            // in case of UUIDs: always a "4" here: xxxxxxxx-xxxx-4xxx
            this.v0 = Hash.hash64(Long.parseUnsignedLong(buff.substring(0, 16), 16));
            this.v1 = Hash.hash64(Long.parseUnsignedLong(buff.substring(16, 32), 16));
            this.v2 = Hash.hash64(Long.parseUnsignedLong(buff.substring(32, Math.min(48, buff.length())), 16));
        }

        @Override
        public int hashCode() {
            return Objects.hash(length, v0, v1, v2);
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj)
                return true;
            if (obj == null)
                return false;
            if (getClass() != obj.getClass())
                return false;
            BinaryId other = (BinaryId) obj;
            return length == other.length && v0 == other.v0 && v1 == other.v1 && v2 == other.v2;
        }

        public long getLongHash() {
            return v0 ^ v1 ^ v2 ^ length;
        }

    }

}
