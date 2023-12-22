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
import java.util.stream.Collectors;

import org.apache.jackrabbit.oak.index.indexer.document.flatfile.analysis.stream.NodeData;
import org.apache.jackrabbit.oak.index.indexer.document.flatfile.analysis.stream.NodeProperty;
import org.apache.jackrabbit.oak.index.indexer.document.flatfile.analysis.stream.NodeProperty.ValueType;
import org.apache.jackrabbit.oak.index.indexer.document.flatfile.analysis.utils.BloomFilter;
import org.apache.jackrabbit.oak.index.indexer.document.flatfile.analysis.utils.HyperLogLog;

/**
 * Collects the number and size of distinct binaries.
 *
 * We keep references to large binaries in a fixed-size set, so that for large
 * binaries, we have accurate data. For smaller binaries, we have approximate
 * data only, managed in a Bloom filter (also with a fixed size). If the set of
 * large binaries grows too large, the entries are removed and instead added to
 * a Bloom filter. The size threshold (which binaries go to the set and which
 * binaries go to the Bloom filter) is changed dynamically. The Bloom filter can
 * collect about 1 million entries per MB with a false-positive rate of 1%.
 *
 * That means that for large binaries, we have the exact de-duplicated count and
 * size. For smaller binaries, we only have an approximation, due to the nature
 * of the Bloom filter. To compensate for false positives, the total size of the
 * presumed duplicates (where the size was not accumulated when adding to the
 * Bloom filter) is summed up, and at the end of the collection process,
 * multiplied with the false-positive rate of the Bloom filter.
 *
 * Experiments show that for 4 million references, total size 17 TB, the
 * approximation error is less than nearly zero when allocating 16 MB for the
 * large set, and 16 MB for the Bloom filter. When not allocating any memory for
 * the large binaries, and only 1 MB for the Bloom filter, the estimation error
 * is 5%. In general it seems that giving more memory to the Bloom filter is
 * more efficient than giving the memory to have accurate data for very large
 * binaries, unless if the size distribution of binaries is very skewed (and so,
 * having an error for a very large binary would have a big effect).
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
            referenceSize += id.getLength();
            if (largeBinariesCountMax > 0 && id.getLength() >= largeBinarySizeThreshold) {
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
            bloomFilterIgnoredSize += id.getLength();
        } else {
            bloomFilter.add(id.getLongHash());
            bloomFilterMinCount++;
            bloomFilterMinSize += id.getLength();
        }
    }

    private void truncateLargeBinariesSet() {
        if (largeBinaries.size() < largeBinariesCountMax * 2) {
            return;
        }
        long[] lengths = new long[largeBinaries.size()];
        int i = 0;
        for(BinaryId id : largeBinaries) {
            lengths[i++] = id.getLength();
        }
        Arrays.sort(lengths);
        // the new threshold is the median of all the lengths
        largeBinarySizeThreshold = lengths[largeBinariesCountMax];
        for(Iterator<BinaryId> it = largeBinaries.iterator(); it.hasNext();) {
            BinaryId id = it.next();
            if (id.getLength() < largeBinarySizeThreshold) {
                addToBloomFilter(id);
                it.remove();
            }
        }
    }

    public void end() {
        storage.add("config Bloom filter memory MB", bloomFilterMB);
        storage.add("config large binaries set memory MB", largeBinariesMB);

        storage.add("large binaries count", largeBinaries.size());
        storage.add("large binaries count max", largeBinariesCountMax * 2L);
        storage.add("large binaries size threshold", largeBinarySizeThreshold);
        long largeBinariesSize = 0;
        for(BinaryId id : largeBinaries) {
            largeBinariesSize += id.getLength();
        }
        storage.add("large binaries size", largeBinariesSize);
        storage.add("small binaries min count", bloomFilterMinCount);
        storage.add("small binaries min size", bloomFilterMinSize);
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
        storage.add("small binaries count", smallBinariesEstimatedCount);
        storage.add("small binaries HLL count", smallBinariesEstimatedCountHLL);
        storage.add("small binaries size", bloomFilterEstimatedSize);

        long estimatedCount = largeBinaries.size() + smallBinariesEstimatedCount;
        storage.add("total distinct count", estimatedCount);
        long estimatedSize = largeBinariesSize + bloomFilterEstimatedSize;
        storage.add("total distinct size", estimatedSize);
        storage.add("total reference count", referenceCount);
        storage.add("total reference size", referenceSize);
    }

    public List<String> getRecords() {
        return BinarySizeHistogram.getRecordsWithSizeAndCount(storage);
    }

    public String toString() {
        StringBuilder buff = new StringBuilder();
        buff.append("DistinctBinarySize\n");
        buff.append(getRecords().stream().map(s -> s + "\n").collect(Collectors.joining()));
        buff.append(storage);
        return buff.toString();
    }

}
