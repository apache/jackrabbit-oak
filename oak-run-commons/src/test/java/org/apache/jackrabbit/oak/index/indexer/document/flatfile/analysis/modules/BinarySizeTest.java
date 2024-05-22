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

import static org.junit.Assert.assertEquals;

import java.util.Arrays;

import org.apache.jackrabbit.oak.index.indexer.document.flatfile.analysis.stream.NodeData;
import org.apache.jackrabbit.oak.index.indexer.document.flatfile.analysis.stream.NodeProperty;
import org.apache.jackrabbit.oak.index.indexer.document.flatfile.analysis.stream.NodeProperty.ValueType;
import org.junit.Test;

public class BinarySizeTest {

    @Test
    public void nodeNameFilter() {
        BinarySizeHistogram histogram = new BinarySizeHistogram(1);
        NodeNameFilter filtered = new NodeNameFilter("filtered", new BinarySizeHistogram(1));

        ListCollector list = new ListCollector();
        list.add(histogram);
        list.add(filtered);

        for (int i = 0; i < 1_000; i++) {
            // 1 MB each
            NodeProperty p1 = new NodeProperty("p1", ValueType.BINARY, ":blobId:" + (i % 20) + "#1000000");
            NodeData n = new NodeData(Arrays.asList("content", "dam", "common", "n" + i), Arrays.asList(p1));
            list.add(n);
            // 1 KB each
            NodeProperty p2 = new NodeProperty("p2", ValueType.BINARY, ":blobId:" + (i % 20) + "#1000");
            n = new NodeData(Arrays.asList("content", "dam", "filtered", "n" + i), Arrays.asList(p2));
            list.add(n);
        }
        // we have an unfiltered, and filtered histogram now
        assertEquals(
                "BinarySizeHistogram\n"
                + "refs / 11 (512B..1KiB): 1000\n"
                + "refs / 21 (512KiB..1MiB): 1000\n"
                + "refs /content 11 (512B..1KiB): 1000\n"
                + "refs /content 21 (512KiB..1MiB): 1000\n"
                + "refs total count: 2000\n"
                + "refs total count million: 0\n"
                + "refs total size: 1001000000\n"
                + "refs total size GiB: 0\n"
                + "storage size: 0 MB; 6 entries\n"
                + "time: 0 seconds\n"
                + "\n"
                + "NodeNameFilter filtered of BinarySizeHistogram\n"
                + "refs / 11 (512B..1KiB): 1000\n"
                + "refs /content 11 (512B..1KiB): 1000\n"
                + "refs total count: 1000\n"
                + "refs total count million: 0\n"
                + "refs total size: 1000000\n"
                + "refs total size GiB: 0\n"
                + "storage size: 0 MB; 4 entries\n"
                + "time: 0 seconds\n"
                + "\n"
                + "", list.toString());
    }

    @Test
    public void manyNodes() {
        // resolution of 1 GB
        BinarySize binary = new BinarySize(false, 1);
        // resolution of 1 MB
        BinarySize binaryEmbedded = new BinarySize(true, 1);
        BinarySizeHistogram histogram = new BinarySizeHistogram(1);
        DistinctBinarySizeHistogram distinctHistogram = new DistinctBinarySizeHistogram(1);
        DistinctBinarySize size = new DistinctBinarySize(1, 1);
        TopLargestBinaries top = new TopLargestBinaries(3);

        ListCollector list = new ListCollector();
        list.add(binary);
        list.add(binaryEmbedded);
        list.add(histogram);
        list.add(distinctHistogram);
        list.add(size);
        list.add(top);

        // add 1000 nodes, where each node has one property:
        // - embedded: 5 KB => 5 MB
        // - external: 5 MB => 5 GB
        // an embedded binary of size 5 KB
        String embeddedBinary5k = "0".repeat(10000);
        for (int i = 0; i < 1_000; i++) {
            // 1 distinct embedded blobs, each 5 KB
            NodeProperty p1 = new NodeProperty("data1", ValueType.BINARY, ":blobId:0x" + embeddedBinary5k);
            // 20 distinct blobs ids, each 500 KB
            String id = ("00" + (i % 20)).repeat(20);
            NodeProperty p2 = new NodeProperty("data2", ValueType.BINARY, ":blobId:" + id + "#5000000");
            NodeData n = new NodeData(Arrays.asList("content", "dam", "abc", "n" + i), Arrays.asList(p1, p2));
            list.add(n);
        }
        list.end();

        assertEquals("[[/: 5, /content: 5, /content/dam: 5, /content/dam/abc: 5]]", Arrays.asList(binary.getRecords()).toString());
        assertEquals("[[/: 5, /content: 5, /content/dam: 5, /content/dam/abc: 5]]", Arrays.asList(binaryEmbedded.getRecords()).toString());
        assertEquals(
                "BinarySize references in GB (resolution: 100000000)\n"
                + "/: 5\n"
                + "/content: 5\n"
                + "/content/dam: 5\n"
                + "/content/dam/abc: 5\n"
                + "storage size: 0 MB; 53 entries\n"
                + "time: 0 seconds\n"
                + "\n"
                + "BinarySize embedded in MB (resolution: 100000)\n"
                + "/: 5\n"
                + "/content: 5\n"
                + "/content/dam: 5\n"
                + "/content/dam/abc: 5\n"
                + "storage size: 0 MB; 50 entries\n"
                + "time: 0 seconds\n"
                + "\n"
                + "BinarySizeHistogram\n"
                + "embedded / 14 (4KiB..8KiB): 1000\n"
                + "embedded /content 14 (4KiB..8KiB): 1000\n"
                + "embedded total count: 1000\n"
                + "embedded total count million: 0\n"
                + "embedded total size: 5000000\n"
                + "embedded total size GiB: 0\n"
                + "refs / 24 (4MiB..8MiB): 1000\n"
                + "refs /content 24 (4MiB..8MiB): 1000\n"
                + "refs total count: 1000\n"
                + "refs total count million: 0\n"
                + "refs total size: 5000000000\n"
                + "refs total size GiB: 4\n"
                + "storage size: 0 MB; 8 entries\n"
                + "time: 0 seconds\n"
                + "\n"
                + "DistinctBinarySizeHistogram\n"
                + "/ 24 (4MiB..8MiB): 1000\n"
                + "/ 24 (4MiB..8MiB) distinct: 20\n"
                + "/content 24 (4MiB..8MiB): 1000\n"
                + "/content 24 (4MiB..8MiB) distinct: 20\n"
                + "total count: 1000\n"
                + "total count million: 0\n"
                + "total size: 5000000000\n"
                + "total size GiB: 4\n"
                + "storage size: 0 MB; 6 entries\n"
                + "time: 0 seconds\n"
                + "\n"
                + "DistinctBinarySize\n"
                + "config Bloom filter memory MB: 1\n"
                + "config large binaries set memory MB: 1\n"
                + "large binaries count: 20\n"
                + "large binaries count million: 0\n"
                + "large binaries count max: 31250\n"
                + "large binaries size: 100000000\n"
                + "large binaries size GiB: 0\n"
                + "total distinct count: 20\n"
                + "total distinct count million: 0\n"
                + "total distinct size: 100000000\n"
                + "total distinct size GiB: 0\n"
                + "total reference count: 1000\n"
                + "total reference count million: 0\n"
                + "total reference size: 5000000000\n"
                + "total reference size GiB: 4\n"
                + "storage size: 0 MB; 15 entries\n"
                + "time: 0 seconds\n"
                + "\n"
                + "TopLargestBinaries\n"
                + "#  1: /content/dam/abc/n0: 5000000\n"
                + "#  2: /content/dam/abc/n1: 5000000\n"
                + "#  3: /content/dam/abc/n999: 5000000\n"
                + "storage size: 0 MB; 3 entries\n"
                + "time: 0 seconds\n"
                + "\n"
                + "", list.toString());
    }
}
