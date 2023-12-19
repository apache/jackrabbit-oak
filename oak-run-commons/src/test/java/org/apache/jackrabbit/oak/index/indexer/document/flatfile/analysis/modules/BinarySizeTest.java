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
                + "refs / 11 (513..1024): 1000\n"
                + "refs / 21 (524289..1048576): 1000\n"
                + "refs /content 11 (513..1024): 1000\n"
                + "refs /content 21 (524289..1048576): 1000\n"
                + "refs total count: 4000\n"
                + "refs total size: 2002000000\n"
                + "storage size: 0 MB; 6 entries\n"
                + "time: 0 seconds\n"
                + "NodeNameFilter filtered of BinarySizeHistogram\n"
                + "refs / 11 (513..1024): 1000\n"
                + "refs /content 11 (513..1024): 1000\n"
                + "refs total count: 2000\n"
                + "refs total size: 2000000\n"
                + "storage size: 0 MB; 4 entries\n"
                + "time: 0 seconds\n"
                + "", list.toString());
    }

    @Test
    public void manyNodes() {
        // resolution of 10 MB
        BinarySize binary = new BinarySize(10_000_000);
        // resolution of 10 KB
        BinarySizeEmbedded binaryEmbedded = new BinarySizeEmbedded(10_000);
        BinarySizeHistogram histogram = new BinarySizeHistogram(1);
        DistinctBinarySizeHistogram distinctHistogram = new DistinctBinarySizeHistogram(1);
        TopLargestBinaries top = new TopLargestBinaries(3);

        ListCollector list = new ListCollector();
        list.add(binary);
        list.add(binaryEmbedded);
        list.add(histogram);
        list.add(distinctHistogram);
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
            NodeProperty p2 = new NodeProperty("data2", ValueType.BINARY, ":blobId:" + (i % 20) + "#5000000");
            NodeData n = new NodeData(Arrays.asList("content", "dam", "abc"), Arrays.asList(p1, p2));
            list.add(n);
        }
        list.end();

        assertEquals("[[/: 5, /content: 5, /content/dam: 5, /content/dam/abc: 4]]", Arrays.asList(binary.getRecords()).toString());
        assertEquals("[[/: 5, /content: 5, /content/dam: 5, /content/dam/abc: 4]]", Arrays.asList(binaryEmbedded.getRecords()).toString());
        assertEquals(
                "BinarySize GB (resolution: 10000000)\n"
                + "/: 5\n"
                + "/content: 5\n"
                + "/content/dam: 5\n"
                + "/content/dam/abc: 4\n"
                + "storage size: 0 MB; 4 entries\n"
                + "time: 0 seconds\n"
                + "BinarySizeEmbedded (MB)\n"
                + "/: 5\n"
                + "/content: 5\n"
                + "/content/dam: 5\n"
                + "/content/dam/abc: 4\n"
                + "storage size: 0 MB; 4 entries\n"
                + "time: 0 seconds\n"
                + "BinarySizeHistogram\n"
                + "embedded / 14 (4097..8192): 1000\n"
                + "embedded /content 14 (4097..8192): 1000\n"
                + "embedded total count: 2000\n"
                + "embedded total size: 10000000\n"
                + "refs / 24 (4194305..8388608): 1000\n"
                + "refs /content 24 (4194305..8388608): 1000\n"
                + "refs total count: 2000\n"
                + "refs total size: 10000000000\n"
                + "storage size: 0 MB; 8 entries\n"
                + "time: 0 seconds\n"
                + "DistinctBinarySizeHistogram\n"
                + "/ 24 (4194305..8388608): 1000\n"
                + "/ 24 (4194305..8388608) distinct: 20\n"
                + "/content 24 (4194305..8388608): 1000\n"
                + "/content 24 (4194305..8388608) distinct: 20\n"
                + "total count: 2000\n"
                + "total size: 10000000000\n"
                + "storage size: 0 MB; 6 entries\n"
                + "time: 0 seconds\n"
                + "TopLargestBinaries\n"
                + "#  1: /content/dam/abc: 5000000\n"
                + "#  2: /content/dam/abc: 5000000\n"
                + "#  3: /content/dam/abc: 5000000\n"
                + "total count: 1000\n"
                + "total size: 5000000000\n"
                + "storage size: 0 MB; 5 entries\n"
                + "time: 0 seconds\n"
                + "", list.toString());
    }
}
