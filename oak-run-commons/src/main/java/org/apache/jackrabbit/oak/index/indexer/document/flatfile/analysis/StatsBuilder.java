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
package org.apache.jackrabbit.oak.index.indexer.document.flatfile.analysis;

import java.io.IOException;

import org.apache.jackrabbit.oak.commons.Profiler;
import org.apache.jackrabbit.oak.index.indexer.document.flatfile.analysis.modules.BinarySize;
import org.apache.jackrabbit.oak.index.indexer.document.flatfile.analysis.modules.BinarySizeHistogram;
import org.apache.jackrabbit.oak.index.indexer.document.flatfile.analysis.modules.DistinctBinarySize;
import org.apache.jackrabbit.oak.index.indexer.document.flatfile.analysis.modules.DistinctBinarySizeHistogram;
import org.apache.jackrabbit.oak.index.indexer.document.flatfile.analysis.modules.ListCollector;
import org.apache.jackrabbit.oak.index.indexer.document.flatfile.analysis.modules.NodeCount;
import org.apache.jackrabbit.oak.index.indexer.document.flatfile.analysis.modules.NodeNameFilter;
import org.apache.jackrabbit.oak.index.indexer.document.flatfile.analysis.modules.NodeTypeCount;
import org.apache.jackrabbit.oak.index.indexer.document.flatfile.analysis.modules.PropertyStats;
import org.apache.jackrabbit.oak.index.indexer.document.flatfile.analysis.modules.StatsCollector;
import org.apache.jackrabbit.oak.index.indexer.document.flatfile.analysis.modules.TopLargestBinaries;
import org.apache.jackrabbit.oak.index.indexer.document.flatfile.analysis.stream.NodeData;
import org.apache.jackrabbit.oak.index.indexer.document.flatfile.analysis.stream.NodeDataReader;
import org.apache.jackrabbit.oak.index.indexer.document.flatfile.analysis.stream.NodeTreeStoreReader;

/**
 * Builder for commonly used statistics for flat file stores.
 */
public class StatsBuilder {

    public static void main(String... args) throws Exception {
        String fileName = null;
        String nodeNameFilter = null;
        boolean profiler = false;
        for(int i = 0; i<args.length; i++) {
            String a = args[i];
            if (a.equals("--fileName")) {
                fileName = args[++i];
            } else if (a.equals("--nodeNameFilter")) {
                nodeNameFilter = args[++i];
            } else if (a.equals("--profiler")) {
                profiler = true;
            }
        }
        if (fileName == null) {
            System.out.println("Command line arguments:");
            System.out.println("  --fileName <file name>     (flat file store file name; mandatory)");
            System.out.println("  --nodeNameFilter <filter>  (node name filter for binaries; optional)");
            System.out.println("  --profiler                 (enable the build-in profiler; optional)");
            return;
        }
        buildStats(fileName, nodeNameFilter, profiler);
    }

    /**
     * Read a flat file store and build statistics.
     *
     * @param fileName the file name
     * @param nodeNameFilter the node names to filter
     * @param profiler also run the profiler
     */
    public static void buildStats(String fileName, String nodeNameFilter, boolean profiler) throws IOException {
        System.out.println("Processing " + fileName);
        ListCollector collectors = new ListCollector();
        collectors.add(new NodeCount(1000, 1));
        PropertyStats ps = new PropertyStats(false, 1);
        collectors.add(ps);
        collectors.add(new NodeTypeCount());
        if (nodeNameFilter != null && !nodeNameFilter.isEmpty()) {
            collectors.add(new NodeNameFilter(nodeNameFilter, new BinarySize(false, 1)));
            collectors.add(new NodeNameFilter(nodeNameFilter, new BinarySize(true, 1)));
            collectors.add(new NodeNameFilter(nodeNameFilter, new BinarySizeHistogram(1)));
            collectors.add(new NodeNameFilter(nodeNameFilter, new TopLargestBinaries(10)));
        }
        collectors.add(new BinarySize(false, 1));
        collectors.add(new BinarySize(true, 1));
        collectors.add(new BinarySizeHistogram(1));
        collectors.add(new TopLargestBinaries(10));
        collectors.add(new DistinctBinarySizeHistogram(1));
        collectors.add(new DistinctBinarySize(16, 16));

        Profiler prof = null;
        if (profiler) {
            prof = new Profiler().startCollecting();
        }
        NodeDataReader reader = NodeTreeStoreReader.open(fileName);
        collect(reader, collectors);

        if (profiler) {
            System.out.println(prof.getTop(10));
        }
        System.out.println();
        System.out.println("Results");
        System.out.println();
        System.out.println(collectors);
        System.out.println("Done");
    }

    private static void collect(NodeDataReader reader, StatsCollector collector) throws IOException {
        long start = System.nanoTime();
        NodeData last = null;
        long lineCount = 0;

        while (true) {
            NodeData node = reader.readNode();
            if (node == null) {
                break;
            }
            if (++lineCount % 1_000_000 == 0) {
                String msg = lineCount + " entries";
                int progressPercent = reader.getProgressPercent();
                if (progressPercent != 0) {
                    msg += "; " + progressPercent + "%";
                }
                System.out.println(msg);
            }
            if (last != null) {
                while (last != null && last.getPathElements().size() >= node.getPathElements().size()) {
                    // go up the chain of parent to find a possible common parent
                    last = last.getParent();
                }
                if (last != null && last.getPathElements().size() == node.getPathElements().size() - 1) {
                    // now it's possible the parent - we assume that's the case
                    node.setParent(last);
                    for (int i = 0; i < last.getPathElements().size(); i++) {
                        if (!last.getPathElements().get(i).equals(node.getPathElements().get(i))) {
                            // nope
                            node.setParent(null);
                            break;
                        }
                    }
                }
            }
            collector.add(node);
            last = node;
        }
        collector.end();
        System.out.println(lineCount + " lines total");
        long time = System.nanoTime() - start;
        System.out.println((time / 1_000_000_000) + " seconds");
        System.out.println(safeDiv(time, lineCount) + " ns/node");
        System.out.println(reader.getFileSize() + " bytes");
        System.out.println(safeDiv(reader.getFileSize(), lineCount) + " bytes/node");
    }

    private static long safeDiv(long x, long y) {
        return y == 0 ? 0 : x / y;
    }

}
