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
import org.apache.jackrabbit.oak.index.indexer.document.flatfile.analysis.modules.BinarySizeEmbedded;
import org.apache.jackrabbit.oak.index.indexer.document.flatfile.analysis.modules.BinarySizeHistogram;
import org.apache.jackrabbit.oak.index.indexer.document.flatfile.analysis.modules.DistinctBinarySizeHistogram;
import org.apache.jackrabbit.oak.index.indexer.document.flatfile.analysis.modules.ListCollector;
import org.apache.jackrabbit.oak.index.indexer.document.flatfile.analysis.modules.NodeCount;
import org.apache.jackrabbit.oak.index.indexer.document.flatfile.analysis.modules.NodeTypeCount;
import org.apache.jackrabbit.oak.index.indexer.document.flatfile.analysis.modules.NodeNameFilter;
import org.apache.jackrabbit.oak.index.indexer.document.flatfile.analysis.modules.PropertyStats;
import org.apache.jackrabbit.oak.index.indexer.document.flatfile.analysis.modules.TopLargestBinaries;

public class StatsBuilder {
    
    private static final boolean ONLY_READ = false;

    public static void main(String... args) throws Exception {
        
        String fileName = args[0];
        String filter = null;
        if (args.length > 1) {
            filter = args[1];
        }
        
        ListCollector collectors = new ListCollector();
        collectors.add(new NodeCount(1000));
        collectors.add(new BinarySize(100_000_000));
        collectors.add(new BinarySizeEmbedded(100_000));
        PropertyStats ps = new PropertyStats(true);
        collectors.add(ps);
        collectors.add(new NodeTypeCount());
        collectors.add(new BinarySizeHistogram(1));
        collectors.add(new DistinctBinarySizeHistogram(1));
        collectors.add(new TopLargestBinaries(10));
        if (filter != null) {
            collectors.add(new NodeNameFilter(filter, new BinarySize(100_000_000)));
            collectors.add(new NodeNameFilter(filter, new BinarySizeEmbedded(100_000)));
            collectors.add(new NodeNameFilter(filter, new BinarySizeHistogram(1)));
            collectors.add(new NodeNameFilter(filter, new TopLargestBinaries(10)));
        }
        
        Profiler prof = new Profiler().startCollecting();
        
        NodeLineReader reader = NodeLineReader.open(fileName);
        // NodeStreamReaderCompressed reader = NodeStreamReaderCompressed.open(fileName);
        collect(reader, collectors);
        
        System.out.println(prof.getTop(10));
        System.out.println(collectors);
    }
    
    private static void collect(NodeDataReader reader, StatsCollector collector) throws IOException {
        long start = System.nanoTime();
        NodeData last = null;
        long lineCount = 0;
        
        while (true) {
            lineCount++;
            // if(lineCount > 1000000) break;
            NodeData node = reader.readNode();
            if (node == null) {
                break;
            }
            if (ONLY_READ) {
                continue;
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
        System.out.println((time / lineCount) + " ns/node");
        System.out.println(reader.getFileSize() + " bytes");
        System.out.println((reader.getFileSize() / lineCount) + " bytes/node");
    }
  
}
