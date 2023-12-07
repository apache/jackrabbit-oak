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
import org.apache.jackrabbit.oak.index.indexer.document.flatfile.analysis.modules.IndexDefinitions;
import org.apache.jackrabbit.oak.index.indexer.document.flatfile.analysis.modules.ListCollector;
import org.apache.jackrabbit.oak.index.indexer.document.flatfile.analysis.modules.NodeCount;
import org.apache.jackrabbit.oak.index.indexer.document.flatfile.analysis.modules.NodeTypes;
import org.apache.jackrabbit.oak.index.indexer.document.flatfile.analysis.modules.PathFilter;
import org.apache.jackrabbit.oak.index.indexer.document.flatfile.analysis.modules.PropertyStats;
import org.apache.jackrabbit.oak.index.indexer.document.flatfile.analysis.modules.TopLargestBinaries;

public class StatsBuilder {
    
    private static final boolean ONLY_READ = true;

    public static void main(String... args) throws IOException {
        IndexDefinitions indexDefs = new IndexDefinitions();
        if (args.length > 1) {
            collect(args[1], indexDefs);
        }  
        NodeTypes nodeTypes = new NodeTypes();
        if (args.length > 2) {
            collect(args[2], nodeTypes);
        }  
        System.out.println(nodeTypes);
        
        ListCollector collectors = new ListCollector();
        collectors.add(new NodeCount(1000));
        PropertyStats ps = new PropertyStats();
        ps.setIndexedProperties(indexDefs.getPropertyMap());
        collectors.add(ps);
        collectors.add(new IndexDefinitions());
        collectors.add(new BinarySize(100_000_000));
        collectors.add(new BinarySizeEmbedded(100_000));
        collectors.add(new BinarySizeHistogram(1));
        collectors.add(new TopLargestBinaries(10));
        collectors.add(new PathFilter("cqdam.text.txt", new BinarySize(100_000_000)));
        collectors.add(new PathFilter("cqdam.text.txt", new BinarySizeEmbedded(100_000)));
        collectors.add(new PathFilter("cqdam.text.txt", new BinarySizeHistogram(1)));
        collectors.add(new PathFilter("cqdam.text.txt", new TopLargestBinaries(10)));
        
        Profiler prof = new Profiler().startCollecting();
        
        collect(args[0], collectors);
        
        System.out.println(prof.getTop(10));
        
        collectors.print();

    }
    
    private static void collect(String fileName, StatsCollector collector) throws IOException {
        NodeLineReader reader = NodeLineReader.open(fileName);
        // NodeStreamReader2 reader = NodeStreamReader2.open(fileName);
        NodeData last = null;
        while (true) {
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
        System.out.println(reader.count + " lines total");
        long time = System.nanoTime() - reader.start;
        System.out.println((time / 1_000_000_000) + " seconds");
        System.out.println((time / reader.count) + " ns/entry");
        System.out.println(reader.fileSize + " bytes");
        System.out.println((reader.fileSize / reader.count) + " bytes/entry");
    }
  
}
