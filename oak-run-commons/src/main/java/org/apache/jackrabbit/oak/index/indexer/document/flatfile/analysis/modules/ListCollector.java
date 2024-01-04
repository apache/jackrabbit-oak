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
import java.util.List;

import org.apache.jackrabbit.oak.index.indexer.document.flatfile.analysis.stream.NodeData;

/**
 * A collector for a list of collectors.
 */
public class ListCollector implements StatsCollector {

    private final ArrayList<StatsCollector> collectors = new ArrayList<>();

    public void add(StatsCollector collector) {
        collectors.add(new TimeMeasureCollector(collector));
    }

    @Override
    public void add(NodeData node) {
        for(StatsCollector collector : collectors) {
            collector.add(node);
        }
    }

    @Override
    public void end() {
        for(StatsCollector collector : collectors) {
            collector.end();
        }
    }

    public List<String> getRecords() {
        List<String> result = new ArrayList<>();
        for(StatsCollector collector : collectors) {
            result.addAll(collector.getRecords());
        }
        return result;
    }

    public String toString() {
        StringBuilder buff = new StringBuilder();
        for (StatsCollector collector : collectors) {
            buff.append(collector.toString());
            buff.append("\n");
        }
        return buff.toString();
    }

}
