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

import java.util.List;

import org.apache.jackrabbit.oak.index.indexer.document.flatfile.analysis.stream.NodeData;

/**
 * A wrapper for a collector that allows to filter for certain node names, or
 * children of those.
 */
public class NodeNameFilter implements StatsCollector {

    private final StatsCollector base;
    private final String nodeName;

    public NodeNameFilter(String nodeName, StatsCollector base) {
        this.nodeName = nodeName;
        this.base = base;
    }

    @Override
    public void add(NodeData node) {
        List<String> pathElements = node.getPathElements();
        for(String pe : pathElements) {
            if (pe.equals(nodeName)) {
                base.add(node);
                break;
            }
        }
    }

    @Override
    public void end() {
        base.end();
    }

    public List<String> getRecords() {
        return base.getRecords();
    }

    public String toString() {
        return "NodeNameFilter " + nodeName + " of " + base.toString();
    }

}
