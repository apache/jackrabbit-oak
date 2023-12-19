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
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

import org.apache.jackrabbit.oak.index.indexer.document.flatfile.analysis.stream.NodeData;
import org.apache.jackrabbit.oak.index.indexer.document.flatfile.analysis.stream.NodeProperty;

/**
 * A collector for index definitions.
 * This is for demonstration purposes only.
 */
public class IndexDefinitions implements StatsCollector {

    private HashMap<String, ArrayList<IndexedProperty>> map = new HashMap<>();

    @Override
    public void add(NodeData node) {
        List<String> pathElements = node.getPathElements();
        if (pathElements.size() < 6 || !pathElements.get(0).equals("oak:index")) {
            return;
        }
        if (!pathElements.get(2).equals("indexRules") || !pathElements.get(4).equals("properties")) {
            return;
        }
        String nodeType = pathElements.get(3);
        NodeProperty property = node.getProperty("name");
        NodeProperty function = node.getProperty("function");
        NodeProperty isRegexp = node.getProperty("isRegexp");
        if (isRegexp != null && isRegexp.getValues()[0].toString().equals("true")) {
            // ignore regex properties
            return;
        }
        if (property != null) {
            String name = property.getValues()[0];
            if (name.equals(":nodeName")) {
                return;
            }
            IndexedProperty ip = IndexedProperty.create(nodeType, name);
            ArrayList<IndexedProperty> list = map.computeIfAbsent(ip.getPropertyName(), s -> new ArrayList<>());
            boolean duplicate = false;
            for(IndexedProperty e : list) {
                if (e.toString().equals(ip.toString())) {
                    duplicate = true;
                }
            }
            if (!duplicate) {
                list.add(ip);
            }
        }
        if (function != null) {
            // ignore for now
        }
    }

    @Override
    public void end() {
    }

    public List<String> getRecords() {
        return Collections.emptyList();
    }

    public HashMap<String, ArrayList<IndexedProperty>> getPropertyMap() {
        return map;
    }

}
