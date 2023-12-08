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
import java.util.List;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.apache.jackrabbit.oak.index.indexer.document.flatfile.analysis.NodeData;
import org.apache.jackrabbit.oak.index.indexer.document.flatfile.analysis.Property;
import org.apache.jackrabbit.oak.index.indexer.document.flatfile.analysis.StatsCollector;
import org.apache.jackrabbit.oak.index.indexer.document.flatfile.analysis.Storage;

public class NodeTypes implements StatsCollector {

    Storage storage;
    TreeMap<String, NodeType> map = new TreeMap<>();
    TreeMap<String, ArrayList<String>> flatMap = new TreeMap<>();

    @Override
    public void setStorage(Storage storage) {
        this.storage = storage;
    }

    @Override
    public void add(NodeData node) {
        List<String> pathElements = node.getPathElements();
        if (pathElements.size() != 3 || !pathElements.get(0).equals("jcr:system")) {
            return;
        }
        if (!pathElements.get(1).equals("jcr:nodeTypes")) {
            return;
        }
        String nodeType = pathElements.get(2);
        Property nodeTypeName = node.getProperty("jcr:nodeTypeName");
        Property superTypes = node.getProperty("rep:supertypes");
        Property primarySubTypes = node.getProperty("rep:primarySubtypes");
        Property mixinSubTypes = node.getProperty("rep:mixinSubtypes");
        Property isMixin = node.getProperty("jcr:isMixin");
        NodeType nt = new NodeType();
        nt.name = nodeTypeName.getValues()[0];
        if (!nodeType.equals(nt.name)) {
            throw new IllegalArgumentException();
        }
        nt.isMixin = isMixin.getValues()[0].equals("true");
        nt.superTypes = superTypes.getValues();
        nt.primarySubTypes = primarySubTypes.getValues();
        if (mixinSubTypes != null) {
            nt.mixinSubTypes = mixinSubTypes.getValues();
        } else {
            nt.mixinSubTypes = new String[0];
        }
        map.put(nt.name, nt);
    }

    @Override
    public void end() {
    }
    
    public String toString() {
        StringBuilder buff = new StringBuilder();
        buff.append("NodeTypes\n");
        for(Entry<String, NodeType> e : map.entrySet()) {
            buff.append(e.getKey() + ": " + e.getValue()).append('\n');
        }
        buff.append(storage);
        return buff.toString();
    }    
    
    static class NodeType {
        String name;
        boolean isMixin;
        String[] superTypes;
        String[] primarySubTypes;
        String[] mixinSubTypes;
        ArrayList<String> allTypes = new ArrayList<>();
        
        public String toString() {
            return name + " " + (isMixin ? "(mixin) " : "") + " super " + Arrays.toString(superTypes) + 
                    " primarySub " + Arrays.toString(primarySubTypes) +
                    " mixinSub " + Arrays.toString(mixinSubTypes);
        }
    }

}