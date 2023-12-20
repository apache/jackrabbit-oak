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

import org.apache.jackrabbit.oak.index.indexer.document.flatfile.analysis.stream.NodeData;
import org.apache.jackrabbit.oak.index.indexer.document.flatfile.analysis.stream.NodeProperty;

/**
 * An indexed property. It can have a list of parent nodes, and a node type.
 */
public class IndexedProperty {
    private final String name;
    private final List<String> parents;
    private final String nodeType;

    private IndexedProperty(String name, List<String> parents, String nodeType) {
        this.name = name;
        this.parents = parents;
        this.nodeType = nodeType;
    }

    public static IndexedProperty create(String nodeType, String fullName) {
        String[] list = fullName.split("/");
        String name = list[list.length - 1];
        ArrayList<String> parents = new ArrayList<>(Arrays.asList(list));
        parents.remove(parents.size() - 1);
        return new IndexedProperty(name, parents, nodeType);
    }

    public String toString() {
        return "(" + nodeType + ") " + String.join("/", parents) + "/" + name;
    }

    /**
     * Check if a certain property matches the index definition.
     *
     * @param name the name of the property
     * @param node the node (which may have parent nodes)
     * @return whether the property matches the index definition
     */
    public boolean matches(String name, NodeData node) {
        if (!name.equals(this.name)) {
            return false;
        }
        List<String> pathElements = node.getPathElements();
        if (pathElements.size() < parents.size()) {
            return false;
        }
        for (int i = 0; i < parents.size(); i++) {
            String pr = parents.get(i);
            String pe = pathElements.get(pathElements.size() - parents.size() + i);
            if (!pr.equals(pe)) {
                return false;
            }
        }
        NodeData nodeTypeCheck = node;
        for (int i = 0; i < parents.size(); i++) {
            NodeData p = nodeTypeCheck.getParent();
            if (p == null) {
                return false;
            }
            nodeTypeCheck = p;
        }
        if (nodeType.equals("nt:base")) {
            return true;
        }
        NodeProperty pt = nodeTypeCheck.getProperty("jcr:primaryType");
        if (pt == null) {
            throw new IllegalStateException("no primary type");
        }
        if (name.equals("jcr:primaryType")) {
            // we index the property "jcr:primaryType"
            // here we simply ignore the hierarchy
            return pt.getValues()[0].equals(nodeType);
        }
        return pt.getValues()[0].equals(nodeType);
    }

    public String getPropertyName() {
        return name;
    }
}
