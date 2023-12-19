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
package org.apache.jackrabbit.oak.index.indexer.document.flatfile.analysis.stream;

import java.util.List;

/**
 * Represents a node in a stream.
 */
public class NodeData {
    private final List<String> pathElements;
    private final List<NodeProperty> properties;
    private NodeData parent;

    public NodeData(List<String> pathElements, List<NodeProperty> properties) {
        this.pathElements = pathElements;
        this.properties = properties;
    }

    public List<String> getPathElements() {
        return pathElements;
    }

    public List<NodeProperty> getProperties() {
        return properties;
    }

    public String toString() {
        return "/" + String.join("/", pathElements);
    }

    public NodeData getParent() {
        return parent;
    }

    public void setParent(NodeData parent) {
        this.parent = parent;
    }

    public NodeProperty getProperty(String name) {
        for(NodeProperty p : properties) {
            if (p.getName().equals(name)) {
                return p;
            }
        }
        return null;
    }
}
