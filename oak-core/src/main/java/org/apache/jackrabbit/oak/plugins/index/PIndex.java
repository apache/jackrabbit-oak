/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.oak.plugins.index;

import java.util.Iterator;

import org.apache.jackrabbit.mk.simple.NodeImpl;
import org.apache.jackrabbit.oak.spi.query.Index;

/**
 * An index is a lookup mechanism. It typically uses a tree to store data. It
 * updates the tree whenever a node was changed. The index is updated
 * automatically.
 */
public interface PIndex extends Index{

    /**
     * The given node was added or removed.
     *
     * @param node the node including (old or new) data
     * @param add true if added, false if removed
     */
    void addOrRemoveNode(NodeImpl node, boolean add);

    /**
     * The given property was added or removed.
     *
     * @param nodePath the path of the node
     * @param propertyName the property name
     * @param value the old (when deleting) or new (when adding) value
     * @param add true if added, false if removed
     */
    void addOrRemoveProperty(String nodePath, String propertyName,
            String value, boolean add);

    /**
     * Get an iterator over the paths for the given value. For unique
     * indexes, the iterator will contain at most one element.
     *
     * @param value the value, or null to return all indexed rows
     * @param revision the revision
     * @return an iterator of the paths (an empty iterator if not found)
     */
    Iterator<String> getPaths(String value, String revision);

}
