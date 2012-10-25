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
package org.apache.jackrabbit.oak.plugins.index.old.mk.simple;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Iterator;
import org.apache.jackrabbit.mk.json.JsopWriter;
import org.apache.jackrabbit.oak.plugins.index.old.mk.simple.NodeImpl.ChildVisitor;

/**
 * A list of child nodes.
 */
interface NodeList {

    /**
     * Get the number of (direct) child nodes.
     *
     * @return the number of child nodes
     */
    long size();

    /**
     * Check if the given child node already exists in this list.
     *
     * @param name the child node name
     * @return true if it exists
     */
    boolean containsKey(String name);

    /**
     * Get the node id of the given child node. The child node must exist.
     *
     * @param name the child node name
     * @return the node id
     */
    NodeId get(String name);

    /**
     * Add a new child node.
     *
     * @param name the node name
     * @param x the node id
     */
    void add(String name, NodeId x);

    /**
     * Replace an existing child node (keep the position).
     *
     * @param name the node name
     * @param x the node id
     */
    void replace(String name, NodeId x);

    /**
     * Get the child node name at this position. If the index is larger than the
     * number of child nodes, then null is returned.
     *
     * @param pos the index
     * @return the node name or null
     */
    String getName(long pos);

    /**
     * Get an iterator over the child node names.
     *
     * @param offset the offset
     * @param maxCount the maximum number of returned names
     * @return the iterator
     */
    Iterator<String> getNames(long offset, int maxCount);

    /**
     * Remove a child node. The node must exist.
     *
     * @param name the child node name
     * @return the old node id
     */
    NodeId remove(String name);

    /**
     * Clone the child node list.
     *
     * @param map the node map
     * @param revId the revision
     * @return a copy
     */
    NodeList createClone(NodeMap map, long revId);

    /**
     * Visit all child nodes.
     *
     * @param v the visitor
     */
    void visit(ChildVisitor v);

    /**
     * Write the child node list into a jsop writer.
     *
     * @param json the jsop writer
     * @param map the node map
     */
    void append(JsopWriter json, NodeMap map);

    /**
     * Estimate the memory used in bytes.
     *
     * @return the memory used
     */
    int getMemory();

    /**
     * Write the data into the output stream in order to calculate the content
     * hash.
     *
     * @param map the node map
     * @param out the output stream
     * @throws IOException if writing to the stream failed
     */
    void updateHash(NodeMap map, OutputStream out) throws IOException;

}
