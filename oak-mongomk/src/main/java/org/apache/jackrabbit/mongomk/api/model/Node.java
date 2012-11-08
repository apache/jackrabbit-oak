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
package org.apache.jackrabbit.mongomk.api.model;

import java.util.Iterator;
import java.util.Map;

import org.apache.jackrabbit.mk.model.NodeDiffHandler;

/**
 * A higher level object representing a node.
 */
public interface Node {

    /**
     * Returns the descendant node entry (descendant)
     *
     * @param name Name of the descendant.
     * @return Descendant node.
     */
    Node getChildNodeEntry(String name);

    /**
     *
     * Returns the total number of children of this node.
     *
     * <p>
     * <strong>This is not necessarily equal to the number of children returned by
     * {@link #getChildren()} since this {@code Node} might be created with only
     * a subset of children.</strong>
     * </p>
     *
     * @return The total number of children.
     */
    int getChildNodeCount();

    /**
     * Returns the children iterator for the supplied offset and count.
     *
     * @param offset The offset to return the children from.
     * @param count The number of children to return.
     * @return Iterator with child entries.
     */
    Iterator<Node> getChildNodeEntries(int offset, int count);

    /**
     * Returns the properties this {@code Node} was created with.
     *
     * @return The properties.
     */
    Map<String, String> getProperties();

    /**
     * Diffs this node with the other node and calls the passed in diff handler.
     *
     * @param otherNode Other node.
     * @param nodeDiffHandler Diff handler.
     */
    void diff(Node otherNode, NodeDiffHandler nodeDiffHandler);

    /**
     * Returns the path of this {@code Node}.
     *
     * @return The path.
     */
    String getPath();

    /**
     * Returns the revision id of this node if known already, else this will return {@code null}.
     * The revision id will be determined only after the commit has been successfully
     * performed or the node has been read as part of an existing revision.
     *
     * @return The revision id of this commit or {@code null}.
     */
    Long getRevisionId();
}