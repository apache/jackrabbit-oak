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
import java.util.Set;

/**
 * A higher level object representing a node.
 *
 * @author <a href="mailto:pmarx@adobe.com>Philipp Marx</a>
 */
public interface Node {

    /**
     * Returns the total number of children of this node.
     *
     * <p>
     * <strong>This is not necessarily equal to the number of children returned by {@link #getChildren()} since this
     * {@code Node} might be created with only a subset of children.</strong>
     * </p>
     *
     * @return The total number of children.
     */
    long getChildCount();

    /**
     * Returns the children this {@code Node} was created with.
     *
     * @return The children.
     */
    Set<Node> getChildren(); // TODO Replace Set with Collection

    // TODO - [Mete] Document.
    Iterator<Node> getChildEntries(int offset, int count);

    /**
     * Returns the descendants (children, children of the children, etc) this {@code Node} was created with.
     *
     * @param includeThis Flag indicating whether this {@code Node} should be included in the list.
     * @return The descendants.
     */
    Set<Node> getDescendants(boolean includeThis);

    /**
     * Returns the name of this {@code Node}.
     *
     * @return The name.
     */
    String getName();

    /**
     * Returns the path of this {@code Node}.
     *
     * @return The path.
     */
    String getPath();

    /**
     * Returns the properties this {@code Node} was created with.
     *
     * @return The properties.
     */
    Map<String, Object> getProperties();

    /**
     * Returns the revision id of this node if known already, else this will return {@code null}.
     * The revision id will be determined only after the commit has been successfully
     * performed or the node has been read as part of an existing revision.
     *
     * @see #setRevisionId(Long)
     *
     * @return The revision id of this commit or {@code null}.
     */
    Long getRevisionId();

    /**
     * Sets the revision id of this node.
     *
     * @see #getRevisionId()
     *
     * @param revisionId The revision id to set.
     */
    void setRevisionId(Long revisionId);
}
