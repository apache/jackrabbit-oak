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
package org.apache.jackrabbit.oak.plugins.observation;

import java.util.Set;

import javax.annotation.CheckForNull;

import com.google.common.collect.ImmutableSet;

/**
 * A ChangeSet is a collection of items that have been changed as part of a
 * commit. A ChangeSet is immutable and built by a ChangeSetBuilder.
 * <p>
 * Those items are parent paths, parent node names, parent node types and
 * (child) properties. 'Changed' refers to any of add, remove, change (where
 * applicable).
 * <p>
 * A ChangeSet is piggybacked on a CommitInfo in the CommitContext and can be
 * used by (downstream) Observers for their convenience.
 * <p>
 * To limit memory usage, the ChangeSet has a limit on the number of items,
 * each, that it collects. If one of those items reach the limit this is called
 * an 'overflow' and the corresponding item type is marked as having
 * 'overflown'. Downstream Observers should thus check if a particular item has
 * overflown or not - this is indicated with null as the return value of the
 * corresponding getters (while empty means: not overflown but nothing changed
 * of that type).
 * <p>
 * Also, the ChangeSet carries a 'maxPathDepth' which is the depth of the path
 * up until which paths have been collected. Thus any path that is longer than
 * this 'maxPathDepth' will be cut off and only reported up to that max depth.
 * Downstream Observers should thus inspect the 'maxPathDepth' and compare
 * actual path depths with it in order to find out if any child paths have been
 * cut off.
 * <p>
 * Naming: note that path, node name and node types all refer to the *parent* of
 * a change. While properties naturally are leafs.
 */
public class ChangeSet {

    private final int maxPathDepth;
    private final Set<String> parentPaths;
    private final Set<String> parentNodeNames;
    private final Set<String> parentNodeTypes;
    private final Set<String> propertyNames;
    private final Set<String> allNodeTypes;

    ChangeSet(int maxPathDepth, Set<String> parentPaths, Set<String> parentNodeNames, Set<String> parentNodeTypes,
            Set<String> propertyNames, Set<String> allNodeTypes) {
        this.maxPathDepth = maxPathDepth;
        this.parentPaths = parentPaths == null ? null : ImmutableSet.copyOf(parentPaths);
        this.parentNodeNames = parentNodeNames == null ? null : ImmutableSet.copyOf(parentNodeNames);
        this.parentNodeTypes = parentNodeTypes == null ? null : ImmutableSet.copyOf(parentNodeTypes);
        this.propertyNames = propertyNames == null ? null : ImmutableSet.copyOf(propertyNames);
        this.allNodeTypes = allNodeTypes == null ? null : ImmutableSet.copyOf(allNodeTypes);
    }

    @Override
    public String toString() {
        return "ChangeSet{paths[maxDepth:" + maxPathDepth + "]=" + parentPaths + ", propertyNames=" + propertyNames
                + ", parentNodeNames=" + parentNodeNames + ", parentNodeTypes=" + parentNodeTypes 
                + ", allNodeTypes=" + allNodeTypes + ", any overflow: " + anyOverflow() + "}";
    }

    @CheckForNull
    public Set<String> getParentPaths() {
        return parentPaths;
    }

    @CheckForNull
    public Set<String> getParentNodeNames() {
        return parentNodeNames;
    }

    @CheckForNull
    public Set<String> getParentNodeTypes() {
        return parentNodeTypes;
    }

    @CheckForNull
    public Set<String> getPropertyNames() {
        return propertyNames;
    }

    public int getMaxPrefilterPathDepth() {
        return maxPathDepth;
    }

    @CheckForNull
    public Set<String> getAllNodeTypes() {
        return allNodeTypes;
    }
    
    public boolean anyOverflow() {
        return getAllNodeTypes() == null || 
                getParentNodeNames() == null ||
                getParentNodeTypes() == null ||
                getParentPaths() == null ||
                getPropertyNames() == null;
    }

}