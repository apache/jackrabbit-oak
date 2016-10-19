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

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

/**
 * Builder of a ChangeSet - only used by ChangeCollectorProvider (and tests..)
 */
public class ChangeSetBuilder {

    private final int maxItems;
    private final int maxPathDepth;
    private final Set<String> parentPaths = Sets.newHashSet();
    private final Set<String> parentNodeNames = Sets.newHashSet();
    private final Set<String> parentNodeTypes = Sets.newHashSet();
    private final Set<String> propertyNames = Sets.newHashSet();

    private boolean parentPathOverflow;
    private boolean parentNodeNameOverflow;
    private boolean parentNodeTypeOverflow;
    private boolean propertyNameOverflow;

    public ChangeSetBuilder(int maxItems, int maxPathDepth) {
        this.maxItems = maxItems;
        this.maxPathDepth = maxPathDepth;
    }

    @Override
    public String toString() {
        return "ChangeSetBuilder{paths[maxDepth:" + maxPathDepth + "]=" + parentPaths + ", propertyNames="
                + propertyNames + ", nodeNames=" + parentNodeNames + ", nodeTypes=" + parentNodeTypes + "}";
    }

    public boolean getParentPathOverflown() {
        return parentPathOverflow;
    }

    public Set<String> getParentPaths() {
        if (parentPathOverflow || parentPaths.size() > maxItems) {
            // if already overflown, reset the buffers anyway
            parentPathOverflow = true;
            parentPaths.clear();
        }
        return parentPaths;
    }

    public boolean getParentNodeNameOverflown() {
        return parentNodeNameOverflow;
    }

    public Set<String> getParentNodeNames() {
        if (parentNodeNameOverflow || parentNodeNames.size() > maxItems) {
            // if already overflown, reset the buffers anyway
            parentNodeNameOverflow = true;
            parentNodeNames.clear();
        }
        return parentNodeNames;
    }

    public boolean getParentNodeTypeOverflown() {
        return parentNodeTypeOverflow;
    }

    public Set<String> getParentNodeTypes() {
        if (parentNodeTypeOverflow || parentNodeTypes.size() > maxItems) {
            // if already overflown, reset the buffers anyway
            parentNodeTypeOverflow = true;
            parentNodeTypes.clear();
        }
        return parentNodeTypes;
    }

    public boolean getPropertyNameOverflown() {
        return propertyNameOverflow;
    }

    public Set<String> getPropertyNames() {
        if (propertyNameOverflow || propertyNames.size() > maxItems) {
            // if already overflown, reset the buffers anyway
            propertyNameOverflow = true;
            propertyNames.clear();
        }
        return propertyNames;
    }

    public int getMaxPrefilterPathDepth() {
        return maxPathDepth;
    }

    public ChangeSet build() {
        // invoke accessors to get overflow evaluated one last time
        getParentPaths();
        getParentNodeNames();
        getParentNodeTypes();
        getPropertyNames();
        return new ChangeSet(maxPathDepth, parentPathOverflow ? null : parentPaths,
                parentNodeNameOverflow ? null : parentNodeNames,
                parentNodeTypeOverflow ? null : parentNodeTypes,
                propertyNameOverflow ? null : propertyNames);
    }

}
