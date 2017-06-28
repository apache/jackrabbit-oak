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
package org.apache.jackrabbit.oak.plugins.index.aggregate;

import static com.google.common.collect.Lists.newArrayList;
import static org.apache.jackrabbit.JcrConstants.JCR_PRIMARYTYPE;
import static org.apache.jackrabbit.oak.api.Type.STRING;
import static org.apache.jackrabbit.oak.commons.PathUtils.elements;
import static org.apache.jackrabbit.oak.commons.PathUtils.getDepth;
import static org.apache.jackrabbit.oak.commons.PathUtils.getName;
import static org.apache.jackrabbit.oak.commons.PathUtils.getParentPath;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.spi.query.QueryIndex;
import org.apache.jackrabbit.oak.spi.state.NodeState;

import com.google.common.collect.Iterators;

/**
 * List based NodeAggregator
 * 
 */
public class SimpleNodeAggregator implements QueryIndex.NodeAggregator {

    public static final String INCLUDE_ALL = "*";

    private final List<ChildNameRule> aggregates = new ArrayList<ChildNameRule>();

    @Override
    public Iterator<String> getParents(NodeState root, String path) {
        return getParents(root, path, true);
    }

    private Iterator<String> getParents(NodeState root, String path,
            boolean acceptStarIncludes) {

        int levelsUp = 0;
        Set<String> primaryType = new HashSet<String>();
        for (ChildNameRule r : aggregates) {
            String name = getName(path);
            for (String inc : r.includes) {
                // check node name match
                if (name.equals(getName(inc))) {
                    levelsUp = getDepth(inc);
                    primaryType.add(r.primaryType);
                    if (acceptStarIncludes) {
                        break;
                    }
                }
                if (acceptStarIncludes) {
                    // check '*' rule, which could span over more than one level
                    if (INCLUDE_ALL.equals(getName(inc))) {
                        // basic approach to dealing with a '*' include
                        levelsUp = Math.max(getDepth(inc), levelsUp);
                        primaryType.add(r.primaryType);
                    }
                }
            }
        }
        if (levelsUp > 0 && !primaryType.isEmpty()) {
            List<String> parents = new ArrayList<String>();
            levelsUp = Math.min(levelsUp, getDepth(path));
            String parentPath = path;
            for (int i = 0; i < levelsUp; i++) {
                parentPath = getParentPath(parentPath);
                if (isNodeType(root, parentPath, primaryType)) {
                    parents.add(parentPath);
                    parents.addAll(newArrayList(getParents(root, parentPath,
                            false)));
                    return parents.iterator();
                }
            }
        }

        return Iterators.emptyIterator();
    }

    private static boolean isNodeType(NodeState root, String path, Set<String> types) {
        NodeState state = root;
        for (String p : elements(path)) {
            if (state.hasChildNode(p)) {
                state = state.getChildNode(p);
            } else {
                return false;
            }
        }
        PropertyState ps = state.getProperty(JCR_PRIMARYTYPE);
        if (ps == null) {
            return false;
        }
        return types.contains(ps.getValue(STRING));
    }

    // ----- builder methods

    /**
     * Include children with the provided name. '*' means include all children
     * 
     * Note: there is no support for property names yet
     * 
     */
    public SimpleNodeAggregator newRuleWithName(String primaryType,
            List<String> includes) {
        aggregates.add(new ChildNameRule(primaryType, includes));
        return this;
    }

    // ----- aggregation rules

    private static interface Rule {

    }

    private static class ChildNameRule implements Rule {

        private final String primaryType;
        private final List<String> includes;

        ChildNameRule(String primaryType, List<String> includes) {
            this.primaryType = primaryType;
            this.includes = includes;
        }
    }

}
