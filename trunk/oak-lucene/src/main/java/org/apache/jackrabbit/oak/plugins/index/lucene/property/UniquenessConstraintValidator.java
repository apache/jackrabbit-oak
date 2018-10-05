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

package org.apache.jackrabbit.oak.plugins.index.lucene.property;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Multimap;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStateUtils;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.jackrabbit.oak.api.CommitFailedException.CONSTRAINT;

/**
 * Performs validation related to unique index by ensuring that for
 * given property value only one indexed entry is present. The query
 * is performed against multiple stores
 *
 *   - Property storage - Stores the recently added unique keys via UniqueStore strategy
 *   - Lucene storage - Stores the long term index in lucene
 */
public class UniquenessConstraintValidator {
    private final NodeState rootState;
    private final String indexPath;
    private final Multimap<String, String> uniqueKeys = HashMultimap.create();
    private final PropertyQuery firstStore;
    private PropertyQuery secondStore = PropertyQuery.DEFAULT;

    public UniquenessConstraintValidator(String indexPath, NodeBuilder builder, NodeState rootState) {
        this.indexPath = indexPath;
        this.rootState = rootState;
        this.firstStore = new PropertyIndexQuery(builder);
    }

    public void add(String propertyRelativePath, Set<String> afterKeys) {
        uniqueKeys.putAll(propertyRelativePath, afterKeys);
    }

    public void validate() throws CommitFailedException {
        for (Map.Entry<String, String> e : uniqueKeys.entries()) {
            String propertyRelativePath = e.getKey();
            String value = e.getValue();
            Iterable<String> indexedPaths = getIndexedPaths(propertyRelativePath, value);
            Set<String> allPaths = ImmutableSet.copyOf(indexedPaths);

            //If more than one match found then filter out stale paths
            if (allPaths.size() > 1) {
                allPaths = getValidPaths(allPaths, propertyRelativePath, value);
            }

            if (allPaths.size() > 1) {
                String msg = String.format("Uniqueness constraint violated for property [%s] with value [%s] for " +
                        "index [%s]. Indexed paths %s", propertyRelativePath, value, indexPath, allPaths);
                throw new CommitFailedException(CONSTRAINT, 30, msg);
            }
        }
    }

    public void setSecondStore(PropertyQuery secondStore) {
        this.secondStore = checkNotNull(secondStore);
    }

    private Iterable<String> getIndexedPaths(String propertyRelativePath, String value) {
        return Iterables.concat(
                firstStore.getIndexedPaths(propertyRelativePath, value),
                secondStore.getIndexedPaths(propertyRelativePath, value)
        );
    }

    /**
     * Paths reported by indexes may be based on stale data. So revalidate by checking reported paths are
     * valid and refers to indexed value or not
     */
    private Set<String> getValidPaths(Set<String> allPaths, String propertyRelativePath, String value) {
        Set<String> validPaths = new HashSet<>();
        for (String path : allPaths) {
            NodeState node = NodeStateUtils.getNode(rootState, path);
            if (!node.exists()) {
                continue;
            }

            PropertyState uniqueProp = getValue(node, propertyRelativePath);
            if (uniqueProp == null) {
                continue;
            }

            //Property can be MVP. So check if any of them matches
            for (String v : uniqueProp.getValue(Type.STRINGS)) {
                if (v.equals(value)) {
                    validPaths.add(path);
                    break;
                }
            }
        }
        return validPaths;
    }

    private static PropertyState getValue(NodeState node, String propertyRelativePath) {
        int depth = PathUtils.getDepth(propertyRelativePath);
        NodeState propNode = node;
        String propName = propertyRelativePath;
        if (depth > 1) {
            propName = PathUtils.getName(propertyRelativePath);
            String parentPath = PathUtils.getParentPath(propertyRelativePath);
            propNode = NodeStateUtils.getNode(node, parentPath);
        }
        return propNode.getProperty(propName);
    }
}
