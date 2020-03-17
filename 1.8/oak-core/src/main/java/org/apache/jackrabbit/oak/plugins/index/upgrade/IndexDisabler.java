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

package org.apache.jackrabbit.oak.plugins.index.upgrade;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStateUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkNotNull;
import static java.util.Collections.emptyList;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.DECLARING_NODE_TYPES;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.DISABLE_INDEXES_ON_NEXT_CYCLE;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.SUPERSEDED_INDEX_PATHS;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.TYPE_DISABLED;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.TYPE_PROPERTY_NAME;

/**
 * Checks and mark old indexes as disabled. It looks for IndexConstants#SUPERSEDED_INDEX_PATHS
 * for the index paths which need to be marked as disabled. The index paths can refer to absolute
 * index path or nodeTypes like /oak:index/nodetype/@foo where 'foo' is one of the nodetype indexed
 * by /oak:index/nodetype
 */
public class IndexDisabler {
    private static final Logger log = LoggerFactory.getLogger(IndexDisabler.class);
    private final NodeBuilder rootBuilder;

    public IndexDisabler(NodeBuilder rootBuilder) {
        this.rootBuilder = rootBuilder;
    }

    public boolean markDisableFlagIfRequired(String currentIndexPath, NodeBuilder idxBuilder) {
        boolean disableRequired = isAnyIndexToBeDisabled(currentIndexPath, idxBuilder);
        if (disableRequired) {
            idxBuilder.setProperty(DISABLE_INDEXES_ON_NEXT_CYCLE, true);
        }
        return disableRequired;
    }

    private boolean isAnyIndexToBeDisabled(String currentIndexPath, NodeBuilder idxBuilder) {
        PropertyState indexPathsProp = idxBuilder.getProperty(SUPERSEDED_INDEX_PATHS);

        if (indexPathsProp == null) {
            return false;
        }

        Iterable<String> indexPaths = indexPathsProp.getValue(Type.STRINGS);
        for (final String indexPath : indexPaths) {
            if (isNodeTypePath(indexPath)) {
                String nodeTypeName = PathUtils.getName(indexPath).substring(1);
                String nodeTypeIndexPath = PathUtils.getParentPath(indexPath);

                NodeState idxSate = NodeStateUtils.getNode(rootBuilder.getBaseState(), nodeTypeIndexPath);
                PropertyState declaredNodeTypes = idxSate.getProperty(DECLARING_NODE_TYPES);
                if (idxSate.exists() && declaredNodeTypes != null){
                    if (Iterables.contains(declaredNodeTypes.getValue(Type.NAMES), nodeTypeName)) {
                        return true;
                    }
                }
            } else {
                NodeState idxSate = NodeStateUtils.getNode(rootBuilder.getBaseState(), indexPath);
                if (idxSate.exists() && !TYPE_DISABLED.equals(idxSate.getString(TYPE_PROPERTY_NAME))) {
                    return true;
                }
            }
        }

        return false;
    }

    public List<String> disableOldIndexes(String currentIndexPath, NodeBuilder idxBuilder) {
        PropertyState indexPathsProp = idxBuilder.getProperty(SUPERSEDED_INDEX_PATHS);

        if (indexPathsProp == null) {
            return emptyList();
        }

        if (!idxBuilder.getBoolean(DISABLE_INDEXES_ON_NEXT_CYCLE)) {
            return emptyList();
        }

        //Skip disabling for the cycle where reindexing just got completed
        //i.e. baseState should not have DISABLE_INDEXES_ON_NEXT_CYCLE set to false
        if (!idxBuilder.getBaseState().getBoolean(DISABLE_INDEXES_ON_NEXT_CYCLE)){
            return emptyList();
        }

        Iterable<String> indexPaths = indexPathsProp.getValue(Type.STRINGS);
        List<String> disabledIndexes = new ArrayList<>();
        for (final String indexPath : indexPaths) {
            if (isNodeTypePath(indexPath)) {
                String nodeTypeName = PathUtils.getName(indexPath).substring(1);
                String nodeTypeIndexPath = PathUtils.getParentPath(indexPath);

                NodeBuilder nodeTypeIndexBuilder = child(rootBuilder, nodeTypeIndexPath);
                PropertyState declaringNodeTypes = nodeTypeIndexBuilder.getProperty(DECLARING_NODE_TYPES);
                if (nodeTypeIndexBuilder.exists() && declaringNodeTypes != null){
                    Set<String> existingTypes = Sets.newHashSet(declaringNodeTypes.getValue(Type.NAMES));
                    if (existingTypes.remove(nodeTypeName)) {
                        disabledIndexes.add(indexPath);
                        nodeTypeIndexBuilder.setProperty(DECLARING_NODE_TYPES, existingTypes, Type.NAMES);
                    }
                }
            } else {
                NodeBuilder disabledIndexBuilder = child(rootBuilder, indexPath);
                if (disabledIndexBuilder.exists()) {
                    disabledIndexBuilder.setProperty(TYPE_PROPERTY_NAME, TYPE_DISABLED);
                    disabledIndexes.add(indexPath);
                }
            }
        }

        if (!disabledIndexes.isEmpty()) {
            log.info("Index at [{}] supersedes indexes {}. Marking those as disabled",
                    currentIndexPath, disabledIndexes);
            idxBuilder.removeProperty(DISABLE_INDEXES_ON_NEXT_CYCLE);
        }

        return disabledIndexes;
    }

    private static boolean isNodeTypePath(String indexPath) {
        String lastPathSegment = PathUtils.getName(indexPath);
        return lastPathSegment.startsWith("@");
    }

    private static NodeBuilder child(NodeBuilder nb, String path) {
        for (String name : PathUtils.elements(checkNotNull(path))) {
            nb = nb.getChildNode(name);
        }
        return nb;
    }
}
