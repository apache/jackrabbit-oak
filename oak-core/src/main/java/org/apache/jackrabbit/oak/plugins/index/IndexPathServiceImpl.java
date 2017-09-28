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

package org.apache.jackrabbit.oak.plugins.index;

import java.util.Iterator;

import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.plugins.index.nodetype.NodeTypeIndexProvider;
import org.apache.jackrabbit.oak.query.NodeStateNodeTypeInfoProvider;
import org.apache.jackrabbit.oak.query.QueryEngineSettings;
import org.apache.jackrabbit.oak.query.ast.NodeTypeInfo;
import org.apache.jackrabbit.oak.query.ast.NodeTypeInfoProvider;
import org.apache.jackrabbit.oak.query.ast.SelectorImpl;
import org.apache.jackrabbit.oak.query.index.FilterImpl;
import org.apache.jackrabbit.oak.spi.mount.MountInfoProvider;
import org.apache.jackrabbit.oak.spi.mount.Mounts;
import org.apache.jackrabbit.oak.spi.query.IndexRow;
import org.apache.jackrabbit.oak.spi.query.QueryIndex;
import org.apache.jackrabbit.oak.spi.state.ChildNodeEntry;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStateUtils;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.Reference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Iterables.filter;
import static com.google.common.collect.Iterables.transform;
import static com.google.common.collect.Iterators.transform;
import static org.apache.jackrabbit.JcrConstants.JCR_PRIMARYTYPE;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.DECLARING_NODE_TYPES;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.INDEX_DEFINITIONS_NODE_TYPE;

@Component
public class IndexPathServiceImpl implements IndexPathService {
    private final Logger log = LoggerFactory.getLogger(getClass());

    private final QueryEngineSettings settings = new QueryEngineSettings();

    @Reference
    private NodeStore nodeStore;

    @Reference
    private MountInfoProvider mountInfoProvider;

    public IndexPathServiceImpl() {
        //Required for SCR
        settings.setFailTraversal(true);
        settings.setLimitReads(Long.MAX_VALUE);
    }

    public IndexPathServiceImpl(NodeStore nodeStore) {
        this(nodeStore, Mounts.defaultMountInfoProvider());
    }

    public IndexPathServiceImpl(NodeStore nodeStore, MountInfoProvider mountInfoProvider) {
        this.nodeStore = nodeStore;
        this.mountInfoProvider = mountInfoProvider;
    }

    @Override
    public Iterable<String> getIndexPaths() {
        NodeState nodeType = NodeStateUtils.getNode(nodeStore.getRoot(), "/oak:index/nodetype");

        checkState("property".equals(nodeType.getString("type")), "nodetype index at " +
                "/oak:index/nodetype is found to be disabled. Cannot determine the paths of all indexes");

        //Check if oak:QueryIndexDefinition is indexed as part of nodetype index
        boolean indxDefnTypeIndexed = Iterables.contains(nodeType.getNames(DECLARING_NODE_TYPES), INDEX_DEFINITIONS_NODE_TYPE);

        if (!indxDefnTypeIndexed) {
            log.warn("{} is not found to be indexed as part of nodetype index. Non root indexes would " +
                    "not be listed", INDEX_DEFINITIONS_NODE_TYPE);
            NodeState oakIndex = nodeStore.getRoot().getChildNode("oak:index");
            return transform(filter(oakIndex.getChildNodeEntries(),
                    cne -> INDEX_DEFINITIONS_NODE_TYPE.equals(cne.getNodeState().getName(JCR_PRIMARYTYPE))),
                    cne -> PathUtils.concat("/oak:index", cne.getName()));
        }

        return () -> {
            Iterator<IndexRow> itr = getIndex().query(createFilter(INDEX_DEFINITIONS_NODE_TYPE), nodeStore.getRoot());
            return transform(itr, input -> input.getPath());
        };
    }

    private FilterImpl createFilter(String nodeTypeName) {
        NodeTypeInfoProvider nodeTypes = new NodeStateNodeTypeInfoProvider(nodeStore.getRoot());
        NodeTypeInfo type = nodeTypes.getNodeTypeInfo(nodeTypeName);
        SelectorImpl selector = new SelectorImpl(type, nodeTypeName);
        return new FilterImpl(selector, "SELECT * FROM [" + nodeTypeName + "]", settings);
    }

    private QueryIndex getIndex() {
        NodeTypeIndexProvider idxProvider = new NodeTypeIndexProvider();
        idxProvider.with(mountInfoProvider);
        return idxProvider.getQueryIndexes(nodeStore.getRoot()).get(0);
    }
}
