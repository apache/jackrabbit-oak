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

import java.util.List;
import java.util.Set;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.oak.InitialContent;
import org.apache.jackrabbit.oak.Oak;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.ContentRepository;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.index.nodetype.NodeTypeIndexProvider;
import org.apache.jackrabbit.oak.plugins.index.property.PropertyIndexEditorProvider;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeStore;
import org.apache.jackrabbit.oak.query.AbstractQueryTest;
import org.apache.jackrabbit.oak.spi.security.OpenSecurityProvider;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.junit.Test;

import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.DECLARING_NODE_TYPES;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.INDEX_DEFINITIONS_NODE_TYPE;
import static org.hamcrest.Matchers.hasItem;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

public class IndexPathServiceImplTest extends AbstractQueryTest {

    private NodeStore nodeStore = new MemoryNodeStore();
    private IndexPathService indexPathService = new IndexPathServiceImpl(nodeStore);

    @Override
    protected ContentRepository createRepository() {
        return new Oak(nodeStore).with(new InitialContent())
                .with(new OpenSecurityProvider())
                .with(new NodeTypeIndexProvider())
                .with(new PropertyIndexEditorProvider())
                .createContentRepository();
    }

    @Test
    public void noErrorIfQueryDefinitionsNotIndexed() throws Exception{
        Set<String> paths = Sets.newHashSet(indexPathService.getIndexPaths());
        assertThat(paths, hasItem("/oak:index/uuid"));
    }

    @Test(expected = IllegalStateException.class)
    public void errorIfNodetypeIndexDisabled() throws Exception{
        Tree tree = root.getTree("/oak:index/nodetype");
        tree.setProperty("type", "disabled");
        root.commit();
        indexPathService.getIndexPaths();
    }

    @Test
    public void nodeTypeIndexed() throws Exception{
        enableIndexDefinitionIndex();
        Set<String> paths = Sets.newHashSet(indexPathService.getIndexPaths());
        assertThat(paths, hasItem("/oak:index/uuid"));
        assertThat(paths, hasItem("/oak:index/nodetype"));
        assertThat(paths, hasItem("/oak:index/reference"));
    }

    @Test
    public void indexInSubTree() throws Exception{
        enableIndexDefinitionIndex();
        Tree tree = root.getTree("/").addChild("a").addChild("b");
        Tree fooIndex = tree.addChild("oak:index").addChild("fooIndex");
        fooIndex.setProperty(JcrConstants.JCR_PRIMARYTYPE, INDEX_DEFINITIONS_NODE_TYPE, Type.NAME);
        fooIndex.setProperty("type", "disabled");
        root.commit();

        Set<String> paths = Sets.newHashSet(indexPathService.getIndexPaths());
        assertThat(paths, hasItem("/a/b/oak:index/fooIndex"));
    }

    private void enableIndexDefinitionIndex() throws CommitFailedException {
        Tree nodetype = root.getTree("/oak:index/nodetype");
        assertTrue(nodetype.exists());

        List<String> nodetypes = Lists.newArrayList();
        if (nodetype.hasProperty(DECLARING_NODE_TYPES)){
            nodetypes = Lists.newArrayList(nodetype.getProperty(DECLARING_NODE_TYPES).getValue(Type.STRINGS));
        }

        nodetypes.add(INDEX_DEFINITIONS_NODE_TYPE);
        nodetype.setProperty(DECLARING_NODE_TYPES, nodetypes, Type.NAMES);
        nodetype.setProperty(IndexConstants.REINDEX_PROPERTY_NAME, true);
        root.commit();
    }

}