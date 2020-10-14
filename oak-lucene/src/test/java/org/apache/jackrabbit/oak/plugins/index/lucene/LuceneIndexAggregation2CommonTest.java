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
package org.apache.jackrabbit.oak.plugins.index.lucene;

import com.google.common.collect.Lists;
import org.apache.jackrabbit.oak.InitialContent;
import org.apache.jackrabbit.oak.Oak;
import org.apache.jackrabbit.oak.api.ContentRepository;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.plugins.index.IndexAggregation2CommonTest;
import org.apache.jackrabbit.oak.plugins.index.LuceneIndexOptions;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeStore;
import org.apache.jackrabbit.oak.plugins.name.NamespaceEditorProvider;
import org.apache.jackrabbit.oak.plugins.nodetype.TypeEditorProvider;
import org.apache.jackrabbit.oak.plugins.nodetype.write.NodeTypeRegistry;
import org.apache.jackrabbit.oak.plugins.tree.factories.RootFactory;
import org.apache.jackrabbit.oak.spi.commit.CompositeEditorProvider;
import org.apache.jackrabbit.oak.spi.commit.EditorHook;
import org.apache.jackrabbit.oak.spi.commit.Observer;
import org.apache.jackrabbit.oak.spi.query.QueryIndexProvider;
import org.apache.jackrabbit.oak.spi.security.OpenSecurityProvider;
import org.apache.jackrabbit.oak.spi.state.ApplyDiff;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.jetbrains.annotations.NotNull;
import org.junit.After;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.List;

import static org.apache.jackrabbit.JcrConstants.JCR_SYSTEM;
import static org.apache.jackrabbit.oak.spi.nodetype.NodeTypeConstants.JCR_NODE_TYPES;
import static org.junit.Assert.fail;

public class LuceneIndexAggregation2CommonTest extends IndexAggregation2CommonTest {
    private static final Logger LOG = LoggerFactory.getLogger(LuceneIndexAggregation2CommonTest.class);

    @Override
    protected ContentRepository createRepository() {
        indexOptions = new LuceneIndexOptions();
        LuceneIndexProvider provider = new LuceneIndexProvider();

        return new Oak()
                .with(new InitialContent() {

                    @Override
                    public void initialize(@NotNull NodeBuilder builder) {
                        super.initialize(builder);

                        // registering additional node types for wider testing
                        InputStream stream = null;
                        try {
                            stream = LuceneIndexAggregation2CommonTest.class
                                    .getResourceAsStream("test_nodetypes.cnd");
                            NodeState base = builder.getNodeState();
                            NodeStore store = new MemoryNodeStore(base);

                            Root root = RootFactory.createSystemRoot(store, new EditorHook(
                                    new CompositeEditorProvider(new NamespaceEditorProvider(),
                                            new TypeEditorProvider())), null, null, null);

                            NodeTypeRegistry.register(root, stream, "testing node types");

                            NodeState target = store.getRoot();
                            target.compareAgainstBaseState(base, new ApplyDiff(builder));
                        } catch (Exception e) {
                            LOG.error("Error while registering required node types. Failing here", e);
                            fail("Error while registering required node types");
                        } finally {
                            printNodeTypes(builder);
                            if (stream != null) {
                                try {
                                    stream.close();
                                } catch (IOException e) {
                                    LOG.debug("Ignoring exception on stream closing.", e);
                                }
                            }
                        }

                    }

                })
                .with(new OpenSecurityProvider())
                .with(((QueryIndexProvider) provider.with(getNodeAggregator())))
                .with((Observer) provider).with(new LuceneIndexEditorProvider())
                .createContentRepository();
    }

    /**
     * convenience method for printing on logs the currently registered node types.
     *
     * @param builder
     */
    private static void printNodeTypes(NodeBuilder builder) {
        if (LOG.isDebugEnabled()) {
            NodeBuilder namespace = builder.child(JCR_SYSTEM).child(JCR_NODE_TYPES);
            List<String> nodes = Lists.newArrayList(namespace.getChildNodeNames());
            Collections.sort(nodes);
            for (String node : nodes) {
                LOG.debug(node);
            }
        }
    }

}
