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

import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
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
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;

public class LuceneIndexAggregation2Test extends IndexAggregation2CommonTest {

    private final ExecutorService executorService = Executors.newFixedThreadPool(2);
    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder(new File("target"));

    @Override
    protected ContentRepository createRepository() {
        indexOptions = new LuceneIndexOptions();
        repositoryOptionsUtil = new LuceneTestRepositoryBuilder(executorService,
            temporaryFolder).build();
        LuceneIndexProvider provider = new LuceneIndexProvider();

        return new Oak()
            .with(new InitialContent() {

                @Override
                public void initialize(@NotNull NodeBuilder builder) {
                    super.initialize(builder);

                    // registering additional node types for wider testing
                    InputStream stream = null;
                    try {
                        stream = Thread.currentThread().getContextClassLoader()
                                       .getResource("test_nodetypes.cnd").openStream();
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

}
