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
package org.apache.jackrabbit.oak.plugins.index.elastic;

import com.google.common.collect.Lists;
import org.apache.jackrabbit.oak.InitialContent;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.index.search.FulltextIndexConstants;
import org.apache.jackrabbit.oak.plugins.index.search.IndexFormatVersion;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeStore;
import org.apache.jackrabbit.oak.plugins.name.NamespaceEditorProvider;
import org.apache.jackrabbit.oak.plugins.nodetype.TypeEditorProvider;
import org.apache.jackrabbit.oak.plugins.nodetype.write.NodeTypeRegistry;
import org.apache.jackrabbit.oak.plugins.tree.factories.RootFactory;
import org.apache.jackrabbit.oak.spi.commit.CompositeEditorProvider;
import org.apache.jackrabbit.oak.spi.commit.EditorHook;
import org.apache.jackrabbit.oak.spi.state.ApplyDiff;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.jetbrains.annotations.NotNull;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.util.Calendar;
import java.util.Collections;
import java.util.List;

import static com.google.common.collect.Lists.newArrayList;
import static org.apache.jackrabbit.JcrConstants.JCR_CONTENT;
import static org.apache.jackrabbit.JcrConstants.JCR_DATA;
import static org.apache.jackrabbit.JcrConstants.JCR_PRIMARYTYPE;
import static org.apache.jackrabbit.JcrConstants.JCR_SYSTEM;
import static org.apache.jackrabbit.JcrConstants.NT_FILE;
import static org.apache.jackrabbit.oak.api.Type.NAME;
import static org.apache.jackrabbit.oak.plugins.memory.BinaryPropertyState.binaryProperty;
import static org.apache.jackrabbit.oak.spi.nodetype.NodeTypeConstants.JCR_NODE_TYPES;
import static org.junit.Assert.fail;

public class ElasticIndexAggregationNtFileTest extends ElasticAbstractQueryTest {
    private static final String NT_TEST_ASSET = "test:Asset";

    @Override
    protected InitialContent getInitialContent() {
        return new InitialContent() {

            @Override
            public void initialize(@NotNull NodeBuilder builder) {
                super.initialize(builder);
                // registering additional node types for wider testing
                InputStream stream = null;
                try {
                    stream = ElasticIndexAggregationNtFileTest.class
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

        };
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

    @Override
    protected void createTestIndexNode() throws Exception {
        Tree index = root.getTree("/");
        Tree indexDefn = createTestIndexNode(index, ElasticIndexDefinition.TYPE_ELASTICSEARCH);
        indexDefn.setProperty(FulltextIndexConstants.COMPAT_MODE, IndexFormatVersion.V2.getVersion());
        Tree includeNtFileContent = indexDefn.addChild(FulltextIndexConstants.AGGREGATES)
                .addChild(NT_TEST_ASSET).addChild("include10");
        includeNtFileContent.setProperty(FulltextIndexConstants.AGG_RELATIVE_NODE, true);
        includeNtFileContent.setProperty(FulltextIndexConstants.AGG_PATH, "jcr:content/renditions/dam.text.txt/jcr:content");
        root.commit();
    }

    @Test
    public void indexNtFileText() throws CommitFailedException {
        setTraversalEnabled(false);
        final String statement = "//element(*, test:Asset)[ " +
                "jcr:contains(jcr:content/renditions/dam.text.txt/jcr:content, 'quick') ]";
        List<String> expected = newArrayList();
        Tree content = root.getTree("/").addChild("content");
        Tree page = content.addChild("asset");
        page.setProperty(JCR_PRIMARYTYPE, NT_TEST_ASSET, NAME);
        Tree ntfile = page.addChild(JCR_CONTENT).addChild("renditions").addChild("dam.text.txt");
        ntfile.setProperty(JCR_PRIMARYTYPE, NT_FILE, Type.NAME);
        Tree resource = ntfile.addChild(JCR_CONTENT);
        resource.setProperty(JCR_PRIMARYTYPE, "nt:resource", Type.NAME);
        resource.setProperty("jcr:lastModified", Calendar.getInstance());
        resource.setProperty("jcr:encoding", "UTF-8");
        resource.setProperty("jcr:mimeType", "text/plain");
        resource.setProperty(binaryProperty(JCR_DATA,
                "the quick brown fox jumps over the lazy dog."));
        root.commit();
        expected.add("/content/asset");

        assertEventually(()-> {
            assertQuery(statement, "xpath", expected);
        });
    }
}
