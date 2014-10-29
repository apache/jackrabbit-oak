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

import static com.google.common.collect.Lists.newArrayList;
import static org.apache.jackrabbit.JcrConstants.JCR_PRIMARYTYPE;
import static org.apache.jackrabbit.JcrConstants.JCR_SYSTEM;
import static org.apache.jackrabbit.JcrConstants.NT_FILE;
import static org.apache.jackrabbit.JcrConstants.NT_UNSTRUCTURED;
import static org.apache.jackrabbit.oak.api.Type.NAME;
import static org.apache.jackrabbit.oak.api.Type.STRING;
import static org.apache.jackrabbit.oak.plugins.nodetype.NodeTypeConstants.JCR_NODE_TYPES;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.List;

import org.apache.jackrabbit.oak.Oak;
import org.apache.jackrabbit.oak.api.ContentRepository;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.core.SystemRoot;
import org.apache.jackrabbit.oak.plugins.index.aggregate.AggregateIndexProvider;
import org.apache.jackrabbit.oak.plugins.index.aggregate.NodeAggregator;
import org.apache.jackrabbit.oak.plugins.index.aggregate.SimpleNodeAggregator;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeStore;
import org.apache.jackrabbit.oak.plugins.name.NamespaceEditorProvider;
import org.apache.jackrabbit.oak.plugins.nodetype.TypeEditorProvider;
import org.apache.jackrabbit.oak.plugins.nodetype.write.InitialContent;
import org.apache.jackrabbit.oak.plugins.nodetype.write.NodeTypeRegistry;
import org.apache.jackrabbit.oak.query.AbstractQueryTest;
import org.apache.jackrabbit.oak.spi.commit.CompositeEditorProvider;
import org.apache.jackrabbit.oak.spi.commit.EditorHook;
import org.apache.jackrabbit.oak.spi.commit.Observer;
import org.apache.jackrabbit.oak.spi.security.OpenSecurityProvider;
import org.apache.jackrabbit.oak.spi.state.ApplyDiff;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

public class LuceneIndexAggregationTest2 extends AbstractQueryTest {
    private static final Logger LOG = LoggerFactory.getLogger(LuceneIndexAggregationTest2.class);
    
    private static final String NT_TEST_PAGE = "test:Page";
    private static final String NT_TEST_PAGECONTENT = "test:PageContent";
    private static final String NT_TEST_ASSET = "test:Asset";
    private static final String NT_TEST_ASSETCONTENT = "test:AssetContent";
    
    @Override
    protected ContentRepository createRepository() {
        LuceneIndexProvider provider = new LuceneIndexProvider();
        
        return new Oak()
            .with(new InitialContent() {

                @Override
                public void initialize(NodeBuilder builder) {
                    super.initialize(builder);

                    // registering additional node types for wider testing
                    InputStream stream = null;
                    try {
                        stream = LuceneIndexAggregationTest2.class
                            .getResourceAsStream("test_nodetypes.cnd");
                        NodeState base = builder.getNodeState();
                        NodeStore store = new MemoryNodeStore(base);

                        Root root = new SystemRoot(store, new EditorHook(
                            new CompositeEditorProvider(new NamespaceEditorProvider(),
                                new TypeEditorProvider())));

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
            .with(AggregateIndexProvider.wrap(provider.with(getNodeAggregator())))
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
    
    @Override
    protected void createTestIndexNode() throws Exception {
        Tree index = root.getTree("/");
        createTestIndexNode(index, LuceneIndexConstants.TYPE_LUCENE);
        root.commit();
    }

    private static NodeAggregator getNodeAggregator() {
        return new SimpleNodeAggregator()
            .newRuleWithName(NT_FILE, newArrayList("jcr:content"))
            .newRuleWithName(NT_TEST_PAGE, newArrayList("jcr:content"))
            .newRuleWithName(NT_TEST_PAGECONTENT, newArrayList("*", "*/*", "*/*/*", "*/*/*/*"))
            .newRuleWithName(NT_TEST_ASSET, newArrayList("jcr:content"))
            .newRuleWithName(
                NT_TEST_ASSETCONTENT,
                newArrayList("metadata", "renditions", "renditions/original", "comments",
                    "renditions/original/jcr:content"))
            .newRuleWithName("rep:User", newArrayList("profile"));
    }
        
    @Test
    public void oak2226() throws Exception {
        //setTraversalEnabled(false);
        final String statement = "/jcr:root/content//element(*, test:Asset)[" +
            "(jcr:contains(., 'mountain')) " +
            "and (jcr:contains(jcr:content/metadata/@format, 'image'))]";
        Tree content = root.getTree("/").addChild("content");
        List<String> expected = Lists.newArrayList();
        
        /*
         * creating structure
         *  "/content" : {
         *      "node" : {
         *          "jcr:primaryType" : "test:Asset",
         *          "jcr:content" : {
         *              "jcr:primaryType" : "test:AssetContent",
         *              "metadata" : {
         *                  "jcr:primaryType" : "nt:unstructured",
         *                  "title" : "Lorem mountain ipsum",
         *                  "format" : "image/jpeg"
         *              }
         *          }
         *      },
         *      "mountain-node" : {
         *          "jcr:primaryType" : "test:Asset",
         *          "jcr:content" : {
         *              "jcr:primaryType" : "test:AssetContent",
         *              "metadata" : {
         *                  "jcr:primaryType" : "nt:unstructured",
         *                  "format" : "image/jpeg"
         *              }
         *          }
         *      }
         *  }
         */
        
        
        // adding a node with 'mountain' property
        Tree node = content.addChild("node");
        node.setProperty(JCR_PRIMARYTYPE, NT_TEST_ASSET, NAME);
        expected.add(node.getPath());
        node = node.addChild("jcr:content");
        node.setProperty(JCR_PRIMARYTYPE, NT_TEST_ASSETCONTENT, NAME);
        node = node.addChild("metadata");
        node.setProperty(JCR_PRIMARYTYPE, NT_UNSTRUCTURED, NAME);
        node.setProperty("title", "Lorem mountain ipsum", STRING);
        node.setProperty("format", "image/jpeg", STRING);
        
        // adding a node with 'mountain' name but not property
        node = content.addChild("mountain-node");
        node.setProperty(JCR_PRIMARYTYPE, NT_TEST_ASSET, NAME);
        expected.add(node.getPath());
        node = node.addChild("jcr:content");
        node.setProperty(JCR_PRIMARYTYPE, NT_TEST_ASSETCONTENT, NAME);
        node = node.addChild("metadata");
        node.setProperty(JCR_PRIMARYTYPE, NT_UNSTRUCTURED, NAME);
        node.setProperty("format", "image/jpeg", STRING);
        
        root.commit();

        assertQuery(statement, "xpath", expected);
        // setTraversalEnabled(true);
    }

}
