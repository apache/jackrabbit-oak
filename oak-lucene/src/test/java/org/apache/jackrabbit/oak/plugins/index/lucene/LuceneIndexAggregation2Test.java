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

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.collect.ImmutableList.of;
import static com.google.common.collect.Lists.newArrayList;
import static org.apache.jackrabbit.JcrConstants.JCR_CONTENT;
import static org.apache.jackrabbit.JcrConstants.JCR_PRIMARYTYPE;
import static org.apache.jackrabbit.JcrConstants.JCR_SYSTEM;
import static org.apache.jackrabbit.JcrConstants.NT_FILE;
import static org.apache.jackrabbit.JcrConstants.NT_UNSTRUCTURED;
import static org.apache.jackrabbit.oak.api.Type.NAME;
import static org.apache.jackrabbit.oak.api.Type.STRING;
import static org.apache.jackrabbit.oak.api.Type.STRINGS;
import static org.apache.jackrabbit.oak.plugins.index.lucene.TestUtil.newNodeAggregator;
import static org.apache.jackrabbit.oak.plugins.index.lucene.TestUtil.useV2;
import static org.apache.jackrabbit.oak.plugins.nodetype.NodeTypeConstants.JCR_NODE_TYPES;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.List;

import javax.annotation.Nonnull;

import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.oak.Oak;
import org.apache.jackrabbit.oak.api.ContentRepository;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.plugins.index.aggregate.SimpleNodeAggregator;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeStore;
import org.apache.jackrabbit.oak.plugins.memory.PropertyStates;
import org.apache.jackrabbit.oak.plugins.name.NamespaceEditorProvider;
import org.apache.jackrabbit.oak.plugins.nodetype.TypeEditorProvider;
import org.apache.jackrabbit.oak.InitialContent;
import org.apache.jackrabbit.oak.plugins.nodetype.write.NodeTypeRegistry;
import org.apache.jackrabbit.oak.plugins.tree.RootFactory;
import org.apache.jackrabbit.oak.query.AbstractQueryTest;
import org.apache.jackrabbit.oak.spi.commit.CompositeEditorProvider;
import org.apache.jackrabbit.oak.spi.commit.EditorHook;
import org.apache.jackrabbit.oak.spi.commit.Observer;
import org.apache.jackrabbit.oak.spi.query.QueryIndex;
import org.apache.jackrabbit.oak.spi.query.QueryIndexProvider;
import org.apache.jackrabbit.oak.spi.security.OpenSecurityProvider;
import org.apache.jackrabbit.oak.spi.state.ApplyDiff;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.apache.jackrabbit.oak.plugins.tree.TreeUtil;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;

public class LuceneIndexAggregation2Test extends AbstractQueryTest {
    private static final Logger LOG = LoggerFactory.getLogger(LuceneIndexAggregation2Test.class);
    
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
                public void initialize(@Nonnull NodeBuilder builder) {
                    super.initialize(builder);

                    // registering additional node types for wider testing
                    InputStream stream = null;
                    try {
                        stream = LuceneIndexAggregation2Test.class
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
            .with(((QueryIndexProvider)provider.with(getNodeAggregator())))
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
        Tree indexDefn = createTestIndexNode(index, LuceneIndexConstants.TYPE_LUCENE);
        useV2(indexDefn);
        //Aggregates
        newNodeAggregator(indexDefn)
                .newRuleWithName(NT_FILE, newArrayList("jcr:content"))
                .newRuleWithName(NT_TEST_PAGE, newArrayList("jcr:content"))
                .newRuleWithName(NT_TEST_PAGECONTENT, newArrayList("*", "*/*", "*/*/*", "*/*/*/*"))
                .newRuleWithName(NT_TEST_ASSET, newArrayList("jcr:content"))
                .newRuleWithName(
                        NT_TEST_ASSETCONTENT,
                        newArrayList("metadata", "renditions", "renditions/original", "comments",
                                "renditions/original/jcr:content"))
                .newRuleWithName("rep:User", newArrayList("profile"));

        Tree originalInclude = indexDefn.getChild(LuceneIndexConstants.AGGREGATES)
                .getChild(NT_TEST_ASSET).addChild("includeOriginal");
        originalInclude.setProperty(LuceneIndexConstants.AGG_RELATIVE_NODE, true);
        originalInclude.setProperty(LuceneIndexConstants.AGG_PATH, "jcr:content/renditions/original");

        Tree includeSingleRel = indexDefn.getChild(LuceneIndexConstants.AGGREGATES)
            .getChild(NT_TEST_ASSET).addChild("includeFirstLevelChild");
        includeSingleRel.setProperty(LuceneIndexConstants.AGG_RELATIVE_NODE, true);
        includeSingleRel.setProperty(LuceneIndexConstants.AGG_PATH, "firstLevelChild");

        //Include all properties
        Tree props = TestUtil.newRulePropTree(indexDefn, "test:Asset");
        TestUtil.enableForFullText(props, "jcr:content/metadata/format");
        TestUtil.enableForFullText(props, LuceneIndexConstants.REGEX_ALL_PROPS, true);
        root.commit();
    }

    private static QueryIndex.NodeAggregator getNodeAggregator() {
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
        setTraversalEnabled(false);
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
        setTraversalEnabled(true);
    }

    @Test
    public void oak2249() throws Exception {
        setTraversalEnabled(false);
        final String statement = "//element(*, test:Asset)[ " +
            "( " +
                "jcr:contains(., 'summer') " +
                "or " + 
                "jcr:content/metadata/@tags = 'namespace:season/summer' " +
            ") and " +
                "jcr:contains(jcr:content/metadata/@format, 'image') " +
            "]"; 
        
        Tree content = root.getTree("/").addChild("content");
        List<String> expected = newArrayList();
        
        Tree metadata = createAssetStructure(content, "tagged");
        metadata.setProperty("tags", of("namespace:season/summer"), STRINGS);
        metadata.setProperty("format", "image/jpeg", STRING);
        expected.add("/content/tagged");
        
        metadata = createAssetStructure(content, "titled");
        metadata.setProperty("title", "Lorem summer ipsum", STRING);
        metadata.setProperty("format", "image/jpeg", STRING);
        expected.add("/content/titled");

        metadata = createAssetStructure(content, "summer-node");
        metadata.setProperty("format", "image/jpeg", STRING);
        expected.add("/content/summer-node");
        
        // the following is NOT expected
        metadata = createAssetStructure(content, "winter-node");
        metadata.setProperty("tags", of("namespace:season/winter"), STRINGS);
        metadata.setProperty("title", "Lorem winter ipsum", STRING);
        metadata.setProperty("format", "image/jpeg", STRING);

        root.commit();
        
        assertQuery(statement, "xpath", expected);
        setTraversalEnabled(true);
    }

    @Test
    public void indexRelativeNode() throws Exception {
        setTraversalEnabled(false);
        final String statement = "//element(*, test:Asset)[ " +
                "jcr:contains(., 'summer') " +
                "and jcr:contains(jcr:content/renditions/original, 'fox')" +
                "and jcr:contains(jcr:content/metadata/@format, 'image') " +
                "]";

        Tree content = root.getTree("/").addChild("content");
        List<String> expected = newArrayList();

        Tree metadata = createAssetStructure(content, "tagged");
        metadata.setProperty("tags", of("namespace:season/summer"), STRINGS);
        metadata.setProperty("format", "image/jpeg", STRING);

        Tree original = metadata.getParent().addChild("renditions").addChild("original");
        original.setProperty(JcrConstants.JCR_PRIMARYTYPE, JcrConstants.NT_FILE);
        original.addChild("jcr:content").setProperty(PropertyStates.createProperty(JcrConstants.JCR_MIMETYPE, "text/plain"));
        original.addChild("jcr:content").setProperty(PropertyStates.createProperty("jcr:data", "fox jumps".getBytes()));

        expected.add("/content/tagged");
        root.commit();

        assertQuery(statement, "xpath", expected);

        //Update the reaggregated node and with that parent should be get updated
        Tree originalContent = TreeUtil.getTree(root.getTree("/"), "/content/tagged/jcr:content/renditions/original/jcr:content");
        originalContent.setProperty(PropertyStates.createProperty("jcr:data", "kiwi jumps".getBytes()));
        root.commit();
        assertQuery(statement, "xpath", Collections.<String>emptyList());
        setTraversalEnabled(true);
    }

    @Test
    public void indexSingleRelativeNode() throws Exception {
        setTraversalEnabled(false);
        final String statement = "//element(*, test:Asset)[ " +
            "jcr:contains(firstLevelChild, 'summer') ]";

        List<String> expected = newArrayList();

        Tree content = root.getTree("/").addChild("content");
        Tree page = content.addChild("pages");
        page.setProperty(JCR_PRIMARYTYPE, NT_TEST_ASSET, NAME);
        Tree child = page.addChild("firstLevelChild");
        child.setProperty("tag", "summer is here", STRING);
        root.commit();

        expected.add("/content/pages");
        assertQuery(statement, "xpath", expected);
    }


    /**
     * <p>
     * convenience method that create an "asset" structure like
     * </p>
     * 
     * <pre>
     *  "parent" : {
     *      "nodeName" : {
     *          "jcr:primaryType" : "test:Asset",
     *          "jcr:content" : {
     *              "jcr:primaryType" : "test:AssetContent",
     *              "metatada" : {
     *                  "jcr:primaryType" : "nt:unstructured"
     *              }
     *          }
     *      }
     *  }
     * </pre>
     * 
     * <p>
     *  and returns the {@code metadata} node
     * </p>
     * 
     * @param parent the parent under which creating the node
     * @param nodeName the node name to be used
     * @return the {@code metadata} node. See above for details
     */
    private static Tree createAssetStructure(@Nonnull final Tree parent, 
                                             @Nonnull final String nodeName) {
        checkNotNull(parent);
        checkArgument(!Strings.isNullOrEmpty(nodeName), "nodeName cannot be null or empty");
        
        Tree node = parent.addChild(nodeName);
        node.setProperty(JCR_PRIMARYTYPE, NT_TEST_ASSET, NAME);
        node = node.addChild(JCR_CONTENT);
        node.setProperty(JCR_PRIMARYTYPE, NT_TEST_ASSETCONTENT, NAME);
        node = node.addChild("metadata");
        node.setProperty(JCR_PRIMARYTYPE, NT_UNSTRUCTURED, NAME);
        return node;
    }
}
