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
package org.apache.jackrabbit.oak.plugins.index;

import static org.apache.jackrabbit.JcrConstants.JCR_CONTENT;
import static org.apache.jackrabbit.JcrConstants.JCR_PRIMARYTYPE;
import static org.apache.jackrabbit.JcrConstants.JCR_SYSTEM;
import static org.apache.jackrabbit.JcrConstants.NT_FILE;
import static org.apache.jackrabbit.JcrConstants.NT_UNSTRUCTURED;
import static org.apache.jackrabbit.oak.api.QueryEngine.NO_BINDINGS;
import static org.apache.jackrabbit.oak.api.Type.NAME;
import static org.apache.jackrabbit.oak.api.Type.STRING;
import static org.apache.jackrabbit.oak.api.Type.STRINGS;
import static org.apache.jackrabbit.oak.spi.nodetype.NodeTypeConstants.JCR_NODE_TYPES;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.stream.StreamSupport;
import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.oak.api.PropertyValue;
import org.apache.jackrabbit.oak.api.Result;
import org.apache.jackrabbit.oak.api.ResultRow;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.plugins.index.aggregate.SimpleNodeAggregator;
import org.apache.jackrabbit.oak.plugins.index.search.FulltextIndexConstants;
import org.apache.jackrabbit.oak.plugins.memory.PropertyStates;
import org.apache.jackrabbit.oak.plugins.tree.TreeUtil;
import org.apache.jackrabbit.oak.query.AbstractQueryTest;
import org.apache.jackrabbit.oak.spi.query.QueryIndex;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.jetbrains.annotations.NotNull;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class IndexAggregation2CommonTest extends AbstractQueryTest {

    protected static final Logger LOG = LoggerFactory.getLogger(IndexAggregation2CommonTest.class);

    private static final String NT_TEST_PAGE = "test:Page";
    private static final String NT_TEST_PAGECONTENT = "test:PageContent";
    private static final String NT_TEST_ASSET = "test:Asset";
    private static final String NT_TEST_ASSETCONTENT = "test:AssetContent";

    protected IndexOptions indexOptions;
    protected TestRepository repositoryOptionsUtil;

    @Override
    protected void createTestIndexNode() throws Exception {
        Tree index = root.getTree("/");
        Tree indexDefn = createTestIndexNode(index, indexOptions.getIndexType());
        TestUtil.useV2(indexDefn);
        //Aggregates
        TestUtil.newNodeAggregator(indexDefn)
                .newRuleWithName(NT_FILE, List.of("jcr:content"))
                .newRuleWithName(NT_TEST_PAGE, List.of("jcr:content"))
                .newRuleWithName(NT_TEST_PAGECONTENT, List.of("*", "*/*", "*/*/*", "*/*/*/*"))
                .newRuleWithName(NT_TEST_ASSET, List.of("jcr:content"))
                .newRuleWithName(
                    NT_TEST_ASSETCONTENT,
                    List.of("metadata", "renditions", "renditions/original", "comments",
                        "renditions/original/jcr:content"))
                .newRuleWithName("rep:User", List.of("profile"));

        Tree originalInclude = indexDefn.getChild(FulltextIndexConstants.AGGREGATES)
                                        .getChild(NT_TEST_ASSET).addChild("includeOriginal");
        originalInclude.setProperty(FulltextIndexConstants.AGG_RELATIVE_NODE, true);
        originalInclude.setProperty(FulltextIndexConstants.AGG_PATH,
            "jcr:content/renditions/original");

        Tree includeSingleRel = indexDefn.getChild(FulltextIndexConstants.AGGREGATES)
                                         .getChild(NT_TEST_ASSET)
                                         .addChild("includeFirstLevelChild");
        includeSingleRel.setProperty(FulltextIndexConstants.AGG_RELATIVE_NODE, true);
        includeSingleRel.setProperty(FulltextIndexConstants.AGG_PATH, "firstLevelChild");

        // Include all properties for both assets and pages
        Tree assetProps = TestUtil.newRulePropTree(indexDefn, NT_TEST_ASSET);
        TestUtil.enableForFullText(assetProps, "jcr:content/metadata/format");
        TestUtil.enableForFullText(assetProps, FulltextIndexConstants.REGEX_ALL_PROPS, true);

        Tree pageProps = TestUtil.newRulePropTree(indexDefn, NT_TEST_PAGE);
        TestUtil.enableForFullText(pageProps, FulltextIndexConstants.REGEX_ALL_PROPS, true);

        root.commit();
    }

    protected static QueryIndex.NodeAggregator getNodeAggregator() {
        return new SimpleNodeAggregator()
            .newRuleWithName(NT_FILE, List.of("jcr:content"))
            .newRuleWithName(NT_TEST_PAGE, List.of("jcr:content"))
            .newRuleWithName(NT_TEST_PAGECONTENT, List.of("*", "*/*", "*/*/*", "*/*/*/*"))
            .newRuleWithName(NT_TEST_ASSET, List.of("jcr:content"))
            .newRuleWithName(
                NT_TEST_ASSETCONTENT,
                List.of("metadata", "renditions", "renditions/original", "comments",
                    "renditions/original/jcr:content"))
            .newRuleWithName("rep:User", List.of("profile"));
    }

    @Test
    public void oak2226() throws Exception {
        setTraversalEnabled(false);
        final String statement = "/jcr:root/content//element(*, test:Asset)[" +
            "(jcr:contains(., 'mountain')) " +
            "and (jcr:contains(jcr:content/metadata/@format, 'image'))]";
        Tree content = root.getTree("/").addChild("content");
        List<String> expected = new ArrayList<>();

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

        assertEventually(() -> assertQuery(statement, "xpath", expected));
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
        List<String> expected = new ArrayList<>();

        Tree metadata = createAssetStructure(content, "tagged");
        metadata.setProperty("tags", List.of("namespace:season/summer"), STRINGS);
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
        metadata.setProperty("tags", List.of("namespace:season/winter"), STRINGS);
        metadata.setProperty("title", "Lorem winter ipsum", STRING);
        metadata.setProperty("format", "image/jpeg", STRING);

        root.commit();

        assertEventually(() -> assertQuery(statement, "xpath", expected));
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
        List<String> expected = new ArrayList<>();

        Tree metadata = createAssetStructure(content, "tagged");
        metadata.setProperty("tags", List.of("namespace:season/summer"), STRINGS);
        metadata.setProperty("format", "image/jpeg", STRING);

        Tree original = metadata.getParent().addChild("renditions").addChild("original");
        original.setProperty(JcrConstants.JCR_PRIMARYTYPE, JcrConstants.NT_FILE);
        original.addChild("jcr:content").setProperty(
            PropertyStates.createProperty(JcrConstants.JCR_MIMETYPE, "text/plain"));
        original.addChild("jcr:content")
                .setProperty(PropertyStates.createProperty("jcr:data", "fox jumps".getBytes()));

        expected.add("/content/tagged");
        root.commit();

        assertEventually(() -> assertQuery(statement, "xpath", expected));

        //Update the re-aggregated node and with that parent should be get updated
        Tree originalContent = TreeUtil.getTree(root.getTree("/"),
            "/content/tagged/jcr:content/renditions/original/jcr:content");
        originalContent.setProperty(
            PropertyStates.createProperty("jcr:data", "kiwi jumps".getBytes()));
        root.commit();
        assertEventually(() -> assertQuery(statement, "xpath", List.of()));
        setTraversalEnabled(true);
    }

    @Test
    public void indexSingleRelativeNode() throws Exception {
        setTraversalEnabled(false);
        final String statement = "//element(*, test:Asset)[ " +
            "jcr:contains(firstLevelChild, 'summer') ]";

        Tree content = root.getTree("/").addChild("content");
        Tree page = content.addChild("pages");
        page.setProperty(JCR_PRIMARYTYPE, NT_TEST_ASSET, NAME);
        Tree child = page.addChild("firstLevelChild");
        child.setProperty("tag", "summer is here", STRING);
        root.commit();

        assertEventually(() -> assertQuery(statement, "xpath", List.of("/content/pages")));
    }

    @Ignore("OAK-6597")
    @Test
    public void excerpt() throws Exception {
        setTraversalEnabled(false);
        final String statement = "select [rep:excerpt] from [test:Page] as page where contains(*, '%s*')";

        Tree content = root.getTree("/").addChild("content");
        Tree pageContent = createPageStructure(content, "foo");
        // contains 'aliq' but not 'tinc'
        pageContent.setProperty("bar",
            "Lorem ipsum dolor sit amet, consectetur adipiscing elit. Quisque aliquet odio varius odio "
                + "imperdiet, non egestas ex consectetur. Fusce congue ac augue quis finibus. Sed vulputate sollicitudin neque, nec "
                + "lobortis nisl varius eget.");
        // doesn't contain 'aliq' but 'tinc'
        pageContent.getParent().setProperty("bar",
            "Donec lacinia luctus leo, sed rutrum nulla. Sed sed hendrerit turpis. Donec ex quam, "
                + "bibendum et metus at, tristique tincidunt leo. Nam at elit ligula. Etiam ullamcorper, elit sit amet varius molestie, "
                + "nisl ex egestas libero, quis elementum enim mi a quam.");

        root.commit();

        for (String term : new String[]{"tinc", "aliq"}) {
            Result result = executeQuery(String.format(statement, term), "JCR-SQL2", NO_BINDINGS);
            Iterator<? extends ResultRow> rows = result.getRows().iterator();
            assertTrue(rows.hasNext());
            ResultRow firstHit = rows.next();
            assertFalse(rows.hasNext()); // assert that there is only a single hit

            PropertyValue excerptValue = firstHit.getValue("rep:excerpt");
            assertNotNull(excerptValue);
            assertNotEquals("Excerpt for '" + term + "' is not supposed to be empty.", "",
                excerptValue.getValue(STRING));
        }
    }

    /**
     * <p>
     * convenience method that create an "asset" structure like
     * </p>
     * <p>
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
     * <p>
     * <p>
     * and returns the {@code metadata} node
     * </p>
     *
     * @param parent   the parent under which creating the node
     * @param nodeName the node name to be used
     * @return the {@code metadata} node. See above for details
     */
    private static Tree createAssetStructure(@NotNull final Tree parent,
        @NotNull final String nodeName) {
        if (nodeName.isEmpty()) {
            throw new IllegalArgumentException("nodeName cannot be null or empty");
        }

        Tree node = parent.addChild(nodeName);
        node.setProperty(JCR_PRIMARYTYPE, NT_TEST_ASSET, NAME);
        node = node.addChild(JCR_CONTENT);
        node.setProperty(JCR_PRIMARYTYPE, NT_TEST_ASSETCONTENT, NAME);
        node = node.addChild("metadata");
        node.setProperty(JCR_PRIMARYTYPE, NT_UNSTRUCTURED, NAME);
        return node;
    }

    /**
     * <p>
     * convenience method that create an "page" structure like
     * </p>
     * <p>
     * <pre>
     *  "parent" : {
     *      "nodeName" : {
     *          "jcr:primaryType" : "test:Page",
     *          "jcr:content" : {
     *              "jcr:primaryType" : "test:PageContent"
     *          }
     *      }
     *  }
     * </pre>
     * <p>
     * <p>
     * and returns the {@code jcr:content} node
     * </p>
     *
     * @param parent   the parent under which creating the node
     * @param nodeName the node name to be used
     * @return the {@code jcr:content} node. See above for details
     */
    private static Tree createPageStructure(@NotNull final Tree parent,
        @NotNull final String nodeName) {
        if (nodeName.isEmpty()) {
            throw new IllegalArgumentException("nodeName cannot be null or empty");
        }

        Tree node = parent.addChild(nodeName);
        node.setProperty(JCR_PRIMARYTYPE, NT_TEST_PAGE, NAME);
        node = node.addChild(JCR_CONTENT);
        node.setProperty(JCR_PRIMARYTYPE, NT_TEST_PAGECONTENT, NAME);

        return node;
    }

    /**
     * convenience method for printing on logs the currently registered node types.
     */
    protected static void printNodeTypes(NodeBuilder builder) {
        if (LOG.isDebugEnabled()) {
            NodeBuilder namespace = builder.child(JCR_SYSTEM).child(JCR_NODE_TYPES);
            StreamSupport.stream(namespace.getChildNodeNames().spliterator(), false)
                         .sorted()
                         .forEach(LOG::debug);
        }
    }

    protected void assertEventually(Runnable r) {
        TestUtil.assertEventually(r,
            ((repositoryOptionsUtil.isAsync()
                ? repositoryOptionsUtil.defaultAsyncIndexingTimeInSeconds : 0) + 3000) * 5);
    }
}
