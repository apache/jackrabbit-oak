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
package org.apache.jackrabbit.oak.composite.blueGreen;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.index.IndexConstants;
import org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexConstants;
import org.apache.jackrabbit.oak.plugins.index.search.FulltextIndexConstants;
import org.apache.jackrabbit.oak.plugins.index.search.IndexDefinition;
import org.apache.jackrabbit.oak.plugins.index.search.IndexFormatVersion;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStateUtils;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.junit.Assert;

import javax.jcr.Node;
import javax.jcr.NodeIterator;
import javax.jcr.Property;
import javax.jcr.PropertyIterator;
import javax.jcr.RepositoryException;
import javax.jcr.query.Query;
import javax.jcr.query.QueryManager;
import javax.jcr.query.QueryResult;
import javax.jcr.query.Row;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.INDEX_DEFINITIONS_NODE_TYPE;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.TYPE_PROPERTY_NAME;

/**
 * Utilities for indexing and query tests.
 */
public class IndexUtils {

    /**
     * Create an index and wait until it is ready.
     *
     * @param p the persistence
     * @param indexName the name of the index
     * @param propertyName the property to index (on nt:base)
     * @param cost the cost per execution (high means the index isn't used if possible)
     * @return the index definition node
     */
    public static Node createIndex(Persistence p, String indexName, String propertyName, double cost) throws RepositoryException {
        Node indexDef = p.session.getRootNode().getNode("oak:index");
        Node index = indexDef.addNode(indexName, INDEX_DEFINITIONS_NODE_TYPE);
        index.setProperty(TYPE_PROPERTY_NAME, LuceneIndexConstants.TYPE_LUCENE);
        index.setProperty(IndexConstants.REINDEX_PROPERTY_NAME, true);
        index.setProperty(FulltextIndexConstants.COMPAT_MODE, IndexFormatVersion.V2.getVersion());
        index.setProperty(IndexConstants.ASYNC_PROPERTY_NAME,
                new String[] { "async", "nrt" });
        index.setProperty(FulltextIndexConstants.COST_PER_EXECUTION, cost);
        // index.setProperty("excludedPaths", "/jcr:system");
        Node indexRules = index.addNode(FulltextIndexConstants.INDEX_RULES);
        Node ntBase = indexRules.addNode("nt:base");
        Node props = ntBase.addNode(FulltextIndexConstants.PROP_NODE);
        Node foo = props.addNode(propertyName);
        foo.setProperty(FulltextIndexConstants.PROP_NAME, propertyName);
        foo.setProperty(FulltextIndexConstants.PROP_PROPERTY_INDEX, true);
        p.session.save();
        for (int i = 0; i < 600; i++) {
            index.refresh(false);
            if (!index.getProperty("reindex").getBoolean()) {
                break;
            }
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                // ignore
            }
        }
        return index;
    }

    /**
     * Run a query and return which index was used.
     *
     * @param p the persistence
     * @param xpath the xpath query
     * @param expectedIndex the index that is expected to be used
     * @param expectedResult the expected list of results
     * @return the index name used
     */
    public static void assertQueryUsesIndexAndReturns(Persistence p, String xpath, String expectedIndex,
            String expectedResult) throws RepositoryException {
        QueryManager qm = p.session.getWorkspace().getQueryManager();
        Query q = qm.createQuery("explain " + xpath, "xpath");
        QueryResult result = q.execute();
        Row r = result.getRows().nextRow();
        String plan = r.getValue("plan").getString();
        if (plan.indexOf(expectedIndex) <= 0) {
            throw new AssertionError("Index " + expectedIndex + " not used for query " + xpath + ": " + plan);
        }
        q = qm.createQuery(xpath, "xpath");
        NodeIterator it = q.execute().getNodes();
        ArrayList<String> list = new ArrayList<>();
        while (it.hasNext()) {
            Node n = it.nextNode();
            list.add(n.getPath());
        }
        Assert.assertEquals(expectedResult, list.toString());
    }

    /**
     * Utility method for debugging.
     *
     * @param node the node to print
     */
    public static void debugPrintProperties(Node node) throws RepositoryException {
        PropertyIterator it = node.getProperties();
        while (it.hasNext()) {
            Property pr = it.nextProperty();
            if (pr.isMultiple()) {
                System.out.println(pr.getName() + " " + Arrays.toString(pr.getValues()));
            } else {
                System.out.println(pr.getName() + " " + pr.getValue().getString());
            }
        }
    }

    /**
     * Check if the /libs node is read-only in this repository.
     *
     * @param p the persistence
     */
    public static void checkLibsIsReadOnly(Persistence p) throws RepositoryException {
        // libs is read-only
        Node libsNode = p.session.getRootNode().getNode("libs");
        try {
            libsNode.addNode("illegal");
            Assert.fail();
        } catch (RepositoryException e) {
            // expected
        }
    }

    // Could throw NPE, but we don't care as it's a test
    public static boolean isIndexDisabledAndHiddenNodesDeleted(NodeStore store, String path) {
        NodeState nodeState = NodeStateUtils.getNode(store.getRoot(), path);
        if (!nodeState.getProperty("type").getValue(Type.STRING).equals("disabled")) {
            return false;
        }
        Iterable<String> childNodeNames = nodeState.getChildNodeNames();
        for (String childNodeName : childNodeNames) {
            if (NodeStateUtils.isHidden(childNodeName)) {
                if (!childNodeName.startsWith(IndexDefinition.HIDDEN_OAK_MOUNT_PREFIX)) {
                    return false;
                }
            }
        }
        return true;
    }

    public static boolean isIndexEnabledAndHiddenNodesPresent(NodeStore store, String path) {
        NodeState nodeState = NodeStateUtils.getNode(store.getRoot(), path);
        if (!nodeState.getProperty("type").getValue(Type.STRING).equals("lucene")
                && !nodeState.getProperty("type").getValue(Type.STRING).equals("elastic")) {
            return false;
        }
        Iterable<String> childNodeNames = nodeState.getChildNodeNames();
        Set<String> indexHiddenNodes = new HashSet<>();
        for (String childNodeName : childNodeNames) {
            if (NodeStateUtils.isHidden(childNodeName)) {
                indexHiddenNodes.add(childNodeName);
            }
        }
        List<String> defaultHiddenNodes = Arrays.asList(":data", ":status",
                ":index-definition", ":oak:mount-libs-index-data");
        return indexHiddenNodes.containsAll(defaultHiddenNodes);
    }

}
