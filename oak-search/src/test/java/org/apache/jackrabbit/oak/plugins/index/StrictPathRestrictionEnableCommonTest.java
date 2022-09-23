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

import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.index.search.FulltextIndexConstants;
import org.apache.jackrabbit.oak.plugins.memory.PropertyStates;
import org.apache.jackrabbit.oak.query.AbstractQueryTest;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.junit.Test;

import javax.jcr.Node;
import java.util.Set;

import static com.google.common.collect.ImmutableSet.of;
import static java.util.Collections.singletonList;
import static java.util.Arrays.asList;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.INDEX_DEFINITIONS_NAME;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.INDEX_DEFINITIONS_NODE_TYPE;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.REINDEX_PROPERTY_NAME;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.TYPE_PROPERTY_NAME;
import static org.apache.jackrabbit.oak.plugins.memory.PropertyStates.createProperty;
import static org.apache.jackrabbit.oak.spi.filter.PathFilter.PROP_EXCLUDED_PATHS;
import static org.apache.jackrabbit.oak.spi.filter.PathFilter.PROP_INCLUDED_PATHS;
import static org.hamcrest.CoreMatchers.containsString;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;

public abstract class StrictPathRestrictionEnableCommonTest extends AbstractQueryTest {

    protected NodeStore nodeStore;
    protected TestRepository repositoryOptionsUtil;
    protected Node indexNode;
    protected IndexOptions indexOptions;

    @Override
    protected void createTestIndexNode() throws Exception {
        setTraversalEnabled(false);
    }

    @Test
    public void pathIncludeWithPathRestrictionsEnabled() throws Exception {

        Tree idx = createIndex("test1", of("propa", "propb"));
        idx.setProperty(createProperty(PROP_INCLUDED_PATHS, of("/test/a"), Type.STRINGS));
        root.commit();

        Tree test = root.getTree("/").addChild("test");
        test.addChild("a").setProperty("propa", 10);
        test.addChild("a").addChild("b").setProperty("propa", 10);
        test.addChild("c").setProperty("propa", 10);
        root.commit();

        assertEventually(() -> {
            assertFalse(explain("select [jcr:path] from [nt:base] where [propa] = 10").contains(indexOptions.getIndexType() + ":test1"));
            assertThat(explain("select [jcr:path] from [nt:base] where [propa] = 10 and isDescendantNode('/test/a')"), containsString(indexOptions.getIndexType() + ":test1"));
            assertQuery("select [jcr:path] from [nt:base] where [propa] = 10 and isDescendantNode('/test/a')", singletonList("/test/a/b"));
        });
    }

    @Test
    public void pathExcludeWithPathRestrictionsEnabled() throws Exception {
        Tree idx = createIndex("test1", of("propa", "propb"));
        idx.setProperty(createProperty(PROP_EXCLUDED_PATHS, of("/test/a"), Type.STRINGS));
        root.commit();

        Tree test = root.getTree("/").addChild("test");
        test.addChild("a").setProperty("propa", 10);
        test.addChild("a").addChild("b").setProperty("propa", 10);
        test.addChild("c").setProperty("propa", 10);
        test.addChild("c").addChild("d").setProperty("propa", 10);
        root.commit();

        assertEventually(() -> {
            assertFalse(explain("select [jcr:path] from [nt:base] where [propa] = 10").contains(indexOptions.getIndexType() + ":test1"));
            assertThat(explain("select [jcr:path] from [nt:base] where [propa] = 10 and isDescendantNode('/test/c')"), containsString(indexOptions.getIndexType() + ":test1"));

            assertQuery("select [jcr:path] from [nt:base] where [propa] = 10 and isDescendantNode('/test/c')", singletonList("/test/c/d"));
        });
        //Make some change and then check
        Tree testc = root.getTree("/").getChild("test").getChild("c");
        testc.addChild("e").addChild("f").setProperty("propa", 10);
        root.commit();
        assertEventually(() -> {
            assertQuery("select [jcr:path] from [nt:base] where [propa] = 10 and isDescendantNode('/test/c')", asList("/test/c/d", "/test/c/e/f"));
            assertThat(explain("select [jcr:path] from [nt:base] where [propa] = 10 and isDescendantNode('/test/c') and not(isDescendantNode('/test/c/e'))"), containsString(indexOptions.getIndexType() + ":test1"));
            assertQuery("select [jcr:path] from [nt:base] where [propa] = 10 and isDescendantNode('/test/c') and not(isDescendantNode('/test/c/e'))", singletonList("/test/c/d"));
        });
    }

    private String explain(String query) {
        String explain = "explain " + query;
        return executeQuery(explain, "JCR-SQL2").get(0);
    }

    private Tree createIndex(String name, Set<String> propNames) throws CommitFailedException {
        Tree index = root.getTree("/");
        return createIndex(index, name, propNames);
    }

    public Tree createIndex(Tree index, String name, Set<String> propNames) throws CommitFailedException {
        Tree def = index.addChild(INDEX_DEFINITIONS_NAME).addChild(name);
        def.setProperty(JcrConstants.JCR_PRIMARYTYPE,
                INDEX_DEFINITIONS_NODE_TYPE, Type.NAME);
        def.setProperty(TYPE_PROPERTY_NAME, indexOptions.getIndexType());
        def.setProperty(REINDEX_PROPERTY_NAME, true);
        def.setProperty(FulltextIndexConstants.FULL_TEXT_ENABLED, false);
        def.setProperty(PropertyStates.createProperty(FulltextIndexConstants.INCLUDE_PROPERTY_NAMES, propNames, Type.STRINGS));
        return index.getChild(INDEX_DEFINITIONS_NAME).getChild(name);
    }

    private static void assertEventually(Runnable r) {
        TestUtil.assertEventually(r, 3000 * 3);
    }

}
