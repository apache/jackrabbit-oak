/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.oak.plugins.index.mongot;

import java.io.InputStream;
import java.util.Iterator;

import org.apache.jackrabbit.oak.InitialContentHelper;
import org.apache.jackrabbit.oak.api.ContentRepository;
import org.apache.jackrabbit.oak.api.PropertyValue;
import org.apache.jackrabbit.oak.api.Result;
import org.apache.jackrabbit.oak.api.ResultRow;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.plugins.index.IndexAggregation2CommonTest;
import org.apache.jackrabbit.oak.plugins.index.TestUtil;
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
import org.junit.ClassRule;
import org.junit.Test;

import static org.apache.jackrabbit.JcrConstants.JCR_CONTENT;
import static org.apache.jackrabbit.JcrConstants.JCR_PRIMARYTYPE;
import static org.apache.jackrabbit.oak.api.QueryEngine.NO_BINDINGS;
import static org.apache.jackrabbit.oak.api.Type.NAME;
import static org.apache.jackrabbit.oak.api.Type.STRING;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class MongotIndexAggregation2Test extends IndexAggregation2CommonTest {

    @ClassRule
    public static final MongotSearchConnectionRule mongo = new MongotSearchConnectionRule();

    public MongotIndexAggregation2Test() {
        indexOptions = new MongotIndexOptions();
    }

    @Override
    protected ContentRepository createRepository() {
        MongotCommonTestRepositoryBuilder builder = new MongotCommonTestRepositoryBuilder(mongo);
        builder.setNodeStore(createNodeStoreWithTestTypes());
        repositoryOptionsUtil = builder.build();
        return repositoryOptionsUtil.getOak().createContentRepository();
    }

    @Override
    protected void assertEventually(Runnable assertion) {
        TestUtil.assertEventually(assertion, 30_000);
    }

    @Override
    @Test
    public void excerpt() throws Exception {
        setTraversalEnabled(false);
        String statement = "select [rep:excerpt] from [test:Page] as page "
                + "where contains(*, '%s*')";

        Tree page = root.getTree("/").addChild("content").addChild("foo");
        page.setProperty(JCR_PRIMARYTYPE, "test:Page", NAME);
        Tree pageContent = page.addChild(JCR_CONTENT);
        pageContent.setProperty(JCR_PRIMARYTYPE, "test:PageContent", NAME);
        pageContent.setProperty("bar", "Lorem ipsum dolor sit amet, consectetur adipiscing elit. "
                + "Quisque aliquet odio varius odio imperdiet, non egestas ex consectetur. "
                + "Fusce congue ac augue quis finibus. Sed vulputate sollicitudin neque, "
                + "nec lobortis nisl varius eget.");
        page.setProperty("bar", "Donec lacinia luctus leo, sed rutrum nulla. Sed sed hendrerit "
                + "turpis. Donec ex quam, bibendum et metus at, tristique tincidunt leo. "
                + "Nam at elit ligula. Etiam ullamcorper, elit sit amet varius molestie, "
                + "nisl ex egestas libero, quis elementum enim mi a quam.");
        root.commit();

        for (String term : new String[] {"tinc", "aliq"}) {
            assertEventually(() -> assertSingleExcerpt(statement, term));
        }
    }

    private void assertSingleExcerpt(String statement, String term) {
        try {
            Result result = executeQuery(String.format(statement, term), "JCR-SQL2", NO_BINDINGS);
            Iterator<? extends ResultRow> rows = result.getRows().iterator();
            assertTrue(rows.hasNext());
            ResultRow firstHit = rows.next();
            assertFalse(rows.hasNext());
            PropertyValue excerpt = firstHit.getValue("rep:excerpt");
            assertNotNull(excerpt);
            assertNotEquals("Excerpt for '" + term + "' is not supposed to be empty.", "",
                    excerpt.getValue(STRING));
        } catch (Exception e) {
            throw new AssertionError(e);
        }
    }

    private static NodeStore createNodeStoreWithTestTypes() {
        NodeBuilder builder = InitialContentHelper.INITIAL_CONTENT.builder();
        NodeState base = builder.getNodeState();
        NodeStore registrationStore = new MemoryNodeStore(base);
        Root registrationRoot = RootFactory.createSystemRoot(registrationStore,
                new EditorHook(new CompositeEditorProvider(
                        new NamespaceEditorProvider(), new TypeEditorProvider())),
                null, null, null);

        try (InputStream stream = Thread.currentThread().getContextClassLoader()
                .getResourceAsStream("test_nodetypes.cnd")) {
            if (stream == null) {
                throw new IllegalStateException("test_nodetypes.cnd is unavailable");
            }
            NodeTypeRegistry.register(registrationRoot, stream, "testing node types");
        } catch (Exception e) {
            throw new IllegalStateException("Unable to register aggregation test node types", e);
        }

        registrationStore.getRoot().compareAgainstBaseState(base, new ApplyDiff(builder));
        return new MemoryNodeStore(builder.getNodeState());
    }
}
