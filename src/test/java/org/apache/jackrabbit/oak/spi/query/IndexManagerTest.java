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
package org.apache.jackrabbit.oak.spi.query;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Arrays;

import org.apache.jackrabbit.mk.api.MicroKernel;
import org.apache.jackrabbit.mk.core.MicroKernelImpl;
import org.apache.jackrabbit.oak.AbstractOakTest;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.ContentRepository;
import org.apache.jackrabbit.oak.api.ContentSession;
import org.apache.jackrabbit.oak.api.CoreValueFactory;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.core.ContentRepositoryImpl;
import org.apache.jackrabbit.oak.core.DefaultConflictHandler;
import org.apache.jackrabbit.oak.kernel.KernelNodeStore;
import org.apache.jackrabbit.oak.spi.commit.ValidatorProvider;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.junit.Before;
import org.junit.Test;

public class IndexManagerTest extends AbstractOakTest {

    protected ContentSession session;
    private CoreValueFactory vf;
    private final MicroKernel mk = new MicroKernelImpl();

    private Root root;

    @Override
    protected ContentRepository createRepository() {
        return new ContentRepositoryImpl(mk, null, (ValidatorProvider) null);
    }

    @Before
    public void before() throws Exception {
        super.before();
        session = createAdminSession();
        vf = session.getCoreValueFactory();
        root = session.getCurrentRoot();

    }

    @Test
    public void testNoDef() throws Exception {

        // setup index definitions
        String indexdef = "indexdefs" + System.currentTimeMillis();
        root.getTree("/").addChild("test").addChild(indexdef);
        root.commit(DefaultConflictHandler.OURS);

        NodeState ns = new KernelNodeStore(mk).getRoot();

        IndexManager im = new IndexManagerImpl("/test/" + indexdef, mk,
                new TestIndexFactory());
        assertTrue(im.getIndexDefinitions(ns).isEmpty());
    }

    @Test
    public void testSimpleDef() throws Exception {

        // setup index definitions
        String indexdef = "indexdefs" + System.currentTimeMillis();

        Tree test = root.getTree("/").addChild("test").addChild(indexdef);

        Tree def = test.addChild("a");
        def.setProperty("type", vf.createValue("custom"));
        def.setProperty("other", vf.createValue("other-value"));
        root.commit(DefaultConflictHandler.OURS);

        NodeState ns = new KernelNodeStore(mk).getRoot();

        IndexManager im = new IndexManagerImpl("/test/" + indexdef, mk,
                new TestIndexFactory());

        assertEquals(1, im.getIndexDefinitions(ns).size());
        IndexDefinition id = im.getIndexDefinitions(ns).iterator().next();

        assertEquals("a", id.getName());
        assertEquals("custom", id.getType());
        assertNotNull(id.getProperties());
        assertEquals("other-value", id.getProperties().get("other"));

    }

    @Test
    public void testIllegalDef() throws Exception {
        // setup index definitions
        String indexdef = "indexdefs" + System.currentTimeMillis();

        Tree test = root.getTree("/").addChild("test").addChild(indexdef);

        Tree def1 = test.addChild("a");
        def1.setProperty("type2", vf.createValue("custom"));
        root.commit(DefaultConflictHandler.OURS);
        NodeState ns = new KernelNodeStore(mk).getRoot();

        IndexManager im = new IndexManagerImpl("/test/" + indexdef, mk,
                new TestIndexFactory());

        assertTrue(im.getIndexDefinitions(ns).isEmpty());
    }

    @Test
    public void testObservation() throws Exception {

        // setup index definitions
        String indexdef = "indexdefs" + System.currentTimeMillis();

        Tree test = root.getTree("/").addChild("test").addChild(indexdef);
        root.commit(DefaultConflictHandler.OURS);
        NodeState ns = new KernelNodeStore(mk).getRoot();

        IndexManager im = new IndexManagerImpl("/test/" + indexdef, mk,
                new TestIndexFactory());
        assertEquals(0, im.getIndexDefinitions(ns).size());

        // bug OAK-283
        test = root.getTree("/test/" + indexdef);

        // add index def after the index manager has been init
        Tree def = test.addChild("a");
        def.setProperty("type", vf.createValue("custom"));
        def.setProperty("other", vf.createValue("other-value"));
        root.commit(DefaultConflictHandler.OURS);
        ns = new KernelNodeStore(mk).getRoot();

        assertEquals(1, im.getIndexDefinitions(ns).size());
        IndexDefinition id = im.getIndexDefinitions(ns).iterator().next();
        assertEquals("a", id.getName());
        assertEquals("custom", id.getType());
        assertNotNull(id.getProperties());
        assertEquals("other-value", id.getProperties().get("other"));
    }

    /**
     * Test IndexFactory, not supposed to do anything, its purpose is to just
     * register a given index type
     * 
     */
    private static class TestIndexFactory implements IndexFactory {

        @Override
        public Index getIndex(IndexDefinition indexDefinition) {
            return new TestIndex(indexDefinition);
        }

        @Override
        public String[] getTypes() {
            return new String[] { "custom" };
        }

        @Override
        public void init(MicroKernel mk) {
        }

        @Override
        public String toString() {
            return "TestIndexFactory [getTypes()="
                    + Arrays.toString(getTypes()) + "]";
        }

        @Override
        public void close() throws IOException {
        }

    }

    private static class TestIndex implements Index {

        private final IndexDefinition indexDefinition;

        public TestIndex(IndexDefinition indexDefinition) {
            this.indexDefinition = indexDefinition;
        }

        @Override
        public NodeState editCommit(NodeStore store, NodeState before,
                NodeState after) throws CommitFailedException {
            return null;
        }

        @Override
        public IndexDefinition getDefinition() {
            return indexDefinition;
        }

        @Override
        public void close() throws IOException {
        }

    }
}
