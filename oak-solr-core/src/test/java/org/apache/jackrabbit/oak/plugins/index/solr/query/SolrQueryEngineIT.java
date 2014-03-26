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
package org.apache.jackrabbit.oak.plugins.index.solr.query;

import java.util.HashSet;
import java.util.Set;

import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.plugins.index.solr.SolrBaseTest;
import org.apache.jackrabbit.oak.query.QueryEngineSettings;
import org.apache.jackrabbit.oak.query.ast.Operator;
import org.apache.jackrabbit.oak.query.ast.SelectorImpl;
import org.apache.jackrabbit.oak.query.index.FilterImpl;
import org.apache.jackrabbit.oak.spi.query.Cursor;
import org.apache.jackrabbit.oak.spi.query.Filter;
import org.apache.jackrabbit.oak.spi.query.PropertyValues;
import org.apache.jackrabbit.oak.spi.query.QueryIndex;
import org.junit.Test;

import static com.google.common.collect.Sets.newHashSet;
import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertNotNull;
import static junit.framework.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Integration test for {@link org.apache.jackrabbit.oak.plugins.index.solr.query.SolrQueryIndex} working
 * together
 */
public class SolrQueryEngineIT extends SolrBaseTest {

    @Test
    public void testExactPathFiltering() throws Exception {
        Root root = createRoot();
        Tree tree = root.getTree("/");
        tree.addChild("somenode");
        tree.addChild("someothernode");
        root.commit();

        QueryIndex index = new SolrQueryIndex("solr", server, configuration);
        FilterImpl filter = new FilterImpl(mock(SelectorImpl.class), "", new QueryEngineSettings());
        filter.restrictPath("/somenode", Filter.PathRestriction.EXACT);
        Cursor cursor = index.query(filter, store.getRoot());
        assertCursor(cursor, "/somenode");
    }

    @Test
    public void testDirectChildrenPathFiltering() throws Exception {
        Root root = createRoot();
        Tree tree = root.getTree("/");
        Tree parent = tree.addChild("somenode");
        parent.addChild("child1");
        Tree child2 = parent.addChild("child2");
        child2.addChild("descendant");
        Tree someothernode = tree.addChild("someothernode");
        someothernode.addChild("someotherchild");
        root.commit();

        QueryIndex index = new SolrQueryIndex("solr", server, configuration);
        FilterImpl filter = new FilterImpl(mock(SelectorImpl.class), "", new QueryEngineSettings());
        filter.restrictPath("/somenode", Filter.PathRestriction.DIRECT_CHILDREN);
        Cursor cursor = index.query(filter, store.getRoot());
        assertCursor(cursor, "/somenode/child1", "/somenode/child2");
    }

    @Test
    public void testAllChildrenPathFiltering() throws Exception {
        Root root = createRoot();
        Tree tree = root.getTree("/");
        Tree parent = tree.addChild("somenode");
        parent.addChild("child1");
        Tree child2 = parent.addChild("child2");
        child2.addChild("descendant");
        Tree someothernode = tree.addChild("someothernode");
        someothernode.addChild("someotherchild");
        root.commit();

        QueryIndex index = new SolrQueryIndex("solr", server, configuration);
        FilterImpl filter = new FilterImpl(mock(SelectorImpl.class), "", new QueryEngineSettings());
        filter.restrictPath("/somenode", Filter.PathRestriction.ALL_CHILDREN);
        Cursor cursor = index.query(filter, store.getRoot());
        assertCursor(
                cursor, "/somenode", "/somenode/child1",
                "/somenode/child2", "/somenode/child2/descendant");
    }

    @Test
    public void testPropertyFiltering() throws Exception {
        Root root = createRoot();
        Tree tree = root.getTree("/");
        tree.addChild("somenode").setProperty("foo", "bar");
        tree.addChild("someothernode").setProperty("foo", "bard");
        tree.addChild("anotherone").setProperty("foo", "a fool's bar");
        root.commit();

        QueryIndex index = new SolrQueryIndex("solr", server, configuration);
        FilterImpl filter = new FilterImpl(mock(SelectorImpl.class), "", new QueryEngineSettings());
        filter.restrictProperty("foo", Operator.EQUAL, PropertyValues.newString("bar"));
        Cursor cursor = index.query(filter, store.getRoot());
        assertCursor(cursor, "/somenode", "/anotherone");
    }

    @Test
    public void testPrimaryTypeFiltering() throws Exception {
        Root root = createRoot();
        Tree tree = root.getTree("/");
        tree.addChild("asamplenode").setProperty("jcr:primaryType", "nt:unstructured");
        tree.addChild("afoldernode").setProperty("jcr:primaryType", "nt:folder");
        tree.addChild("anothersamplenode").setProperty("jcr:primaryType", "nt:unstructured");
        root.commit();

        QueryIndex index = new SolrQueryIndex("solr", server, configuration);
        SelectorImpl selector = mock(SelectorImpl.class);
        Set<String> primaryTypes = new HashSet<String>();
        primaryTypes.add("nt:folder");
        when(selector.getPrimaryTypes()).thenReturn(primaryTypes);
        FilterImpl filter = new FilterImpl(selector, "select * from [nt:folder]", new QueryEngineSettings());
        Cursor cursor = index.query(filter, store.getRoot());
        assertCursor(cursor, "/afoldernode");
    }

    private void assertCursor(Cursor cursor, String... paths) {
        assertNotNull(cursor);
        Set<String> set = newHashSet();
        while (cursor.hasNext()) {
            assertTrue(set.add(cursor.next().getPath()));
        }
        assertEquals(newHashSet(paths), set);
    }

}
