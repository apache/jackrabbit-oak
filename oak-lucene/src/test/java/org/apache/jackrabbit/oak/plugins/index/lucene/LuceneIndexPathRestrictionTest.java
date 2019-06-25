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
package org.apache.jackrabbit.oak.plugins.index.lucene;

import com.google.common.collect.Sets;
import org.apache.jackrabbit.oak.InitialContentHelper;
import org.apache.jackrabbit.oak.plugins.index.IndexConstants;
import org.apache.jackrabbit.oak.plugins.index.IndexUpdateProvider;
import org.apache.jackrabbit.oak.plugins.index.lucene.util.IndexDefinitionBuilder;
import org.apache.jackrabbit.oak.plugins.memory.PropertyValues;
import org.apache.jackrabbit.oak.query.NodeStateNodeTypeInfoProvider;
import org.apache.jackrabbit.oak.query.QueryEngineSettings;
import org.apache.jackrabbit.oak.query.ast.NodeTypeInfo;
import org.apache.jackrabbit.oak.query.ast.NodeTypeInfoProvider;
import org.apache.jackrabbit.oak.query.ast.Operator;
import org.apache.jackrabbit.oak.query.ast.SelectorImpl;
import org.apache.jackrabbit.oak.query.index.FilterImpl;
import org.apache.jackrabbit.oak.spi.commit.EditorHook;
import org.apache.jackrabbit.oak.spi.query.Cursor;
import org.apache.jackrabbit.oak.spi.query.Filter;
import org.apache.jackrabbit.oak.spi.query.QueryIndex;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.Set;

import static com.google.common.collect.ImmutableSet.of;
import static org.apache.jackrabbit.JcrConstants.NT_BASE;
import static org.apache.jackrabbit.oak.spi.commit.CommitInfo.EMPTY;
import static org.junit.Assert.assertEquals;

public class LuceneIndexPathRestrictionTest {
    private static final EditorHook HOOK = new EditorHook(
            new IndexUpdateProvider(new LuceneIndexEditorProvider()));

    private NodeState root;
    private NodeBuilder rootBuilder;
    private IndexTracker tracker;
    private LucenePropertyIndex index;

    @Before
    public void setup() throws Exception {
        root = InitialContentHelper.INITIAL_CONTENT;
        rootBuilder = root.builder();

        tracker = new IndexTracker();
        tracker.update(root);
        index = new LucenePropertyIndex(tracker);

        // remove any indexes (that cause commits to fail due to missing provider)
        rootBuilder.child(IndexConstants.INDEX_DEFINITIONS_NAME).remove();

        commit();
    }

    @Test
    public void pathTranformationWithNoPathRestriction() throws Exception {
        IndexDefinitionBuilder idxBuilder =
                new IndexDefinitionBuilder(rootBuilder.child(IndexConstants.INDEX_DEFINITIONS_NAME).child("fooIndex"))
                        .noAsync().evaluatePathRestrictions();
        idxBuilder.indexRule("nt:base").property("foo").propertyIndex();
        idxBuilder.build();
        commit();

        NodeBuilder testRootBuilder = rootBuilder.child("test");
        testRootBuilder.child("a").child("j:c").setProperty("foo", "bar");
        testRootBuilder.child("b").setProperty("foo", "bar");
        testRootBuilder.child("c").child("d").child("j:c").setProperty("foo", "bar");
        commit();

        FilterImpl f;

        // //*[j:c/foo = 'bar'] -> foo:bar -> transform 1 level up
        f = createFilter(root, NT_BASE);
        f.restrictProperty("j:c/foo", Operator.EQUAL, PropertyValues.newString("bar"));
        validateResult(f, of("/test", "/test/a", "/test/c/d"));

        // //*[*/foo = 'bar'] -> foo:bar -> transform 1 level up
        f = createFilter(root, NT_BASE);
        f.restrictProperty("*/foo", Operator.EQUAL, PropertyValues.newString("bar"));
        validateResult(f, of("/test/a", "/test", "/test/c/d"));

        // //*[d/*/foo = 'bar'] -> foo:bar -> transform 2 level up
        f = createFilter(root, NT_BASE);
        f.restrictProperty("d/*/foo", Operator.EQUAL, PropertyValues.newString("bar"));
        validateResult(f, of("/", "/test", "/test/c"));
    }

    @Test
    public void pathTranformationWithAllChildrenPathRestriction() throws Exception {
        IndexDefinitionBuilder idxBuilder =
                new IndexDefinitionBuilder(rootBuilder.child(IndexConstants.INDEX_DEFINITIONS_NAME).child("fooIndex"))
                        .noAsync().evaluatePathRestrictions();
        idxBuilder.indexRule("nt:base").property("foo").propertyIndex();
        idxBuilder.build();
        commit();

        NodeBuilder testRootBuilder = rootBuilder.child("test");
        testRootBuilder.child("a").child("j:c").setProperty("foo", "bar");
        testRootBuilder.child("b").setProperty("foo", "bar");
        testRootBuilder.child("c").child("d").child("j:c").setProperty("foo", "bar");
        commit();

        FilterImpl f;

        // /jcr:root/test//*[j:c/foo = 'bar'] -> foo:bar :ancestors:/test -> transform 1 level up
        f = createFilter(root, NT_BASE);
        f.restrictProperty("j:c/foo", Operator.EQUAL, PropertyValues.newString("bar"));
        f.restrictPath("/test", Filter.PathRestriction.ALL_CHILDREN);
        validateResult(f, of("/test/a", "/test/c/d"));

        // /jcr:root/test//*[*/foo = 'bar'] -> foo:bar :ancestors:/test -> transform 1 level up
        f = createFilter(root, NT_BASE);
        f.restrictProperty("*/foo", Operator.EQUAL, PropertyValues.newString("bar"));
        f.restrictPath("/test", Filter.PathRestriction.ALL_CHILDREN);
        validateResult(f, of("/test/a", "/test/c/d"));

        // /jcr:root/test//*[d/*/foo = 'bar'] -> foo:bar :ancestors:/test -> transform 2 level up
        f = createFilter(root, NT_BASE);
        f.restrictProperty("d/*/foo", Operator.EQUAL, PropertyValues.newString("bar"));
        f.restrictPath("/test", Filter.PathRestriction.ALL_CHILDREN);
        validateResult(f, of("/test/c"));
    }

    @Test
    public void pathTranformationWithDirectChildrenPathRestriction() throws Exception {
        IndexDefinitionBuilder idxBuilder =
                new IndexDefinitionBuilder(rootBuilder.child(IndexConstants.INDEX_DEFINITIONS_NAME).child("fooIndex"))
                        .noAsync().evaluatePathRestrictions();
        idxBuilder.indexRule("nt:base").property("foo").propertyIndex();
        idxBuilder.build();
        commit();

        NodeBuilder testRootBuilder = rootBuilder.child("test");
        testRootBuilder.child("a").child("j:c").setProperty("foo", "bar");
        testRootBuilder.child("b").setProperty("foo", "bar");
        testRootBuilder.child("c").child("d").child("j:c").setProperty("foo", "bar");
        commit();

        FilterImpl f;

        // /jcr:root/test/*[j:c/foo = 'bar'] -> foo:bar :ancestors:/test :depth:[3 TO 3] -> transform 1 level up
        f = createFilter(root, NT_BASE);
        f.restrictProperty("j:c/foo", Operator.EQUAL, PropertyValues.newString("bar"));
        f.restrictPath("/test", Filter.PathRestriction.DIRECT_CHILDREN);
        validateResult(f, of("/test/a"));

        // /jcr:root/test/*[*/foo = 'bar'] -> foo:bar :ancestors:/test :depth:[3 TO 3] -> transform 1 level up
        f = createFilter(root, NT_BASE);
        f.restrictProperty("*/foo", Operator.EQUAL, PropertyValues.newString("bar"));
        f.restrictPath("/test", Filter.PathRestriction.DIRECT_CHILDREN);
        validateResult(f, of("/test/a"));

        // /jcr:root/test/*[d/*/foo = 'bar'] -> foo:bar :ancestors:/test :depth:[4 TO 4] -> transform 2 level up
        f = createFilter(root, NT_BASE);
        f.restrictProperty("d/*/foo", Operator.EQUAL, PropertyValues.newString("bar"));
        f.restrictPath("/test", Filter.PathRestriction.DIRECT_CHILDREN);
        validateResult(f, of("/test/c"));
    }

    @Test
    public void pathTranformationWithExactPathRestriction() throws Exception {
        IndexDefinitionBuilder idxBuilder =
                new IndexDefinitionBuilder(rootBuilder.child(IndexConstants.INDEX_DEFINITIONS_NAME).child("fooIndex"))
                        .noAsync().evaluatePathRestrictions();
        idxBuilder.indexRule("nt:base").property("foo").propertyIndex();
        idxBuilder.build();
        commit();

        NodeBuilder testRootBuilder = rootBuilder.child("test");
        testRootBuilder.child("a").child("j:c").setProperty("foo", "bar");
        testRootBuilder.child("b").setProperty("foo", "bar");
        testRootBuilder.child("c").child("d").child("j:c").setProperty("foo", "bar");
        commit();

        FilterImpl f;

        // /jcr:root/test/a[j:c/foo = 'bar'] -> foo:bar :path:/test/a/j:c -> transform 1 level up
        f = createFilter(root, NT_BASE);
        f.restrictProperty("j:c/foo", Operator.EQUAL, PropertyValues.newString("bar"));
        f.restrictPath("/test/a", Filter.PathRestriction.EXACT);
        validateResult(f, of("/test/a"));

        // /jcr:root/test/a[*/foo = 'bar'] -> foo:bar -> transform 1 level up + filter path restriction
        f = createFilter(root, NT_BASE);
        f.restrictProperty("*/foo", Operator.EQUAL, PropertyValues.newString("bar"));
        f.restrictPath("/test/a", Filter.PathRestriction.EXACT);
        validateResult(f, of("/test/a"));

        // /jcr:root/test/c[d/*/foo = 'bar'] -> foo:bar -> transform 2 level up + filter path restriction
        f = createFilter(root, NT_BASE);
        f.restrictProperty("d/*/foo", Operator.EQUAL, PropertyValues.newString("bar"));
        f.restrictPath("/test/c", Filter.PathRestriction.EXACT);
        validateResult(f, of("/test/c"));
    }

    @Test
    public void pathTranformationWithParentFilter() throws Exception {
        IndexDefinitionBuilder idxBuilder =
                new IndexDefinitionBuilder(rootBuilder.child(IndexConstants.INDEX_DEFINITIONS_NAME).child("fooIndex"))
                        .noAsync().evaluatePathRestrictions();
        idxBuilder.indexRule("nt:base").property("foo").propertyIndex();
        idxBuilder.build();
        commit();

        NodeBuilder testRootBuilder = rootBuilder.child("test");
        testRootBuilder.child("a").child("j:c").setProperty("foo", "bar");
        testRootBuilder.child("b").setProperty("foo", "bar");
        testRootBuilder.child("c").child("d").child("j:c").setProperty("foo", "bar");
        commit();

        FilterImpl f;

        // /jcr:root/test/a/b/j:c/..[j:c/foo = 'bar'] -> foo:bar :path:/test/a/b/j:c -> transform 1 level up
        f = createFilter(root, NT_BASE);
        f.restrictProperty("j:c/foo", Operator.EQUAL, PropertyValues.newString("bar"));
        f.restrictPath("/test/c/d/j:c", Filter.PathRestriction.PARENT);
        validateResult(f, of("/test/c/d"));

        // /jcr:root/test/a/b/j:c/..[*/foo = 'bar'] -> foo:bar -> transform 1 level up
        f = createFilter(root, NT_BASE);
        f.restrictProperty("*/foo", Operator.EQUAL, PropertyValues.newString("bar"));
        f.restrictPath("/test/a/b/j:c", Filter.PathRestriction.PARENT);
        validateResult(f, of("/test", "/test/a", "/test/c/d"));

        // /jcr:root/test/c/d/..[d/*/foo = 'bar'] -> foo:bar -> transform 2 level up
        f = createFilter(root, NT_BASE);
        f.restrictProperty("d/*/foo", Operator.EQUAL, PropertyValues.newString("bar"));
        f.restrictPath("/test/c/d", Filter.PathRestriction.PARENT);
        validateResult(f, of("/", "/test", "/test/c"));
    }

    private void validateResult(Filter f, Set<String> expected) {
        List<QueryIndex.IndexPlan> plans = index.getPlans(f, null, root);

        assertEquals("Only one plan must show up", 1, plans.size());

        QueryIndex.IndexPlan plan = plans.get(0);

        Cursor cursor = index.query(plan, root);
        Set<String> paths = Sets.newHashSet();

        while (cursor.hasNext()) {
            paths.add(cursor.next().getPath());
        }

        assertEquals(f.toString(), expected, paths);
    }

    private void commit() throws Exception {
        root = HOOK.processCommit(rootBuilder.getBaseState(), rootBuilder.getNodeState(), EMPTY);
        rootBuilder = root.builder();

        tracker.update(root);
    }

    private static FilterImpl createFilter(NodeState root, String nodeTypeName) {
        NodeTypeInfoProvider nodeTypes = new NodeStateNodeTypeInfoProvider(root);
        NodeTypeInfo type = nodeTypes.getNodeTypeInfo(nodeTypeName);
        SelectorImpl selector = new SelectorImpl(type, nodeTypeName);
        return new FilterImpl(selector, "SELECT * FROM [" + nodeTypeName + "]", new QueryEngineSettings());
    }
}
