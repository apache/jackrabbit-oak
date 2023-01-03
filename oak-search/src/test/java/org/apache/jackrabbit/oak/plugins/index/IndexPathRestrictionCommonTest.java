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
package org.apache.jackrabbit.oak.plugins.index;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import org.apache.jackrabbit.oak.InitialContentHelper;
import org.apache.jackrabbit.oak.commons.junit.LogCustomizer;
import org.apache.jackrabbit.oak.plugins.index.search.spi.query.FulltextIndex;
import org.apache.jackrabbit.oak.plugins.index.search.spi.query.FulltextIndexPlanner;
import org.apache.jackrabbit.oak.plugins.index.search.util.IndexDefinitionBuilder;
import org.apache.jackrabbit.oak.plugins.memory.PropertyValues;
import org.apache.jackrabbit.oak.query.NodeStateNodeTypeInfoProvider;
import org.apache.jackrabbit.oak.query.QueryEngineSettings;
import org.apache.jackrabbit.oak.query.ast.NodeTypeInfo;
import org.apache.jackrabbit.oak.query.ast.NodeTypeInfoProvider;
import org.apache.jackrabbit.oak.query.ast.Operator;
import org.apache.jackrabbit.oak.query.ast.SelectorImpl;
import org.apache.jackrabbit.oak.query.index.FilterImpl;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EditorHook;
import org.apache.jackrabbit.oak.spi.query.Cursor;
import org.apache.jackrabbit.oak.spi.query.Filter;
import org.apache.jackrabbit.oak.spi.query.QueryIndex;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static com.google.common.collect.ImmutableSet.of;
import static org.apache.jackrabbit.JcrConstants.NT_BASE;
import static org.junit.Assert.assertEquals;

@RunWith(Parameterized.class)
public abstract class IndexPathRestrictionCommonTest {

    protected EditorHook hook;
    private NodeBuilder rootBuilder;
    protected NodeState root;
    protected FulltextIndex index;

    protected final boolean evaluatePathRestrictionsInIndex;

    public IndexPathRestrictionCommonTest(boolean evaluatePathRestrictionsInIndex) {
        this.evaluatePathRestrictionsInIndex = evaluatePathRestrictionsInIndex;
    }

    public static Object[] doesIndexEvaluatePathRestrictions(boolean flag) {
        return new Object[]{
                flag
        };
    }

    @Before
    public void setup() throws Exception {
        root = InitialContentHelper.INITIAL_CONTENT;
        rootBuilder = root.builder();
        setupHook();
        setupFullTextIndex();

        // remove any indexes (that cause commits to fail due to missing provider)
        rootBuilder.child(IndexConstants.INDEX_DEFINITIONS_NAME).remove();

        commit();
    }

    @Test
    public void pathTransformationWithNoPathRestriction() throws Exception {
        setupTestData(evaluatePathRestrictionsInIndex);
        FilterImpl f;

        // NOTE : The expected results here might seem "incorrect"
        // for example : /test doesn't really satisfy the condition j:c/@foo=bar but still is in the result set
        // This is due to how transformed paths are handled by FullTextIndex - In case of nonFullTextConstraints we simply query the index
        // for foo=bar and return all results (after path transformation) - and then expect QueryEngine to deliver the correct results.
        // Refer FullTextIndexCommonTest#pathTransformationsWithNoPathRestrictions for seeing the e2e behaviour when executing an xpath query.

        // //*[j:c/foo = 'bar'] -> foo:bar -> transform 1 level up
        f = createFilter(root, NT_BASE);
        f.restrictProperty("j:c/foo", Operator.EQUAL, PropertyValues.newString("bar"));
        Map<String, Boolean> expectedMap = new LinkedHashMap<>();
        expectedMap.put("/test/a", true);
        expectedMap.put("/test", true);
        expectedMap.put("/test/c/d", true);
        expectedMap.put("/tmp/a", true);
        expectedMap.put("/tmp", true);
        expectedMap.put("/tmp/c/d", true);
        validateResult(f, ImmutableSet.of("/test", "/test/a", "/test/c/d", "/tmp", "/tmp/a", "/tmp/c/d"), expectedMap);

        // //*[*/foo = 'bar'] -> foo:bar -> transform 1 level up
        f = createFilter(root, NT_BASE);
        f.restrictProperty("*/foo", Operator.EQUAL, PropertyValues.newString("bar"));
        validateResult(f, ImmutableSet.of("/test", "/test/a", "/test/c/d", "/tmp", "/tmp/a", "/tmp/c/d"), expectedMap);

        // //*[d/*/foo = 'bar'] -> foo:bar -> transform 2 level up
        f = createFilter(root, NT_BASE);
        f.restrictProperty("d/*/foo", Operator.EQUAL, PropertyValues.newString("bar"));
        Map<String, Boolean> expectedMap2 = new LinkedHashMap<>();
        expectedMap2.put("/test", true);
        expectedMap2.put("/", true);
        expectedMap2.put("/test/c", true);
        expectedMap2.put("/tmp", true);
        expectedMap2.put("/tmp/c", true);
        validateResult(f, of("/", "/test", "/test/c", "/tmp", "/tmp/c"), expectedMap2);
    }


    @Test
    public void entryCountWithNoPathRestriction() throws Exception {
        IndexDefinitionBuilder idxBuilder =
                getIndexDefinitionBuilder(rootBuilder.child(IndexConstants.INDEX_DEFINITIONS_NAME).
                        child("fooIndex"))
                        .noAsync().evaluatePathRestrictions();
        idxBuilder.indexRule("nt:base").property("foo").propertyIndex();
        idxBuilder.build();
        commit();

        NodeBuilder testRootBuilder = rootBuilder.child("test");
        int count = 100;
        for (int i = 0; i < count; i++) {
            testRootBuilder.child("n" + i).setProperty("foo", "bar");
        }
        commit();

        FilterImpl f;

        // //*[foo = 'bar']
        f = createFilter(root, NT_BASE);
        f.restrictProperty("foo", Operator.EQUAL, PropertyValues.newString("bar"));
        // equality check: we assume FulltextIndexPlanner#DEFAULT_PROPERTY_WEIGHT different values
        int cost = count / FulltextIndexPlanner.DEFAULT_PROPERTY_WEIGHT;
        validateEstimatedCount(f, cost);

        // /jcr:root/test/*[foo = 'bar']
        f = createFilter(root, NT_BASE);
        f.restrictProperty("foo", Operator.EQUAL, PropertyValues.newString("bar"));
        f.restrictPath("/test", Filter.PathRestriction.DIRECT_CHILDREN);
        // direct children + equality check: we assume 50% of just checking for equality
        validateEstimatedCount(f, (int) (cost * 0.5));

        // /jcr:root/test//*[foo = 'bar']
        f = createFilter(root, NT_BASE);
        f.restrictProperty("foo", Operator.EQUAL, PropertyValues.newString("bar"));
        f.restrictPath("/test", Filter.PathRestriction.ALL_CHILDREN);
        // descendants + equality check: we assume 90% of just checking for equality
        validateEstimatedCount(f, (int) (cost * 0.9));

        // /jcr:root/test/x[foo = 'bar']
        f = createFilter(root, NT_BASE);
        f.restrictProperty("foo", Operator.EQUAL, PropertyValues.newString("bar"));
        f.restrictPath("/test/x", Filter.PathRestriction.EXACT);
        // exact path + equality check: we assume just 1 (as we have only one possible node)
        validateEstimatedCount(f, 1);
    }

    @Test
    public void pathTransformationWithAllChildrenPathRestriction() throws Exception {
        setupTestData(evaluatePathRestrictionsInIndex);

        NodeBuilder testRootBuilder = rootBuilder.child("test");
        testRootBuilder.child("d").child("e").child("j:c").setProperty("foo", "bar");
        commit();

        FilterImpl f;

        // Validation 1
        // /jcr:root/test//*[j:c/foo = 'bar'] -> foo:bar :ancestors:/test -> transform 1 level up
        f = createFilter(root, NT_BASE);
        f.restrictProperty("j:c/foo", Operator.EQUAL, PropertyValues.newString("bar"));
        f.restrictPath("/test", Filter.PathRestriction.ALL_CHILDREN);

        Map<String, Boolean> expectedMap1 = new LinkedHashMap<>();
        if (evaluatePathRestrictionsInIndex) {
            // When index evaluates path restriction - we don't expect log messages for shouldInclude filtering using FullTextIndex#shouldInclude
            // Index will itself handle path restrictions and only pass on the filtered nodes/paths to shouldInclude filtering
            expectedMap1.put("/test/a", true);
            expectedMap1.put("/test", false);
            expectedMap1.put("/test/c/d", true);
            expectedMap1.put("/test/d/e", true);
        } else {
            // When evaluatePathRestrictions is not set - index will return all paths based on property restriction only
            // and the respective fulltext index (LucenePropertyIndex) will filter out results based on path restrictions.
            // This case is only valid for lucene since Elastic always indexes ancestor paths by default
            // See ElasticDocumentMaker#finalizeDoc
            expectedMap1.put("/test/a", true);
            expectedMap1.put("/test", false);
            expectedMap1.put("/test/c/d", true);
            expectedMap1.put("/tmp/a", false);
            expectedMap1.put("/tmp", false);
            expectedMap1.put("/tmp/c/d", false);
            expectedMap1.put("/test/d/e", true);
        }
        validateResult(f, of("/test/a", "/test/c/d", "/test/d/e"), expectedMap1);

        // Validation 2
        // /jcr:root/test//*[*/foo = 'bar'] -> foo:bar :ancestors:/test -> transform 1 level up
        f = createFilter(root, NT_BASE);
        f.restrictProperty("*/foo", Operator.EQUAL, PropertyValues.newString("bar"));
        f.restrictPath("/test", Filter.PathRestriction.ALL_CHILDREN);

        Map<String, Boolean> expectedMap2 = new LinkedHashMap<>();
        if (evaluatePathRestrictionsInIndex) {
            // When index evaluates path restriction - we don't expect log messages for shouldInclude filtering using FullTextIndex#shouldInclude
            // Index will itself handle path restrictions and only pass on the filtered nodes/paths to shouldInclude filtering
            expectedMap2.put("/test/a", true);
            expectedMap2.put("/test", false);
            expectedMap2.put("/test/c/d", true);
            expectedMap2.put("/test/d/e", true);
        } else {
            // When evaluatePathRestrictions is not set - index will return all paths based on property restriction only
            // and the respective fulltext index (LucenePropertyIndex) will filter out results based on path restrictions.
            // This case is only valid for lucene since Elastic always indexes ancestor paths by default
            // See ElasticDocumentMaker#finalizeDoc
            expectedMap2.put("/test/a", true);
            expectedMap2.put("/test", false);
            expectedMap2.put("/test/c/d", true);
            expectedMap2.put("/tmp/a", false);
            expectedMap2.put("/tmp", false);
            expectedMap2.put("/tmp/c/d", false);
            expectedMap2.put("/test/d/e", true);
        }
        validateResult(f, of("/test/a", "/test/c/d", "/test/d/e"), expectedMap2);

        // Validation 3
        // /jcr:root/test//*[d/*/foo = 'bar'] -> foo:bar :ancestors:/test -> transform 2 level up
        f = createFilter(root, NT_BASE);
        f.restrictProperty("d/*/foo", Operator.EQUAL, PropertyValues.newString("bar"));
        f.restrictPath("/test", Filter.PathRestriction.ALL_CHILDREN);
        Map<String, Boolean> expectedMap3 = new LinkedHashMap<>();
        if (evaluatePathRestrictionsInIndex) {
            // When index evaluates path restriction - we don't expect log messages for shouldInclude filtering using FullTextIndex#shouldInclude
            // Index will itself handle path restrictions and only pass on the filtered nodes/paths to shouldInclude filtering
            expectedMap3.put("/test", false);
            expectedMap3.put("/", false);
            expectedMap3.put("/test/c", true);
            expectedMap3.put("/test/d", true);
        } else {
            // When evaluatePathRestrictions is not set - index will return all paths based on property restriction only
            // and the respective fulltext index (LucenePropertyIndex) will filter out results based on path restrictions.
            // This case is only valid for lucene since Elastic always indexes ancestor paths by default
            // See ElasticDocumentMaker#finalizeDoc
            expectedMap3.put("/test", false);
            expectedMap3.put("/", false);
            expectedMap3.put("/test/c", true);
            expectedMap3.put("/tmp", false);
            expectedMap3.put("/tmp/c", false);
            expectedMap3.put("/test/d", true);
        }
        // NOTE -
        // We have 2 nodes in the content -
        // /test/c/d/j:c{foo=bar} and /test/d/e/j:c{foo=bar}
        // Here the expected result also contains (/test/d) which is transformed path for (/test/d/e/j:c{foo=bar}) which effectively doesn't satisfy
        // transformed/relative property restriction d/*/foo=bar
        // If you notice d/*/foo is effectively treated as */*/foo by FullTextIndex.
        // This is handled later in the QueryEngine and the final result
        // of query like /jcr:root/test//*[d/*/foo = 'bar'] will not contain /test/d and only return /test/c.
        // This test is demonstrated at QueryEngine level in FullTextIndexCommonTest#pathTransformationsWithPathRestrictions
        validateResult(f, of("/test/c", "/test/d"), expectedMap3);

        // Validation 4
        // /jcr:root/test//*[foo = 'bar'] -> foo:bar :ancestors:/test -> no transformation
        f = createFilter(root, NT_BASE);
        f.restrictProperty("foo", Operator.EQUAL, PropertyValues.newString("bar"));
        f.restrictPath("/tmp", Filter.PathRestriction.ALL_CHILDREN);
        Map<String, Boolean> expectedMap4 = new LinkedHashMap<>();
        if (evaluatePathRestrictionsInIndex) {
            // When index evaluates path restriction - we don't expect log messages for shouldInclude filtering using FullTextIndex#shouldInclude
            // Index will itself handle path restrictions and only pass on the filtered nodes/paths to shouldInclude filtering
            expectedMap4.put("/tmp/a/j:c", true);
            expectedMap4.put("/tmp/b", true);
            expectedMap4.put("/tmp/c/d/j:c", true);
        } else {
            // When evaluatePathRestrictions is not set - index will return all paths based on property restriction only
            // and the respective fulltext index (LucenePropertyIndex) will filter out results based on path restrictions.
            // This case is only valid for lucene since Elastic always indexes ancestor paths by default
            // See ElasticDocumentMaker#finalizeDoc
            expectedMap4.put("/test/a/j:c", false);
            expectedMap4.put("/test/b", false);
            expectedMap4.put("/test/c/d/j:c", false);
            expectedMap4.put("/tmp/a/j:c", true);
            expectedMap4.put("/tmp/b", true);
            expectedMap4.put("/tmp/c/d/j:c", true);
            expectedMap4.put("/test/d/e/j:c", false);
        }
        validateResult(f, of("/tmp/a/j:c", "/tmp/b", "/tmp/c/d/j:c"), expectedMap4);
    }

    @Test
    public void pathTransformationWithDirectChildrenPathRestriction() throws Exception {
        setupTestData(evaluatePathRestrictionsInIndex);
        FilterImpl f;

        // /jcr:root/test/*[j:c/foo = 'bar'] -> foo:bar :ancestors:/test :depth:[3 TO 3] -> transform 1 level up
        f = createFilter(root, NT_BASE);
        f.restrictProperty("j:c/foo", Operator.EQUAL, PropertyValues.newString("bar"));
        f.restrictPath("/test", Filter.PathRestriction.DIRECT_CHILDREN);
        Map<String, Boolean> expectedMap = new LinkedHashMap<>();
        if (evaluatePathRestrictionsInIndex) {
            expectedMap.put("/test/a", true);
        } else {
            expectedMap.put("/test/a", true);
            expectedMap.put("/test", false);
            expectedMap.put("/test/c/d", false);
            expectedMap.put("/tmp/a", false);
            expectedMap.put("/tmp", false);
            expectedMap.put("/tmp/c/d", false);
        }
        validateResult(f, of("/test/a"), expectedMap);

        // /jcr:root/test/*[*/foo = 'bar'] -> foo:bar :ancestors:/test :depth:[3 TO 3] -> transform 1 level up
        f = createFilter(root, NT_BASE);
        f.restrictProperty("*/foo", Operator.EQUAL, PropertyValues.newString("bar"));
        f.restrictPath("/test", Filter.PathRestriction.DIRECT_CHILDREN);
        Map<String, Boolean> expectedMap2 = new LinkedHashMap<>();
        if (evaluatePathRestrictionsInIndex) {
            expectedMap2.put("/test/a", true);
        } else {
            expectedMap2.put("/test/a", true);
            expectedMap2.put("/test", false);
            expectedMap2.put("/test/c/d", false);
            expectedMap2.put("/tmp/a", false);
            expectedMap2.put("/tmp", false);
            expectedMap2.put("/tmp/c/d", false);
        }
        validateResult(f, of("/test/a"), expectedMap2);

        // /jcr:root/test/*[d/*/foo = 'bar'] -> foo:bar :ancestors:/test :depth:[4 TO 4] -> transform 2 level up
        f = createFilter(root, NT_BASE);
        f.restrictProperty("d/*/foo", Operator.EQUAL, PropertyValues.newString("bar"));
        f.restrictPath("/test", Filter.PathRestriction.DIRECT_CHILDREN);
        Map<String, Boolean> expectedMap3 = new LinkedHashMap<>();
        if (evaluatePathRestrictionsInIndex) {
            expectedMap3.put("/test/c", true);
        } else {
            expectedMap3.put("/test", false);
            expectedMap3.put("/", false);
            expectedMap3.put("/test/c", true);
            expectedMap3.put("/tmp", false);
            expectedMap3.put("/tmp/c", false);
        }
        validateResult(f, of("/test/c"), expectedMap3);
    }

    @Test
    public void pathTransformationWithExactPathRestriction() throws Exception {
        setupTestData(evaluatePathRestrictionsInIndex);
        FilterImpl f;

        // /jcr:root/test/a[j:c/foo = 'bar'] -> foo:bar :path:/test/a/j:c -> transform 1 level up
        f = createFilter(root, NT_BASE);
        f.restrictProperty("j:c/foo", Operator.EQUAL, PropertyValues.newString("bar"));
        f.restrictPath("/test/a", Filter.PathRestriction.EXACT);
        Map<String, Boolean> expectedMap = new LinkedHashMap<>();
        // In case of EXACT path restriction, index can still serve path restriction even if evaluatePathRestrictions is false
        // (because we don't need ancestor query here, a simple term query on path term(:path) works which is always indexed)
        // so expectedMap here would be same for both
        expectedMap.put("/test/a", true);
        validateResult(f, of("/test/a"), expectedMap);

        // /jcr:root/test/a[*/foo = 'bar'] -> foo:bar -> transform 1 level up + filter path restriction
        f = createFilter(root, NT_BASE);
        f.restrictProperty("*/foo", Operator.EQUAL, PropertyValues.newString("bar"));
        f.restrictPath("/test/a", Filter.PathRestriction.EXACT);
        Map<String, Boolean> expectedMap2 = new LinkedHashMap<>();
        // IN this case for EXACT path restriction even setting evaluatePathRestrictions cannot use index because of transformed path in query prop
        // hence in both the cases evaluatePathRestrictions = true and false, path restriction will be applied in post filtering after getting results from index
        // on foo=prop
        expectedMap2.put("/test/a", true);
        expectedMap2.put("/test", false);
        expectedMap2.put("/test/c/d", false);
        expectedMap2.put("/tmp/a", false);
        expectedMap2.put("/tmp", false);
        expectedMap2.put("/tmp/c/d", false);
        validateResult(f, of("/test/a"), expectedMap2);

        // /jcr:root/test/c[d/*/foo = 'bar'] -> foo:bar -> transform 2 level up + filter path restriction
        f = createFilter(root, NT_BASE);
        f.restrictProperty("d/*/foo", Operator.EQUAL, PropertyValues.newString("bar"));
        f.restrictPath("/test/c", Filter.PathRestriction.EXACT);
        // IN this case for EXACT path restriction even setting evaluatePathRestrictionsInIndex cannot use index because of transformed path in query prop
        // hence in both the cases evaluatePathRestrictionsInIndex = true and false, path restriction will be applied in post filtering after getting results from index
        // on foo=prop
        Map<String, Boolean> expectedMap3 = new LinkedHashMap<>();
        expectedMap3.put("/test", false);
        expectedMap3.put("/", false);
        expectedMap3.put("/test/c", true);
        expectedMap3.put("/tmp", false);
        expectedMap3.put("/tmp/c", false);
        validateResult(f, of("/test/c"), expectedMap3);


        // /jcr:root/test/a/j:c[foo = 'bar'] -> foo:bar :path:/test/a/j:c -> No Transformation
        f = createFilter(root, NT_BASE);
        f.restrictProperty("foo", Operator.EQUAL, PropertyValues.newString("bar"));
        f.restrictPath("/test/a/j:c", Filter.PathRestriction.EXACT);
        Map<String, Boolean> expectedMap4 = new LinkedHashMap<>();
        expectedMap4.put("/test/a/j:c", true);
        validateResult(f, of("/test/a/j:c"), expectedMap4);
    }

    @Test
    public void pathTransformationWithParentFilter() throws Exception {
        setupTestData(evaluatePathRestrictionsInIndex);

        FilterImpl f;

        // /jcr:root/test/a/b/j:c/..[j:c/foo = 'bar'] -> foo:bar :path:/test/a/b/j:c -> transform 1 level up
        f = createFilter(root, NT_BASE);
        f.restrictProperty("j:c/foo", Operator.EQUAL, PropertyValues.newString("bar"));
        f.restrictPath("/test/c/d/j:c", Filter.PathRestriction.PARENT);
        Map<String, Boolean> expectedMap = new LinkedHashMap<>();
        // In case of PARENT path restriction, index can still serve path restriction even if evaluatePathRestrictions is false
        // (because we don't need ancestor query here, a simple term query on path term(:path) works which is always indexed)
        expectedMap.put("/test/c/d", true);
        validateResult(f, of("/test/c/d"), expectedMap);

        // /jcr:root/test/a/b/j:c/..[*/foo = 'bar'] -> foo:bar -> transform 1 level up
        f = createFilter(root, NT_BASE);
        f.restrictProperty("*/foo", Operator.EQUAL, PropertyValues.newString("bar"));
        f.restrictPath("/test/a/b/j:c", Filter.PathRestriction.PARENT);
        Map<String, Boolean> expectedMap2 = new LinkedHashMap<>();
        // IN this case for PARENT path restriction even setting evaluatePathRestrictions cannot use index because of transformed path in query prop
        // hence in both the cases evaluatePathRestrictions = true and false, path restriction are NOT applied via index.
        // We also DO NOT support path restrictions on PARENT type in our post filtering (FulltextIndex#shouldInclude)
        // So effectively this path restriction gets ignored
        // TODO : Check if we need to support this or Query Engine handles this ?
        expectedMap2.put("/test/a", true);
        expectedMap2.put("/test", true);
        expectedMap2.put("/test/c/d", true);
        expectedMap2.put("/tmp/a", true);
        expectedMap2.put("/tmp", true);
        expectedMap2.put("/tmp/c/d", true);
        validateResult(f, ImmutableSet.of("/test", "/test/a", "/test/c/d", "/tmp", "/tmp/a", "/tmp/c/d"), expectedMap2);

        // /jcr:root/test/c/d/..[d/*/foo = 'bar'] -> foo:bar -> transform 2 level up
        f = createFilter(root, NT_BASE);
        f.restrictProperty("d/*/foo", Operator.EQUAL, PropertyValues.newString("bar"));
        f.restrictPath("/test/c/d", Filter.PathRestriction.PARENT);
        Map<String, Boolean> expectedMap3 = new LinkedHashMap<>();

        expectedMap3.put("/test", true);
        expectedMap3.put("/", true);
        expectedMap3.put("/test/c", true);
        expectedMap3.put("/tmp", true);
        expectedMap3.put("/tmp/c", true);
        validateResult(f, of("/", "/test", "/test/c", "/tmp", "/tmp/c"), expectedMap3);


        // /jcr:root/test/c/d/..[foo = 'bar'] -> foo:bar :path:/test/c/d -> No Transformation
        f = createFilter(root, NT_BASE);
        f.restrictProperty("foo", Operator.EQUAL, PropertyValues.newString("testBar"));
        f.restrictPath("/tmp2/a/j:c", Filter.PathRestriction.PARENT);
        Map<String, Boolean> expectedMap4 = new LinkedHashMap<>();

        expectedMap4.put("/tmp2/a", true);

        validateResult(f, of("/tmp2/a"), expectedMap4);
    }

    private void setupTestData(boolean evaluatePathRestrictionsInIndex) throws Exception {
        IndexDefinitionBuilder idxBuilder =
                getIndexDefinitionBuilder(rootBuilder.child(IndexConstants.INDEX_DEFINITIONS_NAME).child("fooIndex"))
                        .noAsync();

        if (evaluatePathRestrictionsInIndex) {
            idxBuilder = idxBuilder.evaluatePathRestrictions();
        }

        idxBuilder.indexRule("nt:base").property("foo").propertyIndex();
        idxBuilder.build();
        commit();

        NodeBuilder testRootBuilder = rootBuilder.child("test");
        testRootBuilder.child("a").child("j:c").setProperty("foo", "bar");
        testRootBuilder.child("b").setProperty("foo", "bar");
        testRootBuilder.child("c").child("d").child("j:c").setProperty("foo", "bar");

        // Now Add some nodes outside the /test path
        NodeBuilder tempRootBuilder = rootBuilder.child("tmp");
        tempRootBuilder.child("a").child("j:c").setProperty("foo", "bar");
        tempRootBuilder.child("b").setProperty("foo", "bar");
        tempRootBuilder.child("c").child("d").child("j:c").setProperty("foo", "bar");

        NodeBuilder temp2RootBuilder = rootBuilder.child("tmp2");
        temp2RootBuilder.child("a").setProperty("foo", "testBar");
        temp2RootBuilder.child("a").child("j:c");
        commit();
    }


    private void commit() throws Exception {
        // This part of code is used by both lucene and elastic tests.
        // The index definitions in these tests don't have async property set
        // So lucene, in this case behaves in a sync manner. But the tests fail on Elastic,
        // since ES indexing is always async.
        // The below commit info map (sync-mode = rt) would make Elastic use RealTimeBulkProcessHandler.
        // This would make ES indexes also sync.
        // This will NOT have any impact on the lucene tests.
        root = hook.processCommit(rootBuilder.getBaseState(), rootBuilder.getNodeState(), new CommitInfo("sync-mode", "rt"));
        rootBuilder = root.builder();

        postCommitHooks();
    }

    private void validateEstimatedCount(Filter f, long expectedCount) {
        TestUtil.assertEventually(() -> {
            List<QueryIndex.IndexPlan> plans = index.getPlans(f, null, root);
            assertEquals("Only one plan must show up", 1, plans.size());
            QueryIndex.IndexPlan plan = plans.get(0);
            // We need to use a comparatively higher timeout here since the estimatedCount comes from cache in ElasticIndexStatistics
            // This cache gets refreshed in async way after every 10 seconds
            // So the timeout here needs to be a little bit more than that
            assertEquals(expectedCount, plan.getEstimatedEntryCount());
        }, 4000 * 5);
    }

    private long getEstimatedCount(Filter f) {
        List<QueryIndex.IndexPlan> plans = index.getPlans(f, null, root);
        assertEquals("Only one plan must show up", 1, plans.size());
        QueryIndex.IndexPlan plan = plans.get(0);
        return plan.getEstimatedEntryCount();
    }

    private static FilterImpl createFilter(NodeState root, String nodeTypeName) {
        NodeTypeInfoProvider nodeTypes = new NodeStateNodeTypeInfoProvider(root);
        NodeTypeInfo type = nodeTypes.getNodeTypeInfo(nodeTypeName);
        SelectorImpl selector = new SelectorImpl(type, nodeTypeName);
        return new FilterImpl(selector, "SELECT * FROM [" + nodeTypeName + "]", new QueryEngineSettings());
    }

    /**
     * @param f                                          Query Filter that will be used to execute the query
     * @param expected                                   Expected set of paths in the result set
     * @param expectedPathMapForPostPathFilterLogEntries Expected map of paths that would be processed in
     *                                                   post path filtering and boolean indicating whether
     *                                                   the path is included or not.
     *                                                   For example - If the index has evaluatePathRestrictions enabled,
     *                                                   then this expected map would not have any paths that are filtered out by the index itslef.
     */
    private void validateResult(Filter f, Set<String> expected, Map<String, Boolean> expectedPathMapForPostPathFilterLogEntries) {
        LogCustomizer customLogs = getLogCustomizer();
        List<QueryIndex.IndexPlan> plans = index.getPlans(f, null, root);

        assertEquals("Only one plan must show up", 1, plans.size());

        QueryIndex.IndexPlan plan = plans.get(0);

        TestUtil.assertEventually(() -> {
            try {
                customLogs.starting();
                Cursor cursor = index.query(plan, root);
                Set<String> paths = Sets.newHashSet();

                while (cursor.hasNext()) {
                    paths.add(cursor.next().getPath());
                }
                assertEquals("Expected number log entries for post path filtering", expectedPathMapForPostPathFilterLogEntries.size(), customLogs.getLogs().size());

                List<String> expectedLogEntries = expectedPathMapForPostPathFilterLogEntries.keySet()
                        .stream()
                        .map(path -> getExpectedLogEntryForPostPathFiltering(path, expectedPathMapForPostPathFilterLogEntries.get(path)))
                        .collect(Collectors.toList());

                assertEquals("Expected log entries for post path filtering", expectedLogEntries.toString(), customLogs.getLogs().toString());
                assertEquals(f.toString(), expected, paths);
            } finally {
                customLogs.finished();
            }


        }, 3000 * 3);

    }

    protected abstract void postCommitHooks();

    protected abstract void setupHook();

    protected abstract void setupFullTextIndex();

    protected abstract IndexDefinitionBuilder getIndexDefinitionBuilder(NodeBuilder builder);

    protected abstract String getExpectedLogEntryForPostPathFiltering(String path, boolean shouldBeIncluded);

    protected abstract LogCustomizer getLogCustomizer();

}
