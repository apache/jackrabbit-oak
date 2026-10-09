/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.oak.plugins.index.luceneNg;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.List;
import java.util.Set;

import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.classic.spi.ThrowableProxyUtil;
import ch.qos.logback.core.read.ListAppender;
import org.apache.jackrabbit.oak.plugins.index.search.IndexDefinition;
import org.apache.jackrabbit.oak.plugins.index.search.spi.query.FulltextIndexPlanner.PlanResult;
import org.apache.jackrabbit.oak.plugins.index.search.util.IndexDefinitionBuilder;
import org.apache.jackrabbit.oak.InitialContentHelper;
import org.apache.jackrabbit.oak.plugins.index.luceneNg.directory.OakDirectory;
import org.apache.jackrabbit.oak.plugins.index.luceneNg.internal.editor.LuceneNgDocumentMaker;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.query.index.FilterImpl;
import org.apache.jackrabbit.oak.query.ast.Operator;
import org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState;
import org.apache.jackrabbit.oak.plugins.memory.PropertyValues;
import org.apache.jackrabbit.oak.spi.query.Filter;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.search.Query;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.facet.FacetsConfig;
import org.junit.Test;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class LuceneNgRestrictionRegressionTest {
    private static final String SECRET = "private-person@example.invalid";

    @Test
    public void invalidDateBoundsAndSetsFailWithoutLeakingInput() throws Exception {
        Method method = LuceneNgIndex.class.getDeclaredMethod(
                "createDateQuery", String.class, Filter.PropertyRestriction.class);
        method.setAccessible(true);
        LuceneNgIndex index = new LuceneNgIndex(new LuceneNgIndexTracker(), "/oak:index/test");
        for (int kind = 0; kind < 5; kind++) {
            Filter.PropertyRestriction pr = new Filter.PropertyRestriction();
            pr.propertyName = "created";
            pr.firstIncluding = true;
            pr.lastIncluding = true;
            switch (kind) {
                case 0:
                    pr.first = pr.last = PropertyValues.newDate(SECRET);
                    break;
                case 1:
                    pr.first = PropertyValues.newDate(SECRET);
                    break;
                case 2:
                    pr.last = PropertyValues.newDate(SECRET);
                    break;
                case 3:
                    pr.list = List.of(PropertyValues.newDate(SECRET));
                    break;
                default:
                    pr.isNot = true;
                    pr.not = PropertyValues.newDate(SECRET);
            }
            assertSafeFailure(() -> method.invoke(index, "created", pr), IllegalArgumentException.class);
        }
        Filter.PropertyRestriction epoch = new Filter.PropertyRestriction();
        epoch.first = epoch.last = PropertyValues.newDate("1970-01-01T00:00:00.000Z");
        epoch.firstIncluding = epoch.lastIncluding = true;
        assertEquals("created:[0 TO 0]", method.invoke(index, "created", epoch).toString());
    }

    @Test
    public void tokenizationFailureCannotReturnPartialTokensOrLogTheInput() throws Exception {
        Method method = LuceneNgIndex.class.getDeclaredMethod("tokenize", String.class, Analyzer.class);
        method.setAccessible(true);
        try (Analyzer analyzer = new Analyzer() {
            @Override
            protected TokenStreamComponents createComponents(String fieldName) {
                return new TokenStreamComponents(new Tokenizer() {
                    private boolean emitted;

                    @Override
                    public boolean incrementToken() throws IOException {
                        if (!emitted) {
                            addAttribute(org.apache.lucene.analysis.tokenattributes.CharTermAttribute.class)
                                    .append("partial");
                            emitted = true;
                            return true;
                        }
                        throw new IOException(SECRET);
                    }
                });
            }
        }) {
            assertSafeFailure(() -> method.invoke(null, SECRET, analyzer), IllegalStateException.class);
        }
    }

    @Test
    public void nodeTypesUseOnlyIndexedFields() throws Exception {
        IndexDefinitionBuilder builder = new IndexDefinitionBuilder().noAsync();
        builder.indexRule("nt:base").property("jcr:primaryType").propertyIndex()
                .property("jcr:mixinTypes").propertyIndex()
                .property("title").propertyIndex();
        Query query = typeQuery(builder);
        assertTrue(query.toString(), query.toString().contains("jcr:primaryType:nt:file"));
        assertTrue(query.toString(), query.toString().contains("jcr:mixinTypes:mix:referenceable"));
        IndexDefinitionBuilder withoutTypes = new IndexDefinitionBuilder().noAsync();
        withoutTypes.indexRule("nt:base").property("title").propertyIndex();
        assertEquals("*:*", typeQuery(withoutTypes).toString());
        org.apache.jackrabbit.oak.spi.state.NodeBuilder disabled = builder.build().builder();
        org.apache.jackrabbit.oak.spi.state.NodeBuilder properties = disabled.getChildNode("indexRules")
                .getChildNode("nt:base").getChildNode("properties");
        for (String name : properties.getChildNodeNames()) {
            org.apache.jackrabbit.oak.spi.state.NodeBuilder property = properties.getChildNode(name);
            if (!"title".equals(property.getNodeState().getString("name"))) {
                property.setProperty("index", false);
            }
        }
        assertEquals("*:*", typeQuery(disabled.getNodeState()).toString());
    }

    @Test
    public void pathSupportDoesNotDependOnTheDefinitionFlag() {
        for (int configured = -1; configured <= 1; configured++) {
            org.apache.jackrabbit.oak.spi.state.NodeBuilder builder = EmptyNodeState.EMPTY_NODE.builder();
            if (configured >= 0) {
                builder.setProperty("evaluatePathRestrictions", configured == 1);
            }
            LuceneNgIndexDefinition definition = new LuceneNgIndexDefinition(
                    EmptyNodeState.EMPTY_NODE, builder.getNodeState(), "/oak:index/test");
            assertTrue(definition.evaluatePathRestrictions());
        }
    }

    @Test
    public void plannerAdvertisesPathSupportForOmittedFalseAndTrueFlags() throws Exception {
        for (int configured = -1; configured <= 1; configured++) {
            org.apache.jackrabbit.oak.spi.state.NodeBuilder root = InitialContentHelper.INITIAL_CONTENT.builder();
            org.apache.jackrabbit.oak.spi.state.NodeBuilder definition = root.child("oak:index").child("test");
            IndexDefinitionBuilder builder = new IndexDefinitionBuilder(definition).noAsync();
            builder.indexRule("nt:base").property("title").propertyIndex();
            definition.setProperty("type", "luceneNg");
            if (configured >= 0) {
                definition.setProperty("evaluatePathRestrictions", configured == 1);
            }
            try (OakDirectory directory = new OakDirectory(
                    LuceneNgIndexStorage.getOrCreateStorageBuilder(definition), "test", false);
                 IndexWriter writer = new IndexWriter(directory, new IndexWriterConfig())) {
                writer.addDocument(new Document());
            }
            LuceneNgIndexTracker tracker = new LuceneNgIndexTracker();
            tracker.update(root.getNodeState());
            FilterImpl filter = FilterImpl.newTestInstance();
            filter.restrictProperty("title", Operator.EQUAL, PropertyValues.newString("x"));
            filter.restrictPath("/content", Filter.PathRestriction.ALL_CHILDREN);
            List<org.apache.jackrabbit.oak.spi.query.QueryIndex.IndexPlan> plans =
                    new LuceneNgIndex(tracker).getPlans(filter, List.of(), root.getNodeState());
            assertEquals(1, plans.size());
            assertTrue(plans.get(0).getSupportsPathRestriction());
            tracker.update(EmptyNodeState.EMPTY_NODE);
        }
    }

    @Test
    public void dateFacetErrorsDoNotLeakValuesOrConversionCauses() throws Exception {
        IndexDefinitionBuilder builder = new IndexDefinitionBuilder().noAsync();
        builder.indexRule("nt:base").property("title").propertyIndex();
        LuceneNgIndexDefinition definition = new LuceneNgIndexDefinition(
                InitialContentHelper.INITIAL_CONTENT, builder.build(), "/oak:index/test");
        LuceneNgDocumentMaker maker = new LuceneNgDocumentMaker(null, definition,
                definition.getApplicableIndexingRule("nt:base"), "/content", new FacetsConfig());
        Logger logger = (Logger) LoggerFactory.getLogger(LuceneNgDocumentMaker.class);
        ListAppender<ILoggingEvent> appender = new ListAppender<>();
        appender.start();
        logger.addAppender(appender);
        try {
            PropertyState single = mock(PropertyState.class);
            org.mockito.Mockito.<Type<?>>when(single.getType()).thenReturn(Type.DATE);
            when(single.getValue(Type.DATE)).thenThrow(new IllegalArgumentException(SECRET));
            Method singleMethod = LuceneNgDocumentMaker.class.getDeclaredMethod("convertToString", PropertyState.class);
            singleMethod.setAccessible(true);
            assertEquals(null, singleMethod.invoke(maker, single));
            Method arrayMethod = LuceneNgDocumentMaker.class.getDeclaredMethod("convertAllToStrings", PropertyState.class);
            arrayMethod.setAccessible(true);
            PropertyState multiple = mock(PropertyState.class);
            org.mockito.Mockito.<Type<?>>when(multiple.getType()).thenReturn(Type.DATES);
            when(multiple.getValue(Type.DATES)).thenReturn(List.of(SECRET));
            assertEquals(List.of(), arrayMethod.invoke(maker, multiple));
            when(multiple.getValue(Type.DATES)).thenThrow(new IllegalArgumentException(SECRET));
            assertEquals(List.of(), arrayMethod.invoke(maker, multiple));
            assertEquals("Each conversion failure must be logged", 3, appender.list.size());
            assertSafeLogs(appender.list);
        } finally {
            logger.detachAppender(appender);
            appender.stop();
        }
    }

    @Test
    public void unboundBackendDoesNotGuessAMissingPlanPath() {
        org.apache.jackrabbit.oak.spi.query.QueryIndex.IndexPlan plan =
                mock(org.apache.jackrabbit.oak.spi.query.QueryIndex.IndexPlan.class);
        assertThrows(IllegalStateException.class, () ->
                new LuceneNgIndex(new LuceneNgIndexTracker()).query(plan, EmptyNodeState.EMPTY_NODE));
        assertThrows(IllegalArgumentException.class, () ->
                new LuceneNgIndex(new LuceneNgIndexTracker(), "/content/oak:index/nested")
                        .query(plan, EmptyNodeState.EMPTY_NODE));
    }

    private static Query typeQuery(IndexDefinitionBuilder builder) throws Exception {
        return typeQuery(builder.build());
    }

    private static Query typeQuery(org.apache.jackrabbit.oak.spi.state.NodeState state) throws Exception {
        LuceneNgIndexDefinition definition = new LuceneNgIndexDefinition(
                InitialContentHelper.INITIAL_CONTENT, state, "/oak:index/test");
        IndexDefinition.IndexingRule rule = definition.getApplicableIndexingRule("nt:base");
        org.junit.Assert.assertNotNull(rule);
        PlanResult result = new PlanResult("/oak:index/test", definition, rule);
        Filter filter = mock(Filter.class);
        when(filter.getPropertyRestrictions()).thenReturn(List.of());
        when(filter.getPathRestriction()).thenReturn(Filter.PathRestriction.NO_RESTRICTION);
        when(filter.getPrimaryTypes()).thenReturn(Set.of("nt:file"));
        when(filter.getMixinTypes()).thenReturn(Set.of("mix:referenceable"));
        Method method = LuceneNgIndex.class.getDeclaredMethod("buildQuery", Filter.class, PlanResult.class);
        method.setAccessible(true);
        return (Query) method.invoke(new LuceneNgIndex(new LuceneNgIndexTracker(), "/oak:index/test"),
                filter, result);
    }

    private static void assertSafeFailure(org.junit.function.ThrowingRunnable operation,
                                         Class<? extends Throwable> expected) {
        Logger logger = (Logger) LoggerFactory.getLogger(LuceneNgIndex.class);
        ListAppender<ILoggingEvent> appender = new ListAppender<>();
        appender.start();
        logger.addAppender(appender);
        try {
            InvocationTargetException error = assertThrows(InvocationTargetException.class, operation);
            assertTrue(error.getCause().toString(), expected.isInstance(error.getCause()));
            Throwable cause = error.getCause();
            while (cause != null) {
                assertFalse(String.valueOf(cause.getMessage()).contains(SECRET));
                cause = cause.getCause();
            }
            assertFalse("The error must remain visible", appender.list.isEmpty());
            assertSafeLogs(appender.list);
        } finally {
            logger.detachAppender(appender);
            appender.stop();
        }
    }

    private static void assertSafeLogs(List<ILoggingEvent> events) {
        for (ILoggingEvent event : events) {
            assertFalse(event.getFormattedMessage().contains(SECRET));
            if (event.getThrowableProxy() != null) {
                assertFalse(ThrowableProxyUtil.asString(event.getThrowableProxy()).contains(SECRET));
            }
        }
    }
}
