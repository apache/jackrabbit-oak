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
package org.apache.jackrabbit.oak.plugins.index.luceneNg.internal;

import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.classic.spi.ThrowableProxyUtil;
import ch.qos.logback.core.read.ListAppender;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.index.luceneNg.LuceneNgIndexDefinition;
import org.apache.jackrabbit.oak.plugins.index.search.FieldNames;
import org.apache.jackrabbit.oak.plugins.index.search.IndexDefinition.IndexingRule;
import org.apache.jackrabbit.oak.plugins.index.search.PropertyDefinition;
import org.apache.jackrabbit.oak.plugins.index.search.spi.query.FulltextIndexPlanner.PlanResult;
import org.apache.jackrabbit.oak.plugins.index.search.util.IndexDefinitionBuilder;
import org.apache.jackrabbit.oak.plugins.memory.PropertyValues;
import org.apache.jackrabbit.oak.spi.query.Filter;
import org.apache.jackrabbit.oak.spi.query.Filter.PropertyRestriction;
import org.apache.jackrabbit.oak.spi.query.QueryIndex.OrderEntry;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.SortedSetSortField;
import org.apache.lucene.search.TermQuery;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Set;

import static org.apache.jackrabbit.oak.InitialContentHelper.INITIAL_CONTENT;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class LuceneNgQueryBuilderTest {

    private static final String SECRET = "private-person@example.invalid";

    @Test
    public void exclusiveLowerBoundStartsAtTheNextLong() {
        PropertyRestriction ageAbove30 = new PropertyRestriction();
        ageAbove30.propertyName = "age";
        ageAbove30.first = PropertyValues.newLong(30L);
        ageAbove30.firstIncluding = false;

        assertEquals(LongPoint.newRangeQuery("age", 31L, Long.MAX_VALUE),
                LuceneNgQueryBuilder.propertyQuery("age", ageAbove30, declared("age", "Long")));
    }

    @Test
    public void exclusiveLowerBoundAtLongMaxValueMatchesNothing() {
        PropertyRestriction aboveMax = new PropertyRestriction();
        aboveMax.propertyName = "score";
        aboveMax.first = PropertyValues.newLong(Long.MAX_VALUE);
        aboveMax.firstIncluding = false;

        assertEquals(new MatchNoDocsQuery(), LuceneNgQueryBuilder.propertyQuery("score", aboveMax, declared("score", "Long")));
    }

    @Test
    public void exclusiveUpperBoundAtLongMinValueMatchesNothing() {
        PropertyRestriction belowMin = new PropertyRestriction();
        belowMin.propertyName = "score";
        belowMin.last = PropertyValues.newLong(Long.MIN_VALUE);
        belowMin.lastIncluding = false;

        assertEquals(new MatchNoDocsQuery(), LuceneNgQueryBuilder.propertyQuery("score", belowMin, declared("score", "Long")));
    }

    @Test
    public void dateEqualityIsAMillisecondPoint() {
        PropertyRestriction epoch = new PropertyRestriction();
        epoch.propertyName = "created";
        epoch.first = epoch.last = PropertyValues.newDate("1970-01-01T00:00:00.000Z");
        epoch.firstIncluding = epoch.lastIncluding = true;

        assertEquals(LongPoint.newExactQuery("created", 0L),
                LuceneNgQueryBuilder.propertyQuery("created", epoch, declared("created", "Date")));
    }

    @Test
    public void invalidDateValuesFailWithoutLeakingTheInput() {
        PropertyDefinition created = declared("created", "Date");
        PropertyRestriction equality = new PropertyRestriction();
        equality.propertyName = "created";
        equality.first = equality.last = PropertyValues.newDate(SECRET);
        equality.firstIncluding = equality.lastIncluding = true;
        PropertyRestriction lowerBound = new PropertyRestriction();
        lowerBound.propertyName = "created";
        lowerBound.first = PropertyValues.newDate(SECRET);
        PropertyRestriction upperBound = new PropertyRestriction();
        upperBound.propertyName = "created";
        upperBound.last = PropertyValues.newDate(SECRET);
        PropertyRestriction in = new PropertyRestriction();
        in.propertyName = "created";
        in.list = List.of(PropertyValues.newDate(SECRET));
        PropertyRestriction not = new PropertyRestriction();
        not.propertyName = "created";
        not.isNot = true;
        not.not = PropertyValues.newDate(SECRET);

        for (PropertyRestriction restriction : List.of(equality, lowerBound, upperBound, in, not)) {
            assertFailsWithoutLeakingSecret(IllegalArgumentException.class,
                    () -> LuceneNgQueryBuilder.propertyQuery("created", restriction, created));
        }
    }

    @Test
    public void tokenizationFailureFailsWithoutPartialTokensOrLeakingTheInput() {
        try (Analyzer failingAfterOneToken = new Analyzer() {
            @Override
            protected TokenStreamComponents createComponents(String fieldName) {
                return new TokenStreamComponents(new Tokenizer() {
                    private boolean emitted;

                    @Override
                    public boolean incrementToken() throws IOException {
                        if (!emitted) {
                            addAttribute(CharTermAttribute.class).append("partial");
                            emitted = true;
                            return true;
                        }
                        throw new IOException(SECRET);
                    }
                });
            }
        }) {
            assertFailsWithoutLeakingSecret(IllegalStateException.class,
                    () -> LuceneNgQueryBuilder.tokenize(SECRET, failingAfterOneToken));
        }
    }

    @Test
    public void nodeTypeRestrictionsUseOnlyIndexedTypeProperties() {
        IndexDefinitionBuilder withTypes = new IndexDefinitionBuilder().noAsync();
        withTypes.indexRule("nt:base")
                .property("jcr:primaryType").propertyIndex()
                .property("jcr:mixinTypes").propertyIndex()
                .property("title").propertyIndex();
        assertEquals("+*:* #(jcr:primaryType:nt:file jcr:mixinTypes:mix:referenceable)",
                typeRestrictedQuery(withTypes.build()));

        IndexDefinitionBuilder withoutTypes = new IndexDefinitionBuilder().noAsync();
        withoutTypes.indexRule("nt:base").property("title").propertyIndex();
        assertEquals("*:*", typeRestrictedQuery(withoutTypes.build()));

        NodeBuilder typesNotIndexed = withTypes.build().builder();
        NodeBuilder properties = typesNotIndexed.getChildNode("indexRules").getChildNode("nt:base")
                .getChildNode("properties");
        for (String name : properties.getChildNodeNames()) {
            NodeBuilder property = properties.getChildNode(name);
            if (!"title".equals(property.getString("name"))) {
                property.setProperty("index", false);
            }
        }
        assertEquals("*:*", typeRestrictedQuery(typesNotIndexed.getNodeState()));
    }

    @Test
    public void descendantRestrictionIsOneAncestorTerm() {
        Filter underContent = mock(Filter.class);
        when(underContent.matchesAllTypes()).thenReturn(true);
        when(underContent.getPropertyRestrictions()).thenReturn(List.of());
        when(underContent.getPathRestriction()).thenReturn(Filter.PathRestriction.ALL_CHILDREN);
        when(underContent.getPath()).thenReturn("/content");
        LuceneNgIndexDefinition definition = new LuceneNgIndexDefinition(
                INITIAL_CONTENT, new IndexDefinitionBuilder().build(), "/oak:index/test");

        assertEquals(new BooleanQuery.Builder()
                        .add(new MatchAllDocsQuery(), Occur.MUST)
                        .add(new TermQuery(new Term(FieldNames.ANCESTORS, "/content")), Occur.FILTER)
                        .build(),
                LuceneNgQueryBuilder.query(underContent, new PlanResult("/oak:index/test", definition, null)));
    }

    @Test
    public void multiValuedOrderedPropertiesSortBySortedSetDocValues() {
        IndexDefinitionBuilder builder = new IndexDefinitionBuilder();
        builder.indexRule("nt:unstructured").property("tags").ordered();
        LuceneNgIndexDefinition definition = new LuceneNgIndexDefinition(INITIAL_CONTENT, builder.build(), "/oak:index/test");

        assertEquals(new SortedSetSortField("tags", false), LuceneNgQueryBuilder.sortField(
                new OrderEntry("tags", Type.STRING, OrderEntry.Order.ASCENDING), definition,
                field -> DocValuesType.SORTED_SET));
    }

    private static PropertyDefinition declared(String name, String type) {
        IndexDefinitionBuilder builder = new IndexDefinitionBuilder();
        builder.indexRule("nt:base").property(name).propertyIndex().type(type);
        return new LuceneNgIndexDefinition(INITIAL_CONTENT, builder.build(), "/oak:index/test")
                .getApplicableIndexingRule("nt:base").getConfig(name);
    }

    private static String typeRestrictedQuery(NodeState definitionState) {
        LuceneNgIndexDefinition definition = new LuceneNgIndexDefinition(INITIAL_CONTENT, definitionState, "/oak:index/test");
        IndexingRule rule = definition.getApplicableIndexingRule("nt:base");
        Filter filter = mock(Filter.class);
        when(filter.getPropertyRestrictions()).thenReturn(List.of());
        when(filter.getPathRestriction()).thenReturn(Filter.PathRestriction.NO_RESTRICTION);
        when(filter.getPrimaryTypes()).thenReturn(Set.of("nt:file"));
        when(filter.getMixinTypes()).thenReturn(Set.of("mix:referenceable"));
        return LuceneNgQueryBuilder.query(filter, new PlanResult("/oak:index/test", definition, rule)).toString();
    }

    private static void assertFailsWithoutLeakingSecret(Class<? extends Throwable> expected, Executable operation) {
        Logger logger = (Logger) LoggerFactory.getLogger(LuceneNgQueryBuilder.class);
        ListAppender<ILoggingEvent> appender = new ListAppender<>();
        appender.start();
        logger.addAppender(appender);
        try {
            Throwable error = assertThrows(expected, operation);
            for (Throwable cause = error; cause != null; cause = cause.getCause()) {
                assertFalse(String.valueOf(cause.getMessage()).contains(SECRET));
            }
            assertFalse(appender.list.isEmpty(), "The error must remain visible");
            for (ILoggingEvent event : appender.list) {
                assertFalse(event.getFormattedMessage().contains(SECRET));
                if (event.getThrowableProxy() != null) {
                    assertFalse(ThrowableProxyUtil.asString(event.getThrowableProxy()).contains(SECRET));
                }
            }
        } finally {
            logger.detachAppender(appender);
            appender.stop();
        }
    }
}
