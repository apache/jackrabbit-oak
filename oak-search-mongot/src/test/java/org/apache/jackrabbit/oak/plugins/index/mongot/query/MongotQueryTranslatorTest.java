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
package org.apache.jackrabbit.oak.plugins.index.mongot.query;

import java.util.Date;
import java.util.List;

import javax.jcr.PropertyType;

import org.apache.jackrabbit.oak.plugins.index.IndexConstants;
import org.apache.jackrabbit.oak.plugins.index.mongot.MongotIndexDefinition;
import org.apache.jackrabbit.oak.plugins.index.mongot.index.MongoFieldNames;
import org.apache.jackrabbit.oak.plugins.index.mongot.util.MongotIndexDefinitionBuilder;
import org.apache.jackrabbit.oak.plugins.index.search.FieldNames;
import org.apache.jackrabbit.oak.plugins.index.search.FulltextIndexConstants;
import org.apache.jackrabbit.oak.plugins.index.search.PropertyDefinition;
import org.apache.jackrabbit.oak.plugins.index.search.spi.query.FulltextIndexPlanner.PlanResult;
import org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState;
import org.apache.jackrabbit.oak.plugins.memory.PropertyValues;
import org.apache.jackrabbit.oak.query.ast.Operator;
import org.apache.jackrabbit.oak.query.index.FilterImpl;
import org.apache.jackrabbit.oak.spi.query.Filter;
import org.apache.jackrabbit.oak.spi.query.QueryConstants;
import org.apache.jackrabbit.oak.spi.query.fulltext.FullTextAnd;
import org.apache.jackrabbit.oak.spi.query.fulltext.FullTextContains;
import org.apache.jackrabbit.oak.spi.query.fulltext.FullTextOr;
import org.apache.jackrabbit.oak.spi.query.fulltext.FullTextTerm;
import org.bson.Document;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class MongotQueryTranslatorTest {

    private static final String TITLE_PATH = "analyzed."
            + MongoFieldNames.encodeProperty("jcr:title");

    @Test
    public void translatesPropertyAndNodeScopedTerms() {
        Document propertyExpected = new Document("text", new Document("path", TITLE_PATH)
                .append("query", "oak"));
        Document nodeExpected = new Document("text", new Document("path", MongoFieldNames.FULLTEXT)
                .append("query", "connector"));

        assertEquals(propertyExpected, MongotQueryTranslator.translateFullText(
                new FullTextTerm("jcr:title", "oak", false, false, null)).searchOperator());
        assertEquals(nodeExpected, MongotQueryTranslator.translateFullText(
                new FullTextTerm("*", "connector", false, false, null)).searchOperator());
    }

    @Test
    public void appliesConfiguredSynonymsToTextQueries() {
        MongotIndexDefinitionBuilder builder = new MongotIndexDefinitionBuilder();
        builder.indexRule("nt:base").property("title").analyzed().nodeScopeIndex();
        builder.getBuilderTree().addChild(FulltextIndexConstants.ANALYZERS)
                .addChild(FulltextIndexConstants.ANL_DEFAULT)
                .addChild(FulltextIndexConstants.ANL_FILTERS)
                .addChild("Synonym");
        MongotIndexDefinition definition = new MongotIndexDefinition(EmptyNodeState.EMPTY_NODE,
                builder.build(), "/oak:index/synonyms");

        Document operator = MongotQueryTranslator.translateFullText(
                new FullTextTerm("title", "plane", false, false, null), definition)
                .searchOperator();

        assertEquals(new Document("text", new Document("path", "analyzed."
                        + MongoFieldNames.encodeProperty("title"))
                        .append("query", "plane")
                        .append("synonyms", "oak_synonyms")), operator);
    }

    @Test
    public void containsScopeOverridesUnscopedBaseTerm() {
        FullTextContains contains = new FullTextContains("jcr:title", "oak",
                new FullTextTerm(null, "oak", false, false, null));

        assertEquals(new Document("text", new Document("path", TITLE_PATH).append("query", "oak")),
                MongotQueryTranslator.translateFullText(contains).searchOperator());
    }

    @Test
    public void placesAndNegationInMustNot() {
        FullTextAnd expression = new FullTextAnd(List.of(
                new FullTextTerm("jcr:title", "oak", false, false, null),
                new FullTextTerm("jcr:title", "elastic", true, false, null)));

        Document expected = new Document("compound", new Document()
                .append("must", List.of(new Document("text",
                        new Document("path", TITLE_PATH).append("query", "oak"))))
                .append("mustNot", List.of(new Document("text",
                        new Document("path", TITLE_PATH).append("query", "elastic")))));
        assertEquals(expected, MongotQueryTranslator.translateFullText(expression).searchOperator());
    }

    @Test
    public void analyzesCompatibleAndTermsTogetherWithAllMatchCriteria() {
        FullTextAnd expression = new FullTextAnd(List.of(
                new FullTextTerm("jcr:title", "quick", false, false, null),
                new FullTextTerm("jcr:title", "brown", false, false, null)));

        Document expected = new Document("compound", new Document("must", List.of(
                new Document("text", new Document("path", TITLE_PATH)
                        .append("query", "quick brown")
                        .append("matchCriteria", "all")))));

        assertEquals(expected, MongotQueryTranslator.translateFullText(expression).searchOperator());
    }

    @Test
    public void translatesOrWithMinimumShouldMatch() {
        FullTextOr expression = new FullTextOr(List.of(
                new FullTextTerm("jcr:title", "oak", false, false, null),
                new FullTextTerm("jcr:title", "mongo", false, false, null)));

        Document expected = new Document("compound", new Document()
                .append("should", List.of(
                        new Document("text", new Document("path", TITLE_PATH).append("query", "oak")),
                        new Document("text", new Document("path", TITLE_PATH).append("query", "mongo"))))
                .append("minimumShouldMatch", 1));
        assertEquals(expected, MongotQueryTranslator.translateFullText(expression).searchOperator());
    }

    @Test
    public void translatesPhraseWildcardFuzzyAndBoost() {
        assertEquals(new Document("phrase", new Document("path", TITLE_PATH)
                        .append("query", "native connector")),
                MongotQueryTranslator.translateFullText(new FullTextTerm(
                        "jcr:title", "native connector", false, false, null)).searchOperator());
        assertEquals(new Document("wildcard", new Document("path", TITLE_PATH)
                        .append("query", "conn*").append("allowAnalyzedField", true)),
                MongotQueryTranslator.translateFullText(new FullTextTerm(
                        "jcr:title", "conn*", false, true, null)).searchOperator());
        assertEquals(new Document("text", new Document("path", TITLE_PATH)
                        .append("query", "conector")
                        .append("fuzzy", new Document("maxEdits", 1))),
                MongotQueryTranslator.translateFullText(new FullTextTerm(
                        "jcr:title", "conector~1", false, true, null)).searchOperator());
        assertEquals(new Document("text", new Document("path", TITLE_PATH)
                        .append("query", "conector")
                        .append("fuzzy", new Document("maxEdits", 1))),
                MongotQueryTranslator.translateFullText(new FullTextTerm(
                        "jcr:title", "conector~0.5", false, true, null)).searchOperator());
        assertEquals(new Document("text", new Document("path", TITLE_PATH)
                        .append("query", "oak")
                        .append("score", new Document("boost", new Document("value", 2.5d)))),
                MongotQueryTranslator.translateFullText(new FullTextTerm(
                        "jcr:title", "oak", false, true, "2.5")).searchOperator());
    }

    @Test
    public void translatesHyphenatedWildcardsAcrossAnalyzedTokens() {
        assertEquals(new Document("compound", new Document("must", List.of(
                        new Document("text", new Document("path", TITLE_PATH)
                                .append("query", "hello")),
                        new Document("wildcard", new Document("path", TITLE_PATH)
                                .append("query", "wor*")
                                .append("allowAnalyzedField", true))))),
                MongotQueryTranslator.translateFullText(new FullTextTerm(
                        "jcr:title", "hello-wor*", false, true, null)).searchOperator());
        assertEquals(new Document("compound", new Document("must", List.of(
                        new Document("wildcard", new Document("path", TITLE_PATH)
                                .append("query", "*hello")
                                .append("allowAnalyzedField", true)),
                        new Document("wildcard", new Document("path", TITLE_PATH)
                                .append("query", "wor*")
                                .append("allowAnalyzedField", true))))),
                MongotQueryTranslator.translateFullText(new FullTextTerm(
                        "jcr:title", "*hello-wor*", false, true, null)).searchOperator());
    }

    @Test
    public void translatesUnescapedClosingBracesToNoMatch() {
        Document exists = new Document("exists", new Document("path", TITLE_PATH));
        Document expected = new Document("compound", new Document()
                .append("must", List.of(exists))
                .append("mustNot", List.of(exists)));

        assertEquals(expected, MongotQueryTranslator.translateFullText(new FullTextContains(
                "jcr:title", "foo}", new FullTextTerm(
                "jcr:title", "foo}", false, false, null))).searchOperator());
        assertEquals(expected, MongotQueryTranslator.translateFullText(new FullTextContains(
                "jcr:title", "foo]", new FullTextTerm(
                "jcr:title", "foo]", false, false, null))).searchOperator());
    }

    @Test
    public void preservesEscapedClosingBraces() {
        assertEquals(new Document("text", new Document("path", TITLE_PATH).append("query", "{foo}")),
                MongotQueryTranslator.translateFullText(new FullTextContains(
                        "jcr:title", "\\{foo\\}", new FullTextTerm(
                        "jcr:title", "{foo}", false, true, null))).searchOperator());
        assertEquals(new Document("text", new Document("path", TITLE_PATH).append("query", "[foo]")),
                MongotQueryTranslator.translateFullText(new FullTextContains(
                        "jcr:title", "\\[foo\\]", new FullTextTerm(
                        "jcr:title", "[foo]", false, true, null))).searchOperator());
    }

    @Test
    public void declinesMalformedFuzzyAndBoostSyntax() {
        MongotQueryTranslation fuzzy = MongotQueryTranslator.translateFullText(
                new FullTextTerm("jcr:title", "oak~7", false, true, null));
        MongotQueryTranslation boost = MongotQueryTranslator.translateFullText(
                new FullTextTerm("jcr:title", "oak", false, true, "many"));

        assertFalse(fuzzy.isSupported());
        assertTrue(fuzzy.reason().contains("fuzzy"));
        assertFalse(boost.isSupported());
        assertTrue(boost.reason().contains("boost"));
    }

    @Test
    public void treatsNonNumericTildeAsAnalyzedText() {
        assertEquals(new Document("phrase", new Document("path", TITLE_PATH)
                        .append("query", "hello folks")),
                MongotQueryTranslator.translateFullText(new FullTextTerm(
                        "jcr:title", "hello~folks", false, true, null)).searchOperator());
    }

    @Test
    public void expandsNodeScopedSearchAcrossBoostedProperties() {
        MongotIndexDefinitionBuilder builder = new MongotIndexDefinitionBuilder();
        builder.indexRule("nt:base").property("a").analyzed().nodeScopeIndex().boost(10);
        builder.indexRule("nt:base").property("b").analyzed().nodeScopeIndex().boost(100);
        MongotIndexDefinition definition = new MongotIndexDefinition(EmptyNodeState.EMPTY_NODE,
                builder.build(), "/oak:index/boosted");

        MongotQueryTranslation translation = MongotQueryTranslator.translateFullText(
                new FullTextTerm("*", "hello", false, false, null), definition);

        assertEquals(new Document("compound", new Document("should", List.of(
                        new Document("text", new Document("path", MongoFieldNames.FULLTEXT)
                                .append("query", "hello")),
                        new Document("text", new Document("path", "analyzed."
                                + MongoFieldNames.encodeProperty("a"))
                                .append("query", "hello")
                                .append("score", new Document("boost", new Document("value", 10.0d)))),
                        new Document("text", new Document("path", "analyzed."
                                + MongoFieldNames.encodeProperty("b"))
                                .append("query", "hello")
                                .append("score", new Document("boost", new Document("value", 100.0d))))))
                        .append("minimumShouldMatch", 1)),
                translation.searchOperator());
    }

    @Test
    public void expandsNodeScopedSearchAcrossDynamicBoostTokens() {
        MongotIndexDefinitionBuilder builder = new MongotIndexDefinitionBuilder();
        builder.indexRule("nt:base").property("tags")
                .getBuilderTree().setProperty(FulltextIndexConstants.PROP_DYNAMIC_BOOST, true);
        MongotIndexDefinition definition = new MongotIndexDefinition(EmptyNodeState.EMPTY_NODE,
                builder.build(), "/oak:index/dynamic");

        Document translated = MongotQueryTranslator.translateFullText(
                new FullTextTerm("*", "plant", false, false, null), definition)
                .searchOperator();
        Document dynamic = translated.get("compound", Document.class)
                .getList("should", Document.class).get(1).get("text", Document.class);

        assertEquals(MongoFieldNames.DYNAMIC_BOOST_TOKENS, dynamic.getString("path"));
        assertEquals(new Document("function", new Document("path", new Document("value",
                        MongoFieldNames.DYNAMIC_BOOST_SCORES + "."
                                + MongoFieldNames.encodeProperty("plant"))
                        .append("undefined", 0.0d))),
                dynamic.get("score", Document.class));
    }

    @Test
    public void scoresEachDynamicBoostTermInAMultiTermQuery() {
        MongotIndexDefinitionBuilder builder = new MongotIndexDefinitionBuilder();
        builder.indexRule("nt:base").property("tags")
                .getBuilderTree().setProperty(FulltextIndexConstants.PROP_DYNAMIC_BOOST, true);
        MongotIndexDefinition definition = new MongotIndexDefinition(EmptyNodeState.EMPTY_NODE,
                builder.build(), "/oak:index/dynamic");

        List<Document> clauses = MongotQueryTranslator.translateFullText(
                        new FullTextTerm("*", "blue flower", false, false, null), definition)
                .searchOperator().get("compound", Document.class)
                .getList("should", Document.class);

        assertEquals(3, clauses.size());
        Document blue = clauses.get(1).get("phrase", Document.class);
        Document flower = clauses.get(2).get("phrase", Document.class);
        assertEquals("blue", blue.getString("query"));
        assertEquals("flower", flower.getString("query"));
        assertEquals(MongoFieldNames.DYNAMIC_BOOST_SCORES + "."
                        + MongoFieldNames.encodeProperty("blue"),
                blue.get("score", Document.class).get("function", Document.class)
                        .get("path", Document.class).get("value"));
    }

    @Test
    public void scoresDynamicBoostWithoutUsingTagsForFullTextRecall() {
        MongotIndexDefinitionBuilder builder = new MongotIndexDefinitionBuilder();
        builder.indexRule("nt:base").property("tags").getBuilderTree()
                .setProperty(FulltextIndexConstants.PROP_DYNAMIC_BOOST, true);
        builder.indexRule("nt:base").property("tags").getBuilderTree()
                .setProperty("useInFullTextQuery", false);
        MongotIndexDefinition definition = new MongotIndexDefinition(EmptyNodeState.EMPTY_NODE,
                builder.build(), "/oak:index/dynamic");

        Document compound = MongotQueryTranslator.translateFullText(
                        new FullTextTerm("*", "plant", false, false, null), definition)
                .searchOperator().get("compound", Document.class);

        Document requiredRecall = compound.getList("must", Document.class).get(0)
                .get("compound", Document.class).getList("should", Document.class).get(0);
        assertEquals(new Document("text", new Document("path", MongoFieldNames.FULLTEXT)
                .append("query", "plant")), requiredRecall);
        Document dynamic = compound.getList("should", Document.class).get(0)
                .get("text", Document.class);
        assertEquals(MongoFieldNames.DYNAMIC_BOOST_TOKENS, dynamic.getString("path"));
        assertEquals("plant", dynamic.getString("query"));
    }

    @Test
    public void scoresPropertyScopedHyphenatedTermsWithoutChangingRecall() {
        MongotIndexDefinitionBuilder builder = new MongotIndexDefinitionBuilder();
        builder.indexRule("nt:base").property("tags")
                .getBuilderTree().setProperty(FulltextIndexConstants.PROP_DYNAMIC_BOOST, true);
        MongotIndexDefinition definition = new MongotIndexDefinition(EmptyNodeState.EMPTY_NODE,
                builder.build(), "/oak:index/dynamic");

        Document compound = MongotQueryTranslator.translateFullText(
                        new FullTextTerm("jcr:title", "red-flower", false, false, null), definition)
                .searchOperator().get("compound", Document.class);

        assertEquals(List.of(new Document("text", new Document("path", TITLE_PATH)
                .append("query", "red-flower"))), compound.getList("must", Document.class));
        List<Document> dynamic = compound.getList("should", Document.class);
        assertEquals(List.of("red", "flower"), dynamic.stream()
                .map(clause -> clause.get("text", Document.class).getString("query"))
                .toList());
        assertEquals(MongoFieldNames.DYNAMIC_BOOST_SCORES + "."
                        + MongoFieldNames.encodeProperty("red"),
                dynamic.get(0).get("text", Document.class)
                        .get("score", Document.class).get("function", Document.class)
                        .get("path", Document.class).get("value"));
    }

    @Test
    public void translatesPathRestrictionsToStructuralMetadata() {
        FilterImpl exact = FilterImpl.newTestInstance();
        exact.restrictPath("/content/a", Filter.PathRestriction.EXACT);
        FilterImpl children = FilterImpl.newTestInstance();
        children.restrictPath("/content", Filter.PathRestriction.DIRECT_CHILDREN);
        FilterImpl descendants = FilterImpl.newTestInstance();
        descendants.restrictPath("/content", Filter.PathRestriction.ALL_CHILDREN);

        assertEquals(List.of(new Document("$match", new Document(MongoFieldNames.PATH, "/content/a"))),
                MongotQueryTranslator.translateFilter(exact).pipeline());
        assertEquals(List.of(new Document("$match", new Document(MongoFieldNames.PARENT, "/content"))),
                MongotQueryTranslator.translateFilter(children).pipeline());
        assertEquals(List.of(new Document("$match", new Document(MongoFieldNames.ANCESTORS, "/content"))),
                MongotQueryTranslator.translateFilter(descendants).pipeline());
    }

    @Test
    public void skipsNonFullTextConstraintsWhenPlannerDefersEvaluation() {
        FilterImpl filter = FilterImpl.newTestInstance();
        filter.restrictPath("/content", Filter.PathRestriction.ALL_CHILDREN);

        PlanResult planResult = mock(PlanResult.class);
        when(planResult.evaluateNonFullTextConstraints()).thenReturn(false);

        assertEquals(List.of(), MongotQueryTranslator.translateFilter(filter, planResult).pipeline());
    }

    @Test
    public void ignoresPlannerOptionsAndResidualUnindexedRestrictions() {
        FilterImpl filter = FilterImpl.newTestInstance();
        filter.restrictProperty(IndexConstants.INDEX_TAG_OPTION, Operator.EQUAL,
                PropertyValues.newString("selected"));
        filter.restrictProperty("indexed", Operator.EQUAL,
                PropertyValues.newString("yes"));
        filter.restrictProperty("residual", Operator.EQUAL,
                PropertyValues.newString("later"));

        PlanResult planResult = mock(PlanResult.class);
        when(planResult.evaluateNonFullTextConstraints()).thenReturn(true);
        when(planResult.hasProperty("indexed")).thenReturn(true);

        assertEquals(List.of(new Document("$match", new Document(
                        "typed." + MongoFieldNames.encodeProperty("indexed"), "yes"))),
                MongotQueryTranslator.translateFilter(filter, planResult).pipeline());
    }

    @Test
    public void translatesTypedEqualityRangeInAndNullRestrictions() {
        FilterImpl filter = FilterImpl.newTestInstance();
        filter.restrictProperty("status", Operator.EQUAL, PropertyValues.newString("published"));
        filter.restrictProperty("price", Operator.GREATER_OR_EQUAL, PropertyValues.newLong(10L));
        filter.restrictProperty("price", Operator.LESS_THAN, PropertyValues.newLong(20L));
        filter.restrictPropertyAsList("tag", List.of(
                PropertyValues.newString("one"), PropertyValues.newString("two")));
        filter.restrictProperty("optional", Operator.EQUAL, null);
        filter.restrictProperty("present", Operator.NOT_EQUAL, null);

        Document expected = new Document("$match", new Document("$and", List.of(
                new Document(MongoFieldNames.NULL_PROPERTIES,
                        MongoFieldNames.encodeProperty("optional")),
                new Document("typed." + MongoFieldNames.encodeProperty("present"),
                        new Document("$exists", true)),
                new Document("typed." + MongoFieldNames.encodeProperty("price"),
                        new Document("$gte", 10L).append("$lt", 20L)),
                new Document("typed." + MongoFieldNames.encodeProperty("status"), "published"),
                new Document("typed." + MongoFieldNames.encodeProperty("tag"),
                        new Document("$in", List.of("one", "two"))))));
        assertEquals(List.of(expected), MongotQueryTranslator.translateFilter(filter).pipeline());
    }

    @Test
    public void translatesDateValuesToBsonDates() {
        FilterImpl filter = FilterImpl.newTestInstance();
        filter.restrictProperty("date", Operator.GREATER_OR_EQUAL,
                PropertyValues.newDate("2020-12-07T10:23:33.933-09:00"));

        Document match = MongotQueryTranslator.translateFilter(filter).pipeline().get(0)
                .get("$match", Document.class);
        Object value = match.get("typed." + MongoFieldNames.encodeProperty("date"), Document.class)
                .get("$gte");
        assertTrue(value instanceof Date);
        assertEquals(1607369013933L, ((Date) value).getTime());
    }

    @Test
    public void usesIndexPropertyTypeWhenTheQueryLiteralIsAString() {
        FilterImpl filter = FilterImpl.newTestInstance();
        filter.restrictProperty("date", Operator.EQUAL,
                PropertyValues.newString("2021-01-22T01:02:03.000Z"));

        PropertyDefinition property = mock(PropertyDefinition.class);
        when(property.isTypeDefined()).thenReturn(true);
        when(property.getType()).thenReturn(PropertyType.DATE);
        PlanResult planResult = mock(PlanResult.class);
        when(planResult.evaluateNonFullTextConstraints()).thenReturn(true);
        when(planResult.hasProperty("date")).thenReturn(true);
        when(planResult.getPropDefn(any())).thenReturn(property);

        Document match = MongotQueryTranslator.translateFilter(filter, planResult).pipeline().get(0)
                .get("$match", Document.class);
        Object value = match.get("typed." + MongoFieldNames.encodeProperty("date"));
        assertTrue(value instanceof Date);
        assertEquals(1611277323000L, ((Date) value).getTime());
    }

    @Test
    public void includesParseableBooleanCandidatesForInRestrictions() {
        FilterImpl filter = FilterImpl.newTestInstance();
        filter.restrictPropertyAsList("booleanField", List.of(
                PropertyValues.newString("true"),
                PropertyValues.newString("True"),
                PropertyValues.newString("InvalidBool")));

        Document match = MongotQueryTranslator.translateFilter(filter).pipeline().get(0)
                .get("$match", Document.class);
        assertEquals(List.of("true", true, "True", "InvalidBool"),
                match.get("typed." + MongoFieldNames.encodeProperty("booleanField"), Document.class)
                        .getList("$in", Object.class));
    }

    @Test
    public void translatesNodeNameRestrictions() {
        FilterImpl filter = FilterImpl.newTestInstance();
        filter.restrictProperty(QueryConstants.RESTRICTION_LOCAL_NAME, Operator.LIKE,
                PropertyValues.newString("camel%"));
        filter.restrictProperty(QueryConstants.FUNCTION_RESTRICTION_PREFIX + "@"
                        + QueryConstants.RESTRICTION_LOCAL_NAME,
                Operator.LIKE, PropertyValues.newString("camel%"));

        assertTrue(MongotQueryTranslator.translateFilter(filter).isSupported());
        assertEquals(List.of(new Document("$match", new Document(
                        "typed." + MongoFieldNames.encodeProperty(FieldNames.NODE_NAME),
                        new Document("$regex", "^camel.*$")))),
                MongotQueryTranslator.translateFilter(filter).pipeline());
    }

    @Test
    public void translatesLikeAndNotEqualWithoutMatchingMissingProperties() {
        FilterImpl filter = FilterImpl.newTestInstance();
        filter.restrictProperty("status", Operator.NOT_EQUAL, PropertyValues.newString("draft"));
        filter.restrictProperty("title", Operator.LIKE, PropertyValues.newString("Mongo%_POC"));

        String status = "typed." + MongoFieldNames.encodeProperty("status");
        String title = "typed." + MongoFieldNames.encodeProperty("title");
        Document expected = new Document("$match", new Document("$and", List.of(
                new Document("$and", List.of(
                        new Document(status, new Document("$exists", true)),
                        new Document(status, new Document("$ne", "draft")))),
                new Document(title, new Document("$regex", "^Mongo.*.POC$")))));
        assertEquals(List.of(expected), MongotQueryTranslator.translateFilter(filter).pipeline());
    }

    @Test
    public void functionRestrictionMakesSourceNotNullMarkerRedundant() {
        FilterImpl filter = FilterImpl.newTestInstance();
        String function = "function*lower*@status";
        filter.restrictProperty(function, Operator.EQUAL, PropertyValues.newString("published"));
        filter.restrictProperty("status", Operator.NOT_EQUAL, null);

        Document expected = new Document("$match", new Document(
                "typed." + MongoFieldNames.encodeProperty(function), "published"));
        assertEquals(List.of(expected), MongotQueryTranslator.translateFilter(filter).pipeline());
    }

    @Test
    public void leavesEnrichedResultPropertiesForTheResultAdapter() {
        FilterImpl filter = FilterImpl.newTestInstance();
        filter.restrictProperty("rep:excerpt", Operator.EQUAL,
                PropertyValues.newString("rep:excerpt()"));

        MongotQueryTranslation translation = MongotQueryTranslator.translateFilter(filter);
        assertTrue(translation.isSupported());
        assertEquals(List.of(), translation.pipeline());
    }

    @Test
    public void leavesSimilarityNativeRestrictionForSearchTranslation() {
        FilterImpl filter = FilterImpl.newTestInstance();
        filter.restrictProperty("native*lucene", Operator.EQUAL,
                PropertyValues.newString(
                        "mlt?mlt.fl=:path&mlt.mindf=0&stream.body=/content/reference"));

        MongotQueryTranslation translation = MongotQueryTranslator.translateFilter(filter);

        assertTrue(translation.isSupported());
        assertEquals(List.of(), translation.pipeline());
    }
}
