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

import java.util.ArrayList;
import java.util.Comparator;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.jackrabbit.oak.api.PropertyValue;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.plugins.index.IndexConstants;
import org.apache.jackrabbit.oak.plugins.index.mongot.MongotIndexDefinition;
import org.apache.jackrabbit.oak.plugins.index.mongot.index.MongoFieldNames;
import org.apache.jackrabbit.oak.plugins.index.search.FieldNames;
import org.apache.jackrabbit.oak.plugins.index.search.IndexDefinition;
import org.apache.jackrabbit.oak.plugins.index.search.PropertyDefinition;
import org.apache.jackrabbit.oak.plugins.index.search.spi.query.FulltextIndex;
import org.apache.jackrabbit.oak.plugins.index.search.spi.query.FulltextIndexPlanner.PlanResult;
import org.apache.jackrabbit.oak.plugins.index.search.util.DataConversionUtil;
import org.apache.jackrabbit.oak.spi.query.Filter;
import org.apache.jackrabbit.oak.spi.query.Filter.PropertyRestriction;
import org.apache.jackrabbit.oak.spi.query.QueryConstants;
import org.apache.jackrabbit.oak.spi.query.fulltext.FullTextAnd;
import org.apache.jackrabbit.oak.spi.query.fulltext.FullTextContains;
import org.apache.jackrabbit.oak.spi.query.fulltext.FullTextExpression;
import org.apache.jackrabbit.oak.spi.query.fulltext.FullTextOr;
import org.apache.jackrabbit.oak.spi.query.fulltext.FullTextTerm;
import org.bson.Document;

public final class MongotQueryTranslator {

    private static final Pattern FUZZY = Pattern.compile("^(.*)~([0-9]+(?:\\.[0-9]+)?)$");
    private static final Set<String> UNSUPPORTED_RESULT_PROPERTIES = Set.of(
            "rep:suggest", "rep:spellcheck", "rep:similar");

    private MongotQueryTranslator() {
    }

    public static MongotQueryTranslation translateFullText(FullTextExpression expression) {
        if (expression == null) {
            return MongotQueryTranslation.unsupported("full-text expression is missing");
        }
        return translate(expression, null, null);
    }

    public static MongotQueryTranslation translateFullText(FullTextExpression expression,
                                                           MongotIndexDefinition definition) {
        return translateFullText(expression, definition, null);
    }

    public static MongotQueryTranslation translateFullText(FullTextExpression expression,
                                                           MongotIndexDefinition definition,
                                                           PlanResult planResult) {
        if (expression == null) {
            return MongotQueryTranslation.unsupported("full-text expression is missing");
        }
        MongotQueryTranslation translation = translate(expression, null, planResult);
        if (!translation.isSupported()) {
            return translation;
        }
        if (definition.hasSynonyms()) {
            translation = MongotQueryTranslation.supported(
                    applySynonyms(translation.searchOperator()));
        }
        Document withoutStopwords = removeStopwords(translation.searchOperator(), definition);
        if (withoutStopwords == null) {
            Document exists = new Document("exists", new Document("path", MongoFieldNames.PATH));
            withoutStopwords = new Document("compound", new Document("must", List.of(exists))
                    .append("mustNot", List.of(exists)));
        }
        translation = MongotQueryTranslation.supported(withoutStopwords);
        Map<String, Double> boosts = nodeScopeBoosts(definition);
        boolean dynamicBoost = definition.hasDynamicBoost();
        boolean fullTextDynamicBoost = definition.hasFullTextDynamicBoost();
        if (!dynamicBoost && boosts.values().stream().noneMatch(value -> value != 1.0d)) {
            return translation;
        }
        Document expanded = expandNodeScopeOperator(
                translation.searchOperator(), boosts, fullTextDynamicBoost);
        if (dynamicBoost && (!fullTextDynamicBoost
                || !containsNodeScopeOperator(translation.searchOperator()))) {
            List<Document> dynamicClauses = new ArrayList<>();
            collectDynamicBoostClauses(translation.searchOperator(), dynamicClauses);
            if (!dynamicClauses.isEmpty()) {
                expanded = new Document("compound", new Document("must", List.of(expanded))
                        .append("should", dynamicClauses));
            }
        }
        return MongotQueryTranslation.supported(expanded);
    }

    private static Document applySynonyms(Document operator) {
        String operatorName = operator.keySet().stream().findFirst().orElse(null);
        if ("text".equals(operatorName) || "phrase".equals(operatorName)) {
            Document body = operator.get(operatorName, Document.class);
            if (body != null && !body.containsKey("fuzzy")) {
                return new Document(operatorName, new Document(body)
                        .append("synonyms", MongotIndexDefinition.SYNONYM_MAPPING_NAME));
            }
            return operator;
        }
        Document compound = operator.get("compound", Document.class);
        if (compound == null) {
            return operator;
        }
        Document expanded = new Document(compound);
        for (String clause : List.of("must", "should", "mustNot", "filter")) {
            List<Document> children = compound.getList(clause, Document.class);
            if (children != null) {
                expanded.put(clause, children.stream()
                        .map(MongotQueryTranslator::applySynonyms).toList());
            }
        }
        return new Document("compound", expanded);
    }

    private static Document removeStopwords(Document operator, MongotIndexDefinition definition) {
        String operatorName = operator.keySet().stream().findFirst().orElse(null);
        if ("text".equals(operatorName)) {
            Document body = operator.get("text", Document.class);
            if (body != null && body.get("query") instanceof String
                    && definition.isFullTextStopword(body.getString("query"))) {
                return null;
            }
            return operator;
        }
        Document compound = operator.get("compound", Document.class);
        if (compound == null) {
            return operator;
        }
        Document filtered = new Document(compound);
        for (String clause : List.of("must", "should", "mustNot", "filter")) {
            List<Document> children = compound.getList(clause, Document.class);
            if (children == null) {
                continue;
            }
            List<Document> retained = children.stream()
                    .map(child -> removeStopwords(child, definition))
                    .filter(java.util.Objects::nonNull)
                    .toList();
            if (retained.isEmpty()) {
                filtered.remove(clause);
                if ("should".equals(clause)) {
                    filtered.remove("minimumShouldMatch");
                }
            } else {
                filtered.put(clause, retained);
            }
        }
        return filtered.isEmpty() ? null : new Document("compound", filtered);
    }

    public static MongotQueryTranslation translateFilter(Filter filter) {
        return translateFilter(filter, null);
    }

    public static MongotQueryTranslation translateFilter(Filter filter, PlanResult planResult) {
        if (planResult != null && !planResult.evaluateNonFullTextConstraints()) {
            return MongotQueryTranslation.supported(null, List.of());
        }

        List<Document> clauses = new ArrayList<>();
        Document pathClause = translatePath(filter.getPath(), filter.getPathRestriction(), planResult);
        if (pathClause != null) {
            clauses.add(pathClause);
        }

        Map<String, List<PropertyRestriction>> restrictions = new LinkedHashMap<>();
        filter.getPropertyRestrictions().stream()
                .filter(restriction -> shouldPushProperty(restriction, planResult))
                .sorted(Comparator.comparing(restriction -> restriction.propertyName))
                .forEach(restriction -> restrictions
                        .computeIfAbsent(propertyName(restriction, planResult),
                                ignored -> new ArrayList<>())
                        .add(restriction));
        for (Map.Entry<String, List<PropertyRestriction>> entry : restrictions.entrySet()) {
            boolean functionGuaranteesPresence = functionReferencesProperty(
                    restrictions.keySet(), entry.getKey());
            MongotQueryTranslation translated = translateProperty(
                    entry.getKey(), entry.getValue(), functionGuaranteesPresence, planResult);
            if (!translated.isSupported()) {
                return translated;
            }
            clauses.addAll(translated.pipeline());
        }

        if (!filter.matchesAllTypes()
                && (planResult == null || planResult.evaluateNodeTypeRestriction())) {
            List<Document> typeClauses = new ArrayList<>();
            if (!filter.getPrimaryTypes().isEmpty()) {
                typeClauses.add(new Document(MongoFieldNames.PRIMARY_TYPE,
                        new Document("$in", new ArrayList<>(filter.getPrimaryTypes()))));
            }
            if (!filter.getMixinTypes().isEmpty()) {
                typeClauses.add(new Document(MongoFieldNames.MIXIN_TYPES,
                        new Document("$in", new ArrayList<>(filter.getMixinTypes()))));
            }
            if (typeClauses.size() == 1) {
                clauses.add(typeClauses.get(0));
            } else if (!typeClauses.isEmpty()) {
                clauses.add(new Document("$or", typeClauses));
            }
        }

        if (clauses.isEmpty()) {
            return MongotQueryTranslation.supported(null, List.of());
        }
        Document match = clauses.size() == 1 ? clauses.get(0) : new Document("$and", clauses);
        return MongotQueryTranslation.supported(null, List.of(new Document("$match", match)));
    }

    private static MongotQueryTranslation translate(FullTextExpression expression,
                                                   String inheritedProperty,
                                                   PlanResult planResult) {
        if (expression instanceof FullTextContains) {
            FullTextContains contains = (FullTextContains) expression;
            String property = contains.getPropertyName() == null
                    ? inheritedProperty
                    : contains.getPropertyName();
            if (hasUnescapedClosingDelimiter(contains.getRawText())) {
                Document exists = new Document("exists",
                        new Document("path", fulltextPath(property, planResult)));
                return MongotQueryTranslation.supported(new Document("compound", new Document()
                        .append("must", List.of(exists))
                        .append("mustNot", List.of(exists))));
            }
            return translate(contains.getBase(), property, planResult);
        }
        if (expression instanceof FullTextTerm) {
            return translateTerm((FullTextTerm) expression, inheritedProperty, planResult);
        }
        if (expression instanceof FullTextAnd) {
            return translateAnd((FullTextAnd) expression, inheritedProperty, planResult);
        }
        if (expression instanceof FullTextOr) {
            return translateOr((FullTextOr) expression, inheritedProperty, planResult);
        }
        return MongotQueryTranslation.unsupported("unsupported full-text expression: " + expression);
    }

    private static MongotQueryTranslation translateAnd(FullTextAnd and,
                                                      String inheritedProperty,
                                                      PlanResult planResult) {
        List<Document> must = new ArrayList<>();
        List<Document> mustNot = new ArrayList<>();
        for (FullTextExpression child : and.list) {
            MongotQueryTranslation translated = translate(child, inheritedProperty, planResult);
            if (!translated.isSupported()) {
                return translated;
            }
            Document operator = translated.searchOperator();
            Document compound = operator.get("compound", Document.class);
            if (compound != null && compound.size() == 1 && compound.containsKey("mustNot")) {
                mustNot.addAll(compound.getList("mustNot", Document.class));
            } else {
                must.add(operator);
            }
        }
        combineCompatibleTextClauses(must);
        Document compound = new Document();
        if (!must.isEmpty()) {
            compound.append("must", must);
        }
        if (!mustNot.isEmpty()) {
            compound.append("mustNot", mustNot);
        }
        return MongotQueryTranslation.supported(new Document("compound", compound));
    }

    private static void combineCompatibleTextClauses(List<Document> clauses) {
        Map<String, List<Document>> byPath = new LinkedHashMap<>();
        for (Document clause : clauses) {
            Document text = clause.get("text", Document.class);
            if (text != null && text.size() == 2 && text.get("query") instanceof String) {
                byPath.computeIfAbsent(text.getString("path"), ignored -> new ArrayList<>())
                        .add(clause);
            }
        }
        for (List<Document> compatible : byPath.values()) {
            if (compatible.size() < 2) {
                continue;
            }
            Document combined = new Document(compatible.get(0).get("text", Document.class))
                    .append("query", compatible.stream()
                            .map(clause -> clause.get("text", Document.class).getString("query"))
                            .collect(java.util.stream.Collectors.joining(" ")))
                    .append("matchCriteria", "all");
            clauses.removeAll(compatible);
            clauses.add(new Document("text", combined));
        }
    }

    private static MongotQueryTranslation translateOr(FullTextOr or,
                                                     String inheritedProperty,
                                                     PlanResult planResult) {
        List<Document> should = new ArrayList<>();
        for (FullTextExpression child : or.list) {
            MongotQueryTranslation translated = translate(child, inheritedProperty, planResult);
            if (!translated.isSupported()) {
                return translated;
            }
            should.add(translated.searchOperator());
        }
        return MongotQueryTranslation.supported(new Document("compound", new Document("should", should)
                .append("minimumShouldMatch", 1)));
    }

    private static MongotQueryTranslation translateTerm(FullTextTerm term,
                                                       String inheritedProperty,
                                                       PlanResult planResult) {
        String text = term.getText();
        if (text == null || text.isBlank()) {
            return MongotQueryTranslation.unsupported("full-text term is empty");
        }
        String property = term.getPropertyName() == null ? inheritedProperty : term.getPropertyName();
        String path = fulltextPath(property, planResult);

        MongotQueryTranslation positive = positiveTerm(path, text, term.getBoost());
        if (!positive.isSupported() || !term.isNot()) {
            return positive;
        }
        return MongotQueryTranslation.supported(new Document("compound",
                new Document("mustNot", List.of(positive.searchOperator()))));
    }

    private static String fulltextPath(String property, PlanResult planResult) {
        if (property == null || ".".equals(property)) {
            return MongoFieldNames.FULLTEXT;
        }
        if (FulltextIndex.isNodePath(property)) {
            if (planResult != null && planResult.isPathTransformed()) {
                property = PathUtils.getName(property);
            } else {
                return MongoFieldNames.RELATIVE_FULLTEXT + "."
                        + MongoFieldNames.encodeProperty(PathUtils.getParentPath(property));
            }
        } else if (planResult != null && planResult.isPathTransformed()) {
            property = PathUtils.getName(property);
        }
        return "*".equals(property)
                ? MongoFieldNames.FULLTEXT
                : MongoFieldNames.ANALYZED + "." + MongoFieldNames.encodeProperty(property);
    }

    private static MongotQueryTranslation positiveTerm(String path, String text, String boost) {
        String operatorName;
        Document operatorBody;
        boolean quoted = text.length() >= 2 && text.startsWith("\"") && text.endsWith("\"");
        if (text.indexOf(' ') >= 0 || quoted) {
            operatorName = "phrase";
            String query = quoted ? text.substring(1, text.length() - 1) : text;
            operatorBody = new Document("path", path).append("query", query);
        } else {
            Matcher fuzzy = FUZZY.matcher(text);
            if (fuzzy.matches()) {
                double fuzzyValue = Double.parseDouble(fuzzy.group(2));
                boolean similarity = fuzzyValue > 0.0d && fuzzyValue < 1.0d;
                int maxEdits = similarity
                        ? 1
                        : (int) fuzzyValue;
                if (maxEdits < 1 || maxEdits > 2 || (!similarity && maxEdits != fuzzyValue)
                        || fuzzy.group(1).isEmpty()) {
                    return MongotQueryTranslation.unsupported("unsupported fuzzy edit distance: " + text);
                }
                operatorName = "text";
                operatorBody = new Document("path", path)
                        .append("query", fuzzy.group(1))
                        .append("fuzzy", new Document("maxEdits", maxEdits));
            } else if (text.indexOf('*') >= 0 || text.indexOf('?') >= 0) {
                List<Document> hyphenated = hyphenatedWildcard(path, text);
                if (hyphenated.size() > 1) {
                    operatorName = "compound";
                    operatorBody = new Document("must", hyphenated);
                } else {
                    operatorName = "wildcard";
                    operatorBody = new Document("path", path)
                            .append("query", text)
                            .append("allowAnalyzedField", true);
                }
            } else if (text.indexOf('~') >= 0) {
                operatorName = "phrase";
                operatorBody = new Document("path", path)
                        .append("query", text.replace('~', ' '));
            } else {
                operatorName = "text";
                operatorBody = new Document("path", path).append("query", text);
            }
        }

        if (boost != null) {
            double value;
            try {
                value = Double.parseDouble(boost);
            } catch (NumberFormatException e) {
                return MongotQueryTranslation.unsupported("malformed full-text boost: " + boost);
            }
            if (!Double.isFinite(value) || value <= 0) {
                return MongotQueryTranslation.unsupported("invalid full-text boost: " + boost);
            }
            operatorBody.append("score", new Document("boost", new Document("value", value)));
        }
        return MongotQueryTranslation.supported(new Document(operatorName, operatorBody));
    }

    private static List<Document> hyphenatedWildcard(String path, String text) {
        if (text.indexOf('-') < 0) {
            return List.of();
        }
        List<Document> clauses = new ArrayList<>();
        for (String part : text.split("-")) {
            if (part.isEmpty()) {
                continue;
            }
            if (part.indexOf('*') >= 0 || part.indexOf('?') >= 0) {
                clauses.add(new Document("wildcard", new Document("path", path)
                        .append("query", part)
                        .append("allowAnalyzedField", true)));
            } else {
                clauses.add(new Document("text", new Document("path", path)
                        .append("query", part)));
            }
        }
        return clauses;
    }

    private static boolean hasUnescapedClosingDelimiter(String text) {
        boolean escaped = false;
        for (int i = 0; i < text.length(); i++) {
            char character = text.charAt(i);
            if (character == '\\') {
                escaped = !escaped;
                continue;
            }
            if (!escaped && (character == '}' || character == ']')) {
                return true;
            }
            escaped = false;
        }
        return false;
    }

    private static Map<String, Double> nodeScopeBoosts(IndexDefinition definition) {
        Map<String, Double> boosts = new LinkedHashMap<>();
        for (IndexDefinition.IndexingRule rule : definition.getDefinedRules()) {
            for (PropertyDefinition property : rule.getNodeScopeAnalyzedProps()) {
                boosts.merge(property.name, (double) property.boost, Math::max);
            }
        }
        return boosts;
    }

    private static Document expandNodeScopeOperator(Document operator, Map<String, Double> boosts,
                                                    boolean dynamicBoost) {
        Document compound = operator.get("compound", Document.class);
        if (compound != null) {
            Document expanded = new Document(compound);
            for (String clause : List.of("must", "should", "mustNot", "filter")) {
                List<Document> children = compound.getList(clause, Document.class);
                if (children != null) {
                    expanded.put(clause, children.stream()
                            .map(child -> expandNodeScopeOperator(child, boosts, dynamicBoost)).toList());
                }
            }
            return new Document("compound", expanded);
        }

        String operatorName = operator.keySet().stream().findFirst().orElse(null);
        if (operatorName == null || !("text".equals(operatorName)
                || "phrase".equals(operatorName) || "wildcard".equals(operatorName))) {
            return operator;
        }
        Document body = operator.get(operatorName, Document.class);
        if (body == null || !MongoFieldNames.FULLTEXT.equals(body.getString("path"))) {
            return operator;
        }

        List<Document> should = new ArrayList<>();
        should.add(operator);
        for (Map.Entry<String, Double> boost : boosts.entrySet()) {
            Document boostedBody = new Document(body);
            boostedBody.put("path", MongoFieldNames.ANALYZED + "."
                    + MongoFieldNames.encodeProperty(boost.getKey()));
            double queryBoost = 1.0d;
            Document score = body.get("score", Document.class);
            if (score != null) {
                Document configuredBoost = score.get("boost", Document.class);
                if (configuredBoost != null && configuredBoost.get("value") instanceof Number) {
                    queryBoost = ((Number) configuredBoost.get("value")).doubleValue();
                }
            }
            boostedBody.put("score", new Document("boost",
                    new Document("value", queryBoost * boost.getValue())));
            should.add(new Document(operatorName, boostedBody));
        }
        if (dynamicBoost) {
            String query = body.getString("query");
            if (query != null && !(query.indexOf('*') >= 0 || query.indexOf('?') >= 0)) {
                for (String term : dynamicTerms(query)) {
                    if (!term.isEmpty()) {
                        should.add(dynamicBoostClause(operatorName, body, term));
                    }
                }
            } else {
                Document dynamicBody = new Document(body);
                dynamicBody.put("path", MongoFieldNames.DYNAMIC_BOOST_TOKENS);
                dynamicBody.put("score", new Document("boost", new Document("value", 0.5d)));
                should.add(new Document(operatorName, dynamicBody));
            }
        }
        return new Document("compound", new Document("should", should)
                .append("minimumShouldMatch", 1));
    }

    private static boolean containsNodeScopeOperator(Document operator) {
        Document compound = operator.get("compound", Document.class);
        if (compound != null) {
            for (String clause : List.of("must", "should", "mustNot", "filter")) {
                List<Document> children = compound.getList(clause, Document.class);
                if (children != null && children.stream().anyMatch(
                        MongotQueryTranslator::containsNodeScopeOperator)) {
                    return true;
                }
            }
            return false;
        }
        Document body = operator.values().stream()
                .filter(Document.class::isInstance)
                .map(Document.class::cast)
                .findFirst().orElse(null);
        return body != null && MongoFieldNames.FULLTEXT.equals(body.getString("path"));
    }

    private static void collectDynamicBoostClauses(Document operator, List<Document> clauses) {
        Document compound = operator.get("compound", Document.class);
        if (compound != null) {
            for (String clause : List.of("must", "should", "filter")) {
                List<Document> children = compound.getList(clause, Document.class);
                if (children != null) {
                    children.forEach(child -> collectDynamicBoostClauses(child, clauses));
                }
            }
            return;
        }
        String operatorName = operator.keySet().stream().findFirst().orElse(null);
        if (!("text".equals(operatorName) || "phrase".equals(operatorName)
                || "wildcard".equals(operatorName))) {
            return;
        }
        Document body = operator.get(operatorName, Document.class);
        if (body == null) {
            return;
        }
        String query = body.getString("query");
        if (query == null || query.indexOf('*') >= 0 || query.indexOf('?') >= 0) {
            Document dynamicBody = new Document(body);
            dynamicBody.put("path", MongoFieldNames.DYNAMIC_BOOST_TOKENS);
            dynamicBody.put("score", new Document("boost", new Document("value", 0.5d)));
            clauses.add(new Document(operatorName, dynamicBody));
            return;
        }
        for (String term : dynamicTerms(query)) {
            clauses.add(dynamicBoostClause(operatorName, body, term));
        }
    }

    private static List<String> dynamicTerms(String query) {
        return java.util.Arrays.stream(query.trim().split("[^\\p{L}\\p{N}]+"))
                .filter(term -> !term.isEmpty())
                .toList();
    }

    private static Document dynamicBoostClause(String operatorName, Document body, String term) {
        Document dynamicBody = new Document(body);
        dynamicBody.put("path", MongoFieldNames.DYNAMIC_BOOST_TOKENS);
        dynamicBody.put("query", term);
        double queryBoost = configuredBoost(body);
        Document pathScore = new Document("path", new Document("value",
                MongoFieldNames.DYNAMIC_BOOST_SCORES + "."
                        + MongoFieldNames.encodeProperty(term.toLowerCase(Locale.ROOT)))
                .append("undefined", 0.0d));
        Object function = queryBoost == 1.0d
                ? pathScore
                : new Document("multiply", List.of(pathScore,
                        new Document("constant", queryBoost)));
        dynamicBody.put("score", new Document("function", function));
        return new Document(operatorName, dynamicBody);
    }

    private static double configuredBoost(Document body) {
        Document score = body.get("score", Document.class);
        if (score != null) {
            Document boost = score.get("boost", Document.class);
            if (boost != null && boost.get("value") instanceof Number) {
                return ((Number) boost.get("value")).doubleValue();
            }
        }
        return 1.0d;
    }

    private static String propertyName(PropertyRestriction restriction, PlanResult planResult) {
        if (planResult == null) {
            return restriction.propertyName;
        }
        String mapped = planResult.getPropertyName(restriction);
        return mapped == null ? restriction.propertyName : mapped;
    }

    private static boolean shouldPushProperty(PropertyRestriction restriction,
                                              PlanResult planResult) {
        String propertyName = restriction.propertyName;
        if (IndexConstants.INDEX_NAME_OPTION.equals(propertyName)
                || IndexConstants.INDEX_TAG_OPTION.equals(propertyName)) {
            return false;
        }
        if (planResult == null) {
            return true;
        }
        if (QueryConstants.REP_EXCERPT.equals(propertyName)
                || QueryConstants.REP_FACET.equals(propertyName)
                || "native*lucene".equals(propertyName)) {
            return true;
        }
        if (QueryConstants.RESTRICTION_LOCAL_NAME.equals(propertyName)) {
            return planResult.evaluateNodeNameRestriction();
        }
        return planResult.hasProperty(propertyName);
    }

    private static Document translatePath(String path,
                                          Filter.PathRestriction restriction,
                                          PlanResult planResult) {
        if (planResult != null && planResult.isPathTransformed()) {
            return translateTransformedPath(path, restriction, planResult);
        }
        switch (restriction) {
            case EXACT:
                return new Document(MongoFieldNames.PATH, path);
            case PARENT:
                return new Document(MongoFieldNames.PATH, PathUtils.getParentPath(path));
            case DIRECT_CHILDREN:
                return new Document(MongoFieldNames.PARENT, path);
            case ALL_CHILDREN:
                return new Document(MongoFieldNames.ANCESTORS, path);
            case NO_RESTRICTION:
                return null;
            default:
                throw new IllegalArgumentException("Unknown path restriction " + restriction);
        }
    }

    private static Document translateTransformedPath(String path,
                                                     Filter.PathRestriction restriction,
                                                     PlanResult planResult) {
        String parentPathSegment = planResult.getParentPathSegment();
        boolean concreteParentPath = parentPathSegment != null
                && parentPathSegment.indexOf('*') < 0;
        switch (restriction) {
            case EXACT:
                return concreteParentPath
                        ? new Document(MongoFieldNames.PATH, path + parentPathSegment)
                        : null;
            case PARENT:
                return concreteParentPath
                        ? new Document(MongoFieldNames.PATH,
                                PathUtils.getParentPath(path) + parentPathSegment)
                        : null;
            case DIRECT_CHILDREN:
                return new Document("$and", List.of(
                        new Document(MongoFieldNames.ANCESTORS, path),
                        new Document(MongoFieldNames.DEPTH,
                                PathUtils.getDepth(path) + planResult.getParentDepth() + 1)));
            case ALL_CHILDREN:
                return new Document(MongoFieldNames.ANCESTORS, path);
            case NO_RESTRICTION:
                return null;
            default:
                throw new IllegalArgumentException("Unknown path restriction " + restriction);
        }
    }

    private static MongotQueryTranslation translateProperty(String propertyName,
                                                            List<PropertyRestriction> restrictions,
                                                            boolean functionGuaranteesPresence,
                                                            PlanResult planResult) {
        if (QueryConstants.REP_EXCERPT.equals(propertyName)
                || QueryConstants.REP_FACET.equals(propertyName)) {
            return MongotQueryTranslation.supported(null, List.of());
        }
        if ("native*lucene".equals(propertyName)) {
            String query = restrictions.get(0).first.getValue(Type.STRING);
            if (!query.isBlank()) {
                return MongotQueryTranslation.supported(null, List.of());
            }
            return MongotQueryTranslation.unsupported("native query is empty");
        }
        if (isNodeNameFunction(propertyName)) {
            return MongotQueryTranslation.supported(null, List.of());
        }
        if ((propertyName.startsWith(":")
                && !QueryConstants.RESTRICTION_LOCAL_NAME.equals(propertyName)
                && !FieldNames.NODE_NAME.equals(propertyName))
                || UNSUPPORTED_RESULT_PROPERTIES.contains(propertyName)) {
            return MongotQueryTranslation.unsupported("unsupported virtual property restriction: " + propertyName);
        }
        String indexedProperty = QueryConstants.RESTRICTION_LOCAL_NAME.equals(propertyName)
                ? FieldNames.NODE_NAME
                : propertyName;
        String field = MongoFieldNames.TYPED + "." + MongoFieldNames.encodeProperty(indexedProperty);
        Document combinedRange = new Document();
        List<Document> clauses = new ArrayList<>();
        boolean hasNotRestriction = restrictions.stream().anyMatch(restriction -> restriction.isNot);
        for (PropertyRestriction restriction : restrictions) {
            if (restriction.isNullRestriction()) {
                clauses.add(new Document(MongoFieldNames.NULL_PROPERTIES,
                        MongoFieldNames.encodeProperty(propertyName)));
            } else if (restriction.isNotNullRestriction()) {
                if (!hasNotRestriction && !functionGuaranteesPresence) {
                    clauses.add(new Document(field, new Document("$exists", true)));
                }
            } else if (restriction.isLike) {
                clauses.add(new Document(field, new Document("$regex",
                        likeRegex(value(restriction.first)))));
            } else if (restriction.isNot) {
                Object excluded = value(restriction.not, restriction, planResult);
                clauses.add(new Document("$and", List.of(
                        new Document(field, new Document("$exists", true)),
                        new Document(field, new Document("$ne", excluded)))));
            } else if (restriction.list != null) {
                List<Object> values = new ArrayList<>();
                for (PropertyValue candidate : restriction.list) {
                    Object converted = value(candidate, restriction, planResult);
                    addIfAbsent(values, converted);
                    if (converted instanceof String) {
                        String string = (String) converted;
                        if ("true".equalsIgnoreCase(string) || "false".equalsIgnoreCase(string)) {
                            addIfAbsent(values, Boolean.valueOf(string));
                        }
                    }
                }
                clauses.add(new Document(field, new Document("$in", values)));
            } else if (restriction.first != null && restriction.first == restriction.last) {
                clauses.add(new Document(field,
                        value(restriction.first, restriction, planResult)));
            } else {
                if (restriction.first != null) {
                    combinedRange.append(restriction.firstIncluding ? "$gte" : "$gt",
                            value(restriction.first, restriction, planResult));
                }
                if (restriction.last != null) {
                    combinedRange.append(restriction.lastIncluding ? "$lte" : "$lt",
                            value(restriction.last, restriction, planResult));
                }
            }
        }
        if (!combinedRange.isEmpty()) {
            clauses.add(0, new Document(field, combinedRange));
        }
        return MongotQueryTranslation.supported(null, clauses);
    }

    private static boolean functionReferencesProperty(Iterable<String> restrictionNames,
                                                       String propertyName) {
        String propertyToken = "@" + propertyName;
        for (String restrictionName : restrictionNames) {
            if (restrictionName.startsWith("function*")
                    && List.of(restrictionName.split("\\*")).contains(propertyToken)) {
                return true;
            }
        }
        return false;
    }

    private static boolean isNodeNameFunction(String propertyName) {
        if (!propertyName.startsWith(QueryConstants.FUNCTION_RESTRICTION_PREFIX)) {
            return false;
        }
        return propertyName.endsWith("@" + QueryConstants.RESTRICTION_LOCAL_NAME)
                || propertyName.endsWith("@" + QueryConstants.RESTRICTION_NAME);
    }

    private static void addIfAbsent(List<Object> values, Object value) {
        if (!values.contains(value)) {
            values.add(value);
        }
    }

    private static Object value(PropertyValue value) {
        return value(value, value.getType().tag());
    }

    private static Object value(PropertyValue value, PropertyRestriction restriction,
                                PlanResult planResult) {
        PropertyDefinition property = planResult == null
                ? null
                : planResult.getPropDefn(restriction);
        int tag = property == null
                ? value.getType().tag()
                : FulltextIndex.determinePropertyType(property, restriction);
        return value(value, tag);
    }

    private static Object value(PropertyValue value, int tag) {
        if (tag == Type.LONG.tag()) {
            return value.getValue(Type.LONG);
        }
        if (tag == Type.DOUBLE.tag()) {
            return value.getValue(Type.DOUBLE);
        }
        if (tag == Type.BOOLEAN.tag()) {
            return value.getValue(Type.BOOLEAN);
        }
        if (tag == Type.DATE.tag()) {
            return new Date(DataConversionUtil.dateToLong(value.getValue(Type.DATE)));
        }
        return value.getValue(Type.STRING);
    }

    private static String likeRegex(Object patternValue) {
        String pattern = String.valueOf(patternValue);
        StringBuilder regex = new StringBuilder("^");
        boolean escaped = false;
        for (int i = 0; i < pattern.length(); i++) {
            char character = pattern.charAt(i);
            if (escaped) {
                regex.append(Pattern.quote(String.valueOf(character)));
                escaped = false;
            } else if (character == '\\') {
                escaped = true;
            } else if (character == '%') {
                regex.append(".*");
            } else if (character == '_') {
                regex.append('.');
            } else if (".[]{}()*+-?^$|".indexOf(character) >= 0) {
                regex.append('\\').append(character);
            } else {
                regex.append(character);
            }
        }
        if (escaped) {
            regex.append("\\\\");
        }
        return regex.append('$').toString();
    }
}
