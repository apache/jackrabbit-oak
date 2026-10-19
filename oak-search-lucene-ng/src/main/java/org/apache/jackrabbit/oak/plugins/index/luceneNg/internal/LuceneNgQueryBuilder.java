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

import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.oak.api.PropertyValue;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.index.luceneNg.LuceneNgIndexConstants;
import org.apache.jackrabbit.oak.plugins.index.search.FieldNames;
import org.apache.jackrabbit.oak.plugins.index.search.IndexDefinition;
import org.apache.jackrabbit.oak.plugins.index.search.IndexDefinition.IndexingRule;
import org.apache.jackrabbit.oak.plugins.index.search.PropertyDefinition;
import org.apache.jackrabbit.oak.plugins.index.search.spi.query.FulltextIndexPlanner.PlanResult;
import org.apache.jackrabbit.oak.plugins.index.search.util.QueryUtils;
import org.apache.jackrabbit.oak.plugins.memory.PropertyValues;
import org.apache.jackrabbit.oak.spi.query.Filter;
import org.apache.jackrabbit.oak.spi.query.Filter.PropertyRestriction;
import org.apache.jackrabbit.oak.spi.query.QueryConstants;
import org.apache.jackrabbit.oak.spi.query.QueryIndex.OrderEntry;
import org.apache.jackrabbit.oak.spi.query.fulltext.FullTextAnd;
import org.apache.jackrabbit.oak.spi.query.fulltext.FullTextContains;
import org.apache.jackrabbit.oak.spi.query.fulltext.FullTextExpression;
import org.apache.jackrabbit.oak.spi.query.fulltext.FullTextOr;
import org.apache.jackrabbit.oak.spi.query.fulltext.FullTextTerm;
import org.apache.jackrabbit.oak.spi.query.fulltext.FullTextVisitor;
import org.apache.jackrabbit.util.ISO8601;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.document.DoublePoint;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.BoostQuery;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.PhraseQuery;
import org.apache.lucene.search.PrefixQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.SortedNumericSortField;
import org.apache.lucene.search.SortedSetSortField;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TermRangeQuery;
import org.apache.lucene.search.WildcardQuery;
import org.apache.lucene.util.BytesRef;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jcr.PropertyType;
import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Translates an Oak query plan into the Lucene query and sort executed against a LuceneNg index.
 */
public final class LuceneNgQueryBuilder {

    private static final Logger LOG = LoggerFactory.getLogger(LuceneNgQueryBuilder.class);

    private LuceneNgQueryBuilder() {
    }

    /**
     * The Lucene query for the filter of a plan. Property restrictions become clauses only when
     * the planner matched them to an indexed property definition; the query engine evaluates
     * the others.
     */
    @NotNull
    public static Query query(@NotNull Filter filter, @NotNull PlanResult planResult) {
        List<Query> propertyQueries = new ArrayList<>();
        for (PropertyRestriction pr : filter.getPropertyRestrictions()) {
            Query propertyQuery = restrictionQuery(pr, planResult);
            if (propertyQuery != null) {
                propertyQueries.add(propertyQuery);
            }
        }

        FullTextExpression ft = filter.getFullTextConstraint();
        Query contentQuery;
        if (ft != null) {
            Query fullTextQuery;
            try (Analyzer analyzer = new StandardAnalyzer()) {
                fullTextQuery = fullTextQuery(ft, analyzer);
            }
            if (propertyQueries.isEmpty()) {
                contentQuery = fullTextQuery;
            } else {
                BooleanQuery.Builder bq = new BooleanQuery.Builder();
                bq.add(fullTextQuery, Occur.MUST);
                propertyQueries.forEach(q -> bq.add(q, Occur.MUST));
                contentQuery = bq.build();
            }
        } else if (propertyQueries.isEmpty()) {
            contentQuery = new MatchAllDocsQuery();
        } else if (propertyQueries.size() == 1) {
            contentQuery = propertyQueries.get(0);
        } else {
            BooleanQuery.Builder bq = new BooleanQuery.Builder();
            propertyQueries.forEach(q -> bq.add(q, Occur.MUST));
            contentQuery = bq.build();
        }

        Query pathQuery = pathQuery(filter);
        Query typeQuery = nodeTypeQuery(filter, planResult);
        if (pathQuery == null && typeQuery == null) {
            return contentQuery;
        }
        BooleanQuery.Builder combined = new BooleanQuery.Builder();
        combined.add(contentQuery, Occur.MUST);
        if (pathQuery != null) {
            combined.add(pathQuery, Occur.FILTER);
        }
        if (typeQuery != null) {
            combined.add(typeQuery, Occur.FILTER);
        }
        return combined.build();
    }

    @Nullable
    private static Query restrictionQuery(PropertyRestriction pr, PlanResult planResult) {
        String name = pr.propertyName;
        if (QueryConstants.REP_FACET.equals(name) || name.startsWith(QueryConstants.FUNCTION_RESTRICTION_PREFIX)
                || name.startsWith("rep:") || name.startsWith("oak:")) {
            return null;
        }
        if (QueryConstants.RESTRICTION_LOCAL_NAME.equals(name)) {
            return planResult.evaluateNodeNameRestriction() ? localNameQuery(pr) : null;
        }
        PropertyDefinition definition = planResult.getPropDefn(pr);
        return definition != null ? propertyQuery(pr, definition) : null;
    }

    @Nullable
    private static Query nodeTypeQuery(Filter filter, PlanResult result) {
        if (filter.matchesAllTypes()) {
            return null;
        }
        BooleanQuery.Builder types = new BooleanQuery.Builder();
        PropertyDefinition primary = result.indexingRule.getConfig(JcrConstants.JCR_PRIMARYTYPE);
        if (primary != null && primary.index && primary.propertyIndex) {
            for (String type : filter.getPrimaryTypes()) {
                types.add(new TermQuery(new Term(JcrConstants.JCR_PRIMARYTYPE, type)), Occur.SHOULD);
            }
        }
        PropertyDefinition mixin = result.indexingRule.getConfig(JcrConstants.JCR_MIXINTYPES);
        if (mixin != null && mixin.index && mixin.propertyIndex) {
            for (String type : filter.getMixinTypes()) {
                types.add(new TermQuery(new Term(JcrConstants.JCR_MIXINTYPES, type)), Occur.SHOULD);
            }
        }
        BooleanQuery query = types.build();
        return query.clauses().isEmpty() ? null : query;
    }

    @Nullable
    private static Query pathQuery(Filter filter) {
        Filter.PathRestriction restriction = filter.getPathRestriction();
        if (restriction == null) {
            return null;
        }
        String path = filter.getPath();
        switch (restriction) {
            case ALL_CHILDREN:
                if ("/".equals(path)) {
                    return null;
                }
                return new PrefixQuery(new Term(FieldNames.PATH, path + "/"));
            case DIRECT_CHILDREN:
                return new TermQuery(new Term(LuceneNgIndexConstants.FIELD_PARENT_PATH, path));
            case EXACT:
                return new TermQuery(new Term(FieldNames.PATH, path));
            case PARENT:
                if ("/".equals(path)) {
                    // root has no parent: a path no document has
                    return new TermQuery(new Term(FieldNames.PATH, "\u0000"));
                }
                int lastSlash = path.lastIndexOf('/');
                String parentPath = lastSlash == 0 ? "/" : path.substring(0, lastSlash);
                return new TermQuery(new Term(FieldNames.PATH, parentPath));
            case NO_RESTRICTION:
            default:
                return null;
        }
    }

    /**
     * The Lucene query for a restriction on a property, according to its declared type.
     */
    @NotNull
    static Query propertyQuery(@NotNull PropertyRestriction pr, @NotNull PropertyDefinition definition) {
        String propertyName = pr.propertyName;
        int propertyType = definition.getType();

        if (pr.isNotNullRestriction()) {
            if (definition.notNullCheckEnabled) {
                return new TermQuery(new Term(FieldNames.NOT_NULL_PROPS, definition.name));
            }
            switch (propertyType) {
                case PropertyType.LONG:
                case PropertyType.DATE:
                    return LongPoint.newRangeQuery(propertyName, Long.MIN_VALUE, Long.MAX_VALUE);
                case PropertyType.DOUBLE:
                case PropertyType.DECIMAL:
                    return DoublePoint.newRangeQuery(propertyName, Double.NEGATIVE_INFINITY, Double.NaN);
                case PropertyType.UNDEFINED:
                    BooleanQuery.Builder existence = new BooleanQuery.Builder();
                    existence.add(new TermRangeQuery(propertyName, null, null, true, true), Occur.SHOULD);
                    existence.add(LongPoint.newRangeQuery(propertyName, Long.MIN_VALUE, Long.MAX_VALUE), Occur.SHOULD);
                    existence.add(DoublePoint.newRangeQuery(propertyName, Double.NEGATIVE_INFINITY, Double.NaN),
                            Occur.SHOULD);
                    return existence.build();
                default:
                    return new TermRangeQuery(propertyName, null, null, true, true);
            }
        }

        if (pr.isNullRestriction()) {
            if (definition.nullCheckEnabled) {
                return new TermQuery(new Term(FieldNames.NULL_PROPS, definition.name));
            }
            return new MatchAllDocsQuery();
        }

        switch (propertyType) {
            case PropertyType.LONG:
                return numericQuery(propertyName, pr, LONG_POINT);
            case PropertyType.DOUBLE:
            case PropertyType.DECIMAL:
                return numericQuery(propertyName, pr, DOUBLE_POINT);
            case PropertyType.DATE:
                return dateQuery(propertyName, pr);
            case PropertyType.BOOLEAN:
                return booleanQuery(propertyName, pr);
            default:
                return stringQuery(propertyName, pr);
        }
    }

    private interface NumericPoint<T extends Number> {
        T convert(PropertyValue pv);
        /** The smallest value greater than {@code val}, or {@code null} if there is none. */
        @Nullable T nextAbove(T val);
        /** The greatest value smaller than {@code val}, or {@code null} if there is none. */
        @Nullable T nextBelow(T val);
        T min();
        T max();
        Query exact(String field, T val);
        Query range(String field, T lo, T hi);
        Query set(String field, List<PropertyValue> list);
    }

    private static final NumericPoint<Long> LONG_POINT = new NumericPoint<>() {
        public Long convert(PropertyValue pv) { return pv.getValue(Type.LONG); }
        public Long nextAbove(Long v) { return v == Long.MAX_VALUE ? null : v + 1; }
        public Long nextBelow(Long v) { return v == Long.MIN_VALUE ? null : v - 1; }
        public Long min() { return Long.MIN_VALUE; }
        public Long max() { return Long.MAX_VALUE; }
        public Query exact(String f, Long v) { return LongPoint.newExactQuery(f, v); }
        public Query range(String f, Long lo, Long hi) { return LongPoint.newRangeQuery(f, lo, hi); }
        public Query set(String f, List<PropertyValue> list) {
            return LongPoint.newSetQuery(f, list.stream().mapToLong(pv -> pv.getValue(Type.LONG)).toArray());
        }
    };

    private static final NumericPoint<Double> DOUBLE_POINT = new NumericPoint<>() {
        public Double convert(PropertyValue pv) { return pv.getValue(Type.DOUBLE); }
        public Double nextAbove(Double v) { return Math.nextUp(v); }
        public Double nextBelow(Double v) { return Math.nextDown(v); }
        public Double min() { return -Double.MAX_VALUE; }
        public Double max() { return Double.MAX_VALUE; }
        public Query exact(String f, Double v) { return DoublePoint.newExactQuery(f, v); }
        public Query range(String f, Double lo, Double hi) { return DoublePoint.newRangeQuery(f, lo, hi); }
        public Query set(String f, List<PropertyValue> list) {
            return DoublePoint.newSetQuery(f, list.stream().mapToDouble(pv -> pv.getValue(Type.DOUBLE)).toArray());
        }
    };

    private static <T extends Number> Query numericQuery(String propertyName, PropertyRestriction pr,
                                                         NumericPoint<T> np) {
        T first = pr.first != null ? np.convert(pr.first) : null;
        T last = pr.last != null ? np.convert(pr.last) : null;

        if (first != null && pr.first.equals(pr.last) && pr.firstIncluding && pr.lastIncluding) {
            return np.exact(propertyName, first);
        } else if (first != null || last != null) {
            T lo = first == null ? np.min() : pr.firstIncluding ? first : np.nextAbove(first);
            T hi = last == null ? np.max() : pr.lastIncluding ? last : np.nextBelow(last);
            if (lo == null || hi == null) {
                return new MatchNoDocsQuery();
            }
            return np.range(propertyName, lo, hi);
        } else if (pr.list != null) {
            return np.set(propertyName, pr.list);
        } else if (pr.isNot && pr.not != null) {
            BooleanQuery.Builder bq = new BooleanQuery.Builder();
            bq.add(new MatchAllDocsQuery(), Occur.MUST);
            bq.add(np.exact(propertyName, np.convert(pr.not)), Occur.MUST_NOT);
            return bq.build();
        }
        throw new IllegalArgumentException("Unsupported property restriction: " + pr);
    }

    private static Query dateQuery(String propertyName, PropertyRestriction pr) {
        PropertyRestriction longPr = new PropertyRestriction();
        longPr.propertyName = propertyName;
        longPr.first = pr.first != null ? PropertyValues.newLong(dateToMillis(pr.first)) : null;
        longPr.last = pr.last != null ? PropertyValues.newLong(dateToMillis(pr.last)) : null;
        longPr.not = pr.not != null ? PropertyValues.newLong(dateToMillis(pr.not)) : null;
        longPr.firstIncluding = pr.firstIncluding;
        longPr.lastIncluding = pr.lastIncluding;
        longPr.isNot = pr.isNot;
        longPr.list = pr.list != null
                ? pr.list.stream().map(LuceneNgQueryBuilder::dateToMillis).map(PropertyValues::newLong)
                        .collect(Collectors.toList())
                : null;
        return numericQuery(propertyName, longPr, LONG_POINT);
    }

    private static long dateToMillis(PropertyValue pv) {
        try {
            Calendar date = ISO8601.parse(pv.getValue(Type.DATE));
            if (date == null) {
                throw new IllegalArgumentException("Invalid DATE query value");
            }
            return date.getTimeInMillis();
        } catch (IllegalArgumentException | IllegalStateException e) {
            // Conversion exceptions can contain the input; do not retain them as causes.
            LOG.error("Failed to parse DATE query value ({})", e.getClass().getSimpleName());
            throw new IllegalArgumentException("Invalid DATE query value");
        }
    }

    private static Query booleanQuery(String propertyName, PropertyRestriction pr) {
        if (pr.first != null && pr.first.equals(pr.last)) {
            return new TermQuery(new Term(propertyName, pr.first.getValue(Type.BOOLEAN).toString()));
        } else if (pr.isNot && pr.not != null) {
            BooleanQuery.Builder bq = new BooleanQuery.Builder();
            bq.add(new MatchAllDocsQuery(), Occur.MUST);
            bq.add(new TermQuery(new Term(propertyName, pr.not.getValue(Type.BOOLEAN).toString())), Occur.MUST_NOT);
            return bq.build();
        }
        throw new IllegalArgumentException("Unsupported boolean restriction: " + pr);
    }

    private static Query stringQuery(String propertyName, PropertyRestriction pr) {
        String first = pr.first != null ? pr.first.getValue(Type.STRING) : null;
        String last = pr.last != null ? pr.last.getValue(Type.STRING) : null;

        if (pr.isLike) {
            return new WildcardQuery(new Term(propertyName, QueryUtils.sqlLikeToLuceneWildcardQuery(first)));
        } else if (first != null && pr.first.equals(pr.last) && pr.firstIncluding && pr.lastIncluding) {
            return new TermQuery(new Term(propertyName, first));
        } else if (first != null || last != null) {
            return new TermRangeQuery(propertyName,
                    first != null ? new BytesRef(first) : null, last != null ? new BytesRef(last) : null,
                    first == null || pr.firstIncluding, last == null || pr.lastIncluding);
        } else if (pr.list != null) {
            BooleanQuery.Builder bq = new BooleanQuery.Builder();
            for (PropertyValue pv : pr.list) {
                bq.add(new TermQuery(new Term(propertyName, pv.getValue(Type.STRING))), Occur.SHOULD);
            }
            return bq.build();
        } else if (pr.isNot && pr.not != null) {
            BooleanQuery.Builder bq = new BooleanQuery.Builder();
            bq.add(new MatchAllDocsQuery(), Occur.MUST);
            bq.add(new TermQuery(new Term(propertyName, pr.not.getValue(Type.STRING))), Occur.MUST_NOT);
            return bq.build();
        }
        throw new IllegalArgumentException("Unsupported string restriction: " + pr);
    }

    @Nullable
    private static Query localNameQuery(PropertyRestriction pr) {
        if (pr.first != null && pr.first.equals(pr.last) && pr.firstIncluding && pr.lastIncluding) {
            return new TermQuery(new Term(FieldNames.NODE_NAME, pr.first.getValue(Type.STRING)));
        }
        if (pr.isLike && pr.first != null) {
            String like = pr.first.getValue(Type.STRING);
            return new WildcardQuery(new Term(FieldNames.NODE_NAME, like.replace("%", "*").replace("_", "?")));
        }
        return null;
    }

    private static Query fullTextQuery(FullTextExpression ft, Analyzer analyzer) {
        AtomicReference<Query> result = new AtomicReference<>();
        ft.accept(new FullTextVisitor() {

            @Override
            public boolean visit(FullTextContains contains) {
                return contains.getBase().accept(this);
            }

            @Override
            public boolean visit(FullTextOr or) {
                BooleanQuery.Builder bq = new BooleanQuery.Builder();
                for (FullTextExpression e : or.list) {
                    bq.add(fullTextQuery(e, analyzer), Occur.SHOULD);
                }
                result.set(bq.build());
                return true;
            }

            @Override
            public boolean visit(FullTextAnd and) {
                BooleanQuery.Builder bq = new BooleanQuery.Builder();
                for (FullTextExpression e : and.list) {
                    bq.add(fullTextQuery(e, analyzer), Occur.MUST);
                }
                result.set(bq.build());
                return true;
            }

            @Override
            public boolean visit(FullTextTerm term) {
                Query q = termQuery(term.getText(), term.getPropertyName(), analyzer);
                if (term.isNot()) {
                    BooleanQuery.Builder bq = new BooleanQuery.Builder();
                    bq.add(new MatchAllDocsQuery(), Occur.MUST);
                    bq.add(q, Occur.MUST_NOT);
                    q = bq.build();
                }
                String boost = term.getBoost();
                if (boost != null) {
                    try {
                        q = new BoostQuery(q, Float.parseFloat(boost));
                    } catch (NumberFormatException e) {
                        LOG.warn("Ignoring unparseable boost value '{}' on fulltext term", boost);
                    }
                }
                result.set(q);
                return true;
            }
        });
        return result.get();
    }

    private static Query termQuery(String text, String propertyName, Analyzer analyzer) {
        String field = propertyName == null || "*".equals(propertyName)
                ? FieldNames.FULLTEXT
                : FieldNames.createAnalyzedFieldName(propertyName);

        // Wildcard terms bypass tokenization to preserve the wildcard characters
        if (text.contains("*") || text.contains("?")) {
            String lower = text.toLowerCase(Locale.ENGLISH);
            if (lower.indexOf('*') == lower.length() - 1 && !lower.contains("?")) {
                return new PrefixQuery(new Term(field, lower.substring(0, lower.length() - 1)));
            }
            return new WildcardQuery(new Term(field, lower));
        }

        List<String> tokens = tokenize(text, analyzer);
        if (tokens.isEmpty()) {
            return new BooleanQuery.Builder().build();
        }
        if (tokens.size() == 1) {
            return new TermQuery(new Term(field, tokens.get(0)));
        }
        PhraseQuery.Builder pq = new PhraseQuery.Builder();
        for (String token : tokens) {
            pq.add(new Term(field, token));
        }
        return pq.build();
    }

    static List<String> tokenize(String text, Analyzer analyzer) {
        List<String> tokens = new ArrayList<>();
        try (TokenStream stream = analyzer.tokenStream(FieldNames.FULLTEXT, new StringReader(text))) {
            CharTermAttribute termAtt = stream.addAttribute(CharTermAttribute.class);
            stream.reset();
            while (stream.incrementToken()) {
                tokens.add(termAtt.toString());
            }
            stream.end();
        } catch (IOException e) {
            LOG.error("Failed to tokenize fulltext query ({})", e.getClass().getSimpleName());
            throw new IllegalStateException("Failed to tokenize fulltext query");
        }
        return tokens;
    }

    /**
     * The Lucene sort for the order entries of a plan.
     *
     * @param docValuesTypes doc-values type of each indexed field, {@link DocValuesType#NONE}
     *                       for fields without doc values
     */
    @NotNull
    public static Sort sort(@NotNull List<OrderEntry> sortOrder, @NotNull IndexDefinition definition,
                            @NotNull Function<String, DocValuesType> docValuesTypes) {
        List<SortField> fields = new ArrayList<>();
        for (OrderEntry order : sortOrder) {
            fields.add(sortField(order, definition, docValuesTypes));
        }
        return new Sort(fields.toArray(new SortField[0]));
    }

    /**
     * Ordered values are written as SORTED_SET (strings) or SORTED_NUMERIC (numbers) doc values
     * for single- and multi-valued properties alike; these sort each document by its smallest value.
     */
    static SortField sortField(OrderEntry order, IndexDefinition definition,
                               Function<String, DocValuesType> docValuesTypes) {
        String propertyName = order.getPropertyName();
        if (QueryConstants.JCR_SCORE.equals(propertyName)) {
            return SortField.FIELD_SCORE;
        }
        SortField.Type fieldType = sortFieldType(declaredType(definition, propertyName, order.getPropertyType().tag()));
        boolean reverse = order.getOrder() == OrderEntry.Order.DESCENDING;
        DocValuesType docValuesType = docValuesTypes.apply(propertyName);
        if (docValuesType == DocValuesType.SORTED_SET) {
            return new SortedSetSortField(propertyName, reverse);
        }
        if (docValuesType == DocValuesType.SORTED_NUMERIC) {
            return new SortedNumericSortField(propertyName, fieldType, reverse);
        }
        return new SortField(propertyName, fieldType, reverse);
    }

    private static int declaredType(IndexDefinition definition, String propertyName, int fallbackType) {
        for (IndexingRule rule : definition.getDefinedRules()) {
            PropertyDefinition propDef = rule.getConfig(propertyName);
            if (propDef != null && propDef.index) {
                return propDef.getType();
            }
        }
        return fallbackType;
    }

    private static SortField.Type sortFieldType(int propertyType) {
        switch (propertyType) {
            case PropertyType.LONG:
            case PropertyType.DATE:
                return SortField.Type.LONG;
            case PropertyType.DOUBLE:
            case PropertyType.DECIMAL:
                return SortField.Type.DOUBLE;
            default:
                return SortField.Type.STRING;
        }
    }
}
