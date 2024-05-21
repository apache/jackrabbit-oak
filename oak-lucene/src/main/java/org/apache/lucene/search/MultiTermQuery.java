/*
 * COPIED FROM APACHE LUCENE 4.7.2
 *
 * Git URL: git@github.com:apache/lucene.git, tag: releases/lucene-solr/4.7.2, path: lucene/core/src/java
 *
 * (see https://issues.apache.org/jira/browse/OAK-10786 for details)
 */

package org.apache.lucene.search;

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

import java.io.IOException;
import org.apache.lucene.index.FilteredTermsEnum;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.SingleTermsEnum;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermContext;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.util.AttributeSource;

/**
 * An abstract {@link Query} that matches documents containing a subset of terms provided by a
 * {@link FilteredTermsEnum} enumeration.
 *
 * <p>This query cannot be used directly; you must subclass
 * it and define {@link #getTermsEnum(Terms, AttributeSource)} to provide a
 * {@link FilteredTermsEnum} that iterates through the terms to be matched.
 *
 * <p><b>NOTE</b>: if {@link #setRewriteMethod} is either
 * {@link #CONSTANT_SCORE_BOOLEAN_QUERY_REWRITE} or {@link #SCORING_BOOLEAN_QUERY_REWRITE}, you may
 * encounter a {@link BooleanQuery.TooManyClauses} exception during searching, which happens when
 * the number of terms to be searched exceeds {@link BooleanQuery#getMaxClauseCount()}.  Setting
 * {@link #setRewriteMethod} to {@link #CONSTANT_SCORE_FILTER_REWRITE} prevents this.
 *
 * <p>The recommended rewrite method is {@link
 * #CONSTANT_SCORE_AUTO_REWRITE_DEFAULT}: it doesn't spend CPU computing unhelpful scores, and it
 * tries to pick the most performant rewrite method given the query. If you need scoring (like
 * {@link FuzzyQuery}, use {@link TopTermsScoringBooleanQueryRewrite} which uses a priority queue to
 * only collect competitive terms and not hit this limitation.
 * <p>
 * Note that org.apache.lucene.queryparser.classic.QueryParser produces MultiTermQueries using
 * {@link #CONSTANT_SCORE_AUTO_REWRITE_DEFAULT} by default.
 */
public abstract class MultiTermQuery extends Query {

    protected final String field;
    protected RewriteMethod rewriteMethod = CONSTANT_SCORE_AUTO_REWRITE_DEFAULT;

    /**
     * Abstract class that defines how the query is rewritten.
     */
    public static abstract class RewriteMethod {

        public abstract Query rewrite(IndexReader reader, MultiTermQuery query) throws IOException;

        /**
         * Returns the {@link MultiTermQuery}s {@link TermsEnum}
         *
         * @see MultiTermQuery#getTermsEnum(Terms, AttributeSource)
         */
        protected TermsEnum getTermsEnum(MultiTermQuery query, Terms terms, AttributeSource atts)
            throws IOException {
            return query.getTermsEnum(terms,
                atts); // allow RewriteMethod subclasses to pull a TermsEnum from the MTQ
        }
    }

    /**
     * A rewrite method that first creates a private Filter, by visiting each term in sequence and
     * marking all docs for that term.  Matching documents are assigned a constant score equal to
     * the query's boost.
     *
     * <p> This method is faster than the BooleanQuery
     * rewrite methods when the number of matched terms or matched documents is non-trivial. Also,
     * it will never hit an errant {@link BooleanQuery.TooManyClauses} exception.
     *
     * @see #setRewriteMethod
     */
    public static final RewriteMethod CONSTANT_SCORE_FILTER_REWRITE = new RewriteMethod() {
        @Override
        public Query rewrite(IndexReader reader, MultiTermQuery query) {
            Query result = new ConstantScoreQuery(
                new MultiTermQueryWrapperFilter<MultiTermQuery>(query));
            result.setBoost(query.getBoost());
            return result;
        }
    };

    /**
     * A rewrite method that first translates each term into {@link BooleanClause.Occur#SHOULD}
     * clause in a BooleanQuery, and keeps the scores as computed by the query.  Note that typically
     * such scores are meaningless to the user, and require non-trivial CPU to compute, so it's
     * almost always better to use {@link #CONSTANT_SCORE_AUTO_REWRITE_DEFAULT} instead.
     *
     * <p><b>NOTE</b>: This rewrite method will hit {@link
     * BooleanQuery.TooManyClauses} if the number of terms exceeds
     * {@link BooleanQuery#getMaxClauseCount}.
     *
     * @see #setRewriteMethod
     */
    public final static RewriteMethod SCORING_BOOLEAN_QUERY_REWRITE = ScoringRewrite.SCORING_BOOLEAN_QUERY_REWRITE;

    /**
     * Like {@link #SCORING_BOOLEAN_QUERY_REWRITE} except scores are not computed.  Instead, each
     * matching document receives a constant score equal to the query's boost.
     *
     * <p><b>NOTE</b>: This rewrite method will hit {@link
     * BooleanQuery.TooManyClauses} if the number of terms exceeds
     * {@link BooleanQuery#getMaxClauseCount}.
     *
     * @see #setRewriteMethod
     */
    public final static RewriteMethod CONSTANT_SCORE_BOOLEAN_QUERY_REWRITE = ScoringRewrite.CONSTANT_SCORE_BOOLEAN_QUERY_REWRITE;

    /**
     * A rewrite method that first translates each term into {@link BooleanClause.Occur#SHOULD}
     * clause in a BooleanQuery, and keeps the scores as computed by the query.
     *
     * <p>
     * This rewrite method only uses the top scoring terms so it will not overflow the boolean max
     * clause count. It is the default rewrite method for {@link FuzzyQuery}.
     *
     * @see #setRewriteMethod
     */
    public static final class TopTermsScoringBooleanQueryRewrite extends
        TopTermsRewrite<BooleanQuery> {

        /**
         * Create a TopTermsScoringBooleanQueryRewrite for at most <code>size</code> terms.
         * <p>
         * NOTE: if {@link BooleanQuery#getMaxClauseCount} is smaller than
         * <code>size</code>, then it will be used instead.
         */
        public TopTermsScoringBooleanQueryRewrite(int size) {
            super(size);
        }

        @Override
        protected int getMaxSize() {
            return BooleanQuery.getMaxClauseCount();
        }

        @Override
        protected BooleanQuery getTopLevelQuery() {
            return new BooleanQuery(true);
        }

        @Override
        protected void addClause(BooleanQuery topLevel, Term term, int docCount, float boost,
            TermContext states) {
            final TermQuery tq = new TermQuery(term, states);
            tq.setBoost(boost);
            topLevel.add(tq, BooleanClause.Occur.SHOULD);
        }
    }

    /**
     * A rewrite method that first translates each term into {@link BooleanClause.Occur#SHOULD}
     * clause in a BooleanQuery, but the scores are only computed as the boost.
     * <p>
     * This rewrite method only uses the top scoring terms so it will not overflow the boolean max
     * clause count.
     *
     * @see #setRewriteMethod
     */
    public static final class TopTermsBoostOnlyBooleanQueryRewrite extends
        TopTermsRewrite<BooleanQuery> {

        /**
         * Create a TopTermsBoostOnlyBooleanQueryRewrite for at most <code>size</code> terms.
         * <p>
         * NOTE: if {@link BooleanQuery#getMaxClauseCount} is smaller than
         * <code>size</code>, then it will be used instead.
         */
        public TopTermsBoostOnlyBooleanQueryRewrite(int size) {
            super(size);
        }

        @Override
        protected int getMaxSize() {
            return BooleanQuery.getMaxClauseCount();
        }

        @Override
        protected BooleanQuery getTopLevelQuery() {
            return new BooleanQuery(true);
        }

        @Override
        protected void addClause(BooleanQuery topLevel, Term term, int docFreq, float boost,
            TermContext states) {
            final Query q = new ConstantScoreQuery(new TermQuery(term, states));
            q.setBoost(boost);
            topLevel.add(q, BooleanClause.Occur.SHOULD);
        }
    }

    /**
     * A rewrite method that tries to pick the best constant-score rewrite method based on term and
     * document counts from the query.  If both the number of terms and documents is small enough,
     * then {@link #CONSTANT_SCORE_BOOLEAN_QUERY_REWRITE} is used. Otherwise,
     * {@link #CONSTANT_SCORE_FILTER_REWRITE} is used.
     */
    public static class ConstantScoreAutoRewrite extends
        org.apache.lucene.search.ConstantScoreAutoRewrite {

    }

    /**
     * Read-only default instance of {@link ConstantScoreAutoRewrite}, with
     * {@link ConstantScoreAutoRewrite#setTermCountCutoff} set to
     * {@link ConstantScoreAutoRewrite#DEFAULT_TERM_COUNT_CUTOFF} and
     * {@link ConstantScoreAutoRewrite#setDocCountPercent} set to
     * {@link ConstantScoreAutoRewrite#DEFAULT_DOC_COUNT_PERCENT}. Note that you cannot alter the
     * configuration of this instance; you'll need to create a private instance instead.
     */
    public final static RewriteMethod CONSTANT_SCORE_AUTO_REWRITE_DEFAULT = new ConstantScoreAutoRewrite() {
        @Override
        public void setTermCountCutoff(int count) {
            throw new UnsupportedOperationException("Please create a private instance");
        }

        @Override
        public void setDocCountPercent(double percent) {
            throw new UnsupportedOperationException("Please create a private instance");
        }
    };

    /**
     * Constructs a query matching terms that cannot be represented with a single Term.
     */
    public MultiTermQuery(final String field) {
        if (field == null) {
            throw new IllegalArgumentException("field must not be null");
        }
        this.field = field;
    }

    /**
     * Returns the field name for this query
     */
    public final String getField() {
        return field;
    }

    /**
     * Construct the enumeration to be used, expanding the pattern term.  This method should only be
     * called if the field exists (ie, implementations can assume the field does exist).  This
     * method should not return null (should instead return {@link TermsEnum#EMPTY} if no terms
     * match).  The TermsEnum must already be positioned to the first matching term. The given
     * {@link AttributeSource} is passed by the {@link RewriteMethod} to provide attributes, the
     * rewrite method uses to inform about e.g. maximum competitive boosts. This is currently only
     * used by {@link TopTermsRewrite}
     */
    protected abstract TermsEnum getTermsEnum(Terms terms, AttributeSource atts) throws IOException;

    /**
     * Convenience method, if no attributes are needed: This simply passes empty attributes and is
     * equal to:
     * <code>getTermsEnum(terms, new AttributeSource())</code>
     */
    protected final TermsEnum getTermsEnum(Terms terms) throws IOException {
        return getTermsEnum(terms, new AttributeSource());
    }

    /**
     * To rewrite to a simpler form, instead return a simpler enum from
     * {@link #getTermsEnum(Terms, AttributeSource)}.  For example, to rewrite to a single term,
     * return a {@link SingleTermsEnum}
     */
    @Override
    public final Query rewrite(IndexReader reader) throws IOException {
        return rewriteMethod.rewrite(reader, this);
    }

    /**
     * @see #setRewriteMethod
     */
    public RewriteMethod getRewriteMethod() {
        return rewriteMethod;
    }

    /**
     * Sets the rewrite method to be used when executing the query.  You can use one of the four
     * core methods, or implement your own subclass of {@link RewriteMethod}.
     */
    public void setRewriteMethod(RewriteMethod method) {
        rewriteMethod = method;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + Float.floatToIntBits(getBoost());
        result = prime * result + rewriteMethod.hashCode();
        if (field != null) {
            result = prime * result + field.hashCode();
        }
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        MultiTermQuery other = (MultiTermQuery) obj;
        if (Float.floatToIntBits(getBoost()) != Float.floatToIntBits(other.getBoost())) {
            return false;
        }
        if (!rewriteMethod.equals(other.rewriteMethod)) {
            return false;
        }
        return (other.field == null ? field == null : other.field.equals(field));
    }

}
