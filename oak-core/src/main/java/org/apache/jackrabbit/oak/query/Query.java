/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0 Unless required by applicable law
 * or agreed to in writing, software distributed under the License is
 * distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language
 * governing permissions and limitations under the License.
 */
package org.apache.jackrabbit.oak.query;

import java.util.Iterator;
import java.util.List;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.osgi.annotation.versioning.ProviderType;

import org.apache.jackrabbit.oak.api.PropertyValue;
import org.apache.jackrabbit.oak.api.Result;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.query.ast.ColumnImpl;
import org.apache.jackrabbit.oak.query.ast.OrderingImpl;
import org.apache.jackrabbit.oak.query.stats.QueryStatsData.QueryExecutionStats;

/**
 * A "select" or "union" query.
 * <p>
 * Lifecycle: use the constructor to create a new object. Call init() to
 * initialize the bind variable map. If the query is re-executed, a new instance
 * is created.
 */
@ProviderType
public interface Query {

    void setExecutionContext(ExecutionContext context);

    void setLimit(long limit);

    void setOffset(long offset);

    void bindValue(String key, PropertyValue value);

    void setTraversalEnabled(boolean traversalEnabled);

    Result executeQuery();

    List<String> getBindVariableNames();

    ColumnImpl[] getColumns();

    int getColumnIndex(String columnName);

    String[] getSelectorNames();

    int getSelectorIndex(String selectorName);

    Iterator<ResultRowImpl> getRows();

    /**
     * Get the size if known.
     * 
     * @return the size, or -1 if unknown
     */
    long getSize();
    
    /**
     * Get the size if known.
     * 
     * @param precision the required precision
     * @param max the maximum nodes read (for an exact size)
     * @return the size, or -1 if unknown
     */
    long getSize(Result.SizePrecision precision, long max);

    void setExplain(boolean explain);

    void setMeasure(boolean measure);

    void setOrderings(OrderingImpl[] orderings);
    
    /**
     * Initialize the query. This will 'wire' selectors into constraints, and
     * collect bind variable names. It will also simplify expressions if
     * possible, but will not prepare the query.
     */
    void init();
    
    /**
     * Prepare the query. The cost is estimated and the execution plan is
     * decided here.
     */
    void prepare();

    /**
     * Get the query plan. The query must already be prepared.
     * 
     * @return the query plan
     */
    String getPlan();

    /**
     * Get the index cost as a JSON string. The query must already be prepared.
     * 
     * @return the index cost
     */
    String getIndexCostInfo();

    /**
     * Get the estimated cost.
     * 
     * @return the estimated cost
     */
    double getEstimatedCost();

    Tree getTree(String path);

    boolean isMeasureOrExplainEnabled();

    void setInternal(boolean internal);

    /**
     * Returns whether the results will be sorted by index. The query must already be prepared.
     *
     * @return if sorted by index
     */
    boolean isSortedByIndex();
    
    /**
     * Try to convert the query to an alternative form, specially a "union". To
     * avoid any potential error due to state variables perform the conversion
     * before the {@link #init()}.
     * 
     * @return {@code this} if no conversions are possible or a new instance of
     *         a {@link Query}. Cannot return null.
     */
    @Nonnull
    Query buildAlternativeQuery();
    
    /**
     * <p>
     * returns a clone of the current object. Will throw an exception in case it's invoked in a non
     * appropriate moment. For example the default {@link QueryImpl} cannot be cloned once the
     * {@link #init()} has been executed.
     * </p>
     * 
     * <p>
     * <strong>May return null if not implemented.</strong>
     * </p>
     * @return a clone of self
     * @throws IllegalStateException
     */
    @Nullable
    Query copyOf() throws IllegalStateException;
    
    /**
     * @return {@code true} if the query has been already initialised. {@code false} otherwise.
     */
    boolean isInit();
    
    /**
     * @return the original statement as it was used to construct the object. If not provided the
     *         toString() will be used instead.
     */
    String getStatement();
    
    /**
     * 
     * @return {@code true} if the current query is internal. {@code false} otherwise.
     */
    boolean isInternal();

    /**
     * Whether the condition contains a fulltext condition that can not be 
     * applied to the filter, for example because it is part of an "or" condition
     * of the form "where a=1 or contains(., 'x')".
     * 
     * @return true if yes
     */    
    boolean containsUnfilteredFullTextCondition();

    /**
     * Set the query option to be used for this query.
     * 
     * @param options the options
     */
    void setQueryOptions(QueryOptions options);

    /**
     * Whether the query is potentially slow.
     * Only supported for prepared queries.
     * 
     * @return true if traversal is the only option
     */
    boolean isPotentiallySlow();

    /**
     * Verify the query is not potentially slow. Only supported for prepared
     * queries.
     * 
     * @throws IllegalArgumentException if potentially slow, and configured to
     *             fail in this case
     */
    void verifyNotPotentiallySlow();
    
    QueryExecutionStats getQueryExecutionStats();

}
