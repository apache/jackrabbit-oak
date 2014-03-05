/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.jackrabbit.oak.spi.query;

import java.util.ArrayList;
import java.util.List;

import javax.annotation.CheckForNull;

import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.index.aggregate.NodeAggregator;
import org.apache.jackrabbit.oak.spi.state.NodeState;

/**
 * Represents an index. The index should use the data in the filter if possible
 * to speed up reading.
 * <p>
 * The query engine will pick the index that returns the lowest cost for the
 * given filter conditions.
 * <p>
 * The index should only use that part of the filter that speeds up data lookup.
 * All other filter conditions should be ignored and not evaluated within this
 * index, because the query engine will in any case evaluate the condition (and
 * join condition), so that evaluating the conditions within the index would
 * actually slow down processing. For example, an index on the property
 * "lastName" should not try to evaluate any other restrictions than those on
 * the property "lastName", even if the query contains other restrictions. For
 * the query "where lastName = 'x' and firstName = 'y'", the query engine will
 * set two filter conditions, one for "lastName" and another for "firstName".
 * The index on "lastName" should not evaluate the condition on "firstName",
 * even thought it will be set in the filter.
 */
public interface QueryIndex {
    
    /**
     * Estimate the worst-case cost to query with the given filter. The returned
     * cost is a value between 1 (very fast; lookup of a unique node) and the
     * estimated number of entries to traverse, if the cursor would be fully
     * read, and if there could in theory be one network roundtrip or disk read
     * operation per node (this method may return a lower number if the data is
     * known to be fully in memory).
     * <p>
     * The returned value is supposed to be an estimate and doesn't have to be
     * very accurate. Please note this method is called on each index whenever a
     * query is run, so the method should be reasonably fast (not read any data
     * itself, or at least not read too much data).
     * <p>
     * If an index implementation can not query the data, it has to return
     * {@code Double.MAX_VALUE}.
     * 
     * @param filter the filter
     * @param rootState root state of the current repository snapshot
     * @return the estimated cost in number of read nodes
     */
    double getCost(Filter filter, NodeState rootState);

    /**
     * Query the index. The returned cursor is supposed to return as few nodes
     * as possible, but may return more nodes than necessary.
     * <p>
     * An implementation should only filter the result if it can do so easily
     * and efficiently; the query engine will verify the data again (in memory)
     * and check for access rights.
     * <p>
     * The method is only called if this index is used for the given query and
     * selector, which is only the case if the given index implementation
     * returned the lowest cost for the given filter. If the implementation
     * returned {@code Double.MAX_VALUE} in the getCost method for the given
     * filter, then this method is not called. If it is still called, then it is
     * supposed to throw an exception (as it would be an internal error of the
     * query engine).
     * 
     * @param filter the filter
     * @param rootState root state of the current repository snapshot
     * @return a cursor to iterate over the result
     */
    Cursor query(Filter filter, NodeState rootState);

    /**
     * Get the query plan for the given filter. This method is called when
     * running an {@code EXPLAIN SELECT} query, or for logging purposes. The
     * result should be human readable.
     * 
     * @param filter the filter
     * @param rootState root state of the current repository snapshot
     * @return the query plan
     */
    String getPlan(Filter filter, NodeState rootState);

    /**
     * Get the unique index name.
     *
     * @return the index name
     */
    String getIndexName();

    /**
     * A maker interface which means this index supports may support more than
     * just the minimal fulltext query syntax. If this index is used, then the
     * query engine does not verify the fulltext constraint(s) for the given
     * selector.
     */
    public interface FulltextQueryIndex extends QueryIndex {

        /**
         * Returns the NodeAggregator responsible for providing the aggregation
         * settings or null if aggregation is not available/desired.
         * 
         * @return the node aggregator or null
         */
        @CheckForNull
        NodeAggregator getNodeAggregator();

    }

    /**
     * An query index that may support using multiple access orders
     * (returning the rows in a specific order), and that can provide detailed
     * information about the cost.
     */
    public interface AdvancedQueryIndex {

        /**
         * Return the possible index plans for the given filter and sort order.
         * Please note this method is supposed to run quickly. That means it
         * should usually not read any data from the storage.
         * 
         * @param filter the filter
         * @param sortOrder the sort order or null if no sorting is required
         * @param rootState root state of the current repository snapshot
         * @return the list of index plans (null if none)
         */
        List<IndexPlan> getPlans(Filter filter, List<OrderEntry> sortOrder,
                NodeState rootState);

        /**
         * Get the query plan description (for logging purposes).
         * 
         * @param plan the index plan
         * @return the query plan description
         */
        String getPlanDescription(IndexPlan plan);

        /**
         * Start a query. The filter and sort order of the index plan is to be
         * used.
         * 
         * @param plan the index plan to use
         * @param rootState root state of the current repository snapshot
         * @return a cursor to iterate over the result
         */
        Cursor query(IndexPlan plan, NodeState rootState);

    }

    /**
     * An index plan.
     */
    public interface IndexPlan {

        /**
         * The cost to execute the query once. The returned value should
         * approximately match the number of disk read operations plus the
         * number of network roundtrips (worst case).
         * 
         * @return the cost per execution, in estimated number of I/O operations
         */
        double getCostPerExecution();

        /**
         * The cost to read one entry from the cursor. The returned value should
         * approximately match the number of disk read operations plus the
         * number of network roundtrips (worst case).
         * 
         * @return the lookup cost per entry, in estimated number of I/O operations
         */
        double getCostPerEntry();

        /**
         * The estimated number of entries. This value does not have to be
         * accurate.
         * 
         * @return the estimated number of entries
         */
        long getEstimatedEntryCount();

        /**
         * The filter to use.
         * 
         * @return the filter
         */
        Filter getFilter();

        /**
         * Whether the index is not always up-to-date.
         * 
         * @return whether the index might be updated asynchronously
         */
        boolean isDelayed();

        /**
         * Whether the fulltext part of the filter is evaluated (possibly with
         * an extended syntax). If set, the fulltext part of the filter is not
         * evaluated any more within the query engine.
         * 
         * @return whether the index supports full-text extraction
         */
        boolean isFulltextIndex();

        /**
         * Whether the cursor is able to read all properties from a node.
         * If yes, then the query engine will not have to read the data itself.
         * 
         * @return wheter node data is returned
         */
        boolean includesNodeData();

        /**
         * The sort order of the returned entries, or null if unsorted.
         * 
         * @return the sort order
         */
        List<OrderEntry> getSortOrder();
        
        /**
         * A builder for index plans.
         */
        public class Builder {

            protected double costPerExecution = 1.0;
            protected double costPerEntry = 1.0;
            protected long estimatedEntryCount = 1000000;
            protected Filter filter;
            protected boolean isDelayed;
            protected boolean isFulltextIndex;
            protected boolean includesNodeData;
            protected List<OrderEntry> sortOrder;

            public Builder setCostPerExecution(double costPerExecution) {
                this.costPerExecution = costPerExecution;
                return this;
            }

            public Builder setCostPerEntry(double costPerEntry) {
                this.costPerEntry = costPerEntry;
                return this;
            }

            public Builder setEstimatedEntryCount(long estimatedEntryCount) {
                this.estimatedEntryCount = estimatedEntryCount;
                return this;
            }

            public Builder setFilter(Filter filter) {
                this.filter = filter;
                return this;
            }

            public Builder setDelayed(boolean isDelayed) {
                this.isDelayed = isDelayed;
                return this;
            }

            public Builder setFulltextIndex(boolean isFulltextIndex) {
                this.isFulltextIndex = isFulltextIndex;
                return this;
            }

            public Builder setIncludesNodeData(boolean includesNodeData) {
                this.includesNodeData = includesNodeData;
                return this;
            }

            public Builder setSortOrder(List<OrderEntry> sortOrder) {
                this.sortOrder = sortOrder;
                return this;
            }
            
            public IndexPlan build() {
                
                return new IndexPlan() {
                    
                    private final double costPerExecution = 
                            Builder.this.costPerExecution;
                    private final double costPerEntry = 
                            Builder.this.costPerEntry;
                    private final long estimatedEntryCount = 
                            Builder.this.estimatedEntryCount;
                    private final Filter filter = 
                            Builder.this.filter;
                    private final boolean isDelayed = 
                            Builder.this.isDelayed;
                    private final boolean isFulltextIndex = 
                            Builder.this.isFulltextIndex;
                    private final boolean includesNodeData = 
                            Builder.this.includesNodeData;
                    private final List<OrderEntry> sortOrder = 
                            Builder.this.sortOrder == null ?
                            null : new ArrayList<OrderEntry>(
                                    Builder.this.sortOrder);                  

                    @Override
                    public double getCostPerExecution() {
                        return costPerExecution;
                    }

                    @Override
                    public double getCostPerEntry() {
                        return costPerEntry;
                    }

                    @Override
                    public long getEstimatedEntryCount() {
                        return estimatedEntryCount;
                    }

                    @Override
                    public Filter getFilter() {
                        return filter;
                    }

                    @Override
                    public boolean isDelayed() {
                        return isDelayed;
                    }

                    @Override
                    public boolean isFulltextIndex() {
                        return isFulltextIndex;
                    }

                    @Override
                    public boolean includesNodeData() {
                        return includesNodeData;
                    }

                    @Override
                    public List<OrderEntry> getSortOrder() {
                        return sortOrder;
                    }
                    
                };
            }
                
        }

    }

    /**
     * A sort order entry.
     */
    static class OrderEntry {

        /**
         * The property name on where to sort.
         */
        private final String propertyName;
        
        /**
         * The property type. Null if not known.
         */
        private final Type<?> propertyType;
        
        /**
         * The sort order (ascending or descending).
         */
        public enum Order { ASCENDING, DESCENDING };
        
        private final Order order;
        
        OrderEntry(String propertyName, Type<?> propertyType, Order order) {
            this.propertyName = propertyName;
            this.propertyType = propertyType;
            this.order = order;
        }

        public String getPropertyName() {
            return propertyName;
        }

        public Order getOrder() {
            return order;
        }

        public Type<?> getPropertyType() {
            return propertyType;
        }

    }

}
