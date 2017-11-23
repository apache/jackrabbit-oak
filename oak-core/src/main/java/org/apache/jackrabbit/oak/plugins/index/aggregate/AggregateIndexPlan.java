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
package org.apache.jackrabbit.oak.plugins.index.aggregate;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;

import javax.annotation.CheckForNull;

import org.apache.jackrabbit.oak.spi.query.Filter;
import org.apache.jackrabbit.oak.spi.query.Filter.PropertyRestriction;
import org.apache.jackrabbit.oak.spi.query.QueryIndex.IndexPlan;
import org.apache.jackrabbit.oak.spi.query.QueryIndex.OrderEntry;
import org.apache.jackrabbit.oak.spi.state.NodeState;

/**
 * An index plan for multiple query indexes.
 */
public class AggregateIndexPlan implements IndexPlan {
    
    private Filter filter;
    private boolean pathWithoutPlan;
    
    /**
     * The map of terms to plans.
     */
    private final HashMap<String, IndexPlan> basePlans = new HashMap<String, IndexPlan>();
    
    AggregateIndexPlan(Filter filter) {
        this.filter = filter;
    }
    
    void setPlan(String path, List<IndexPlan> plans) {
        if (plans.size() == 0) {
            // no index
            basePlans.put(path, null);
            pathWithoutPlan = true;
        } else {
            // we always pick the first plan
            basePlans.put(path, plans.get(0));
        }
    }
    
    boolean containsPathWithoutPlan() {
        return pathWithoutPlan;
    }
    
    IndexPlan getPlan(String path) {
        return basePlans.get(path);
    }
    
    Collection<IndexPlan> getPlans() {
        return basePlans.values();
    }

    @Override
    public double getCostPerExecution() {
        double cost = 0;
        for (IndexPlan p : basePlans.values()) {
            if (p != null) {
                cost += p.getCostPerExecution();
            }
        }
        return cost;
    }

    @Override
    public double getCostPerEntry() {
        // calculate the weigted average
        double costPerEntry = 0;
        long totalEntries = getEstimatedEntryCount();
        if (totalEntries == 0) {
            return 0;
        }
        for (IndexPlan p : basePlans.values()) {
            if (p != null) {
                costPerEntry += p.getCostPerEntry() * p.getEstimatedEntryCount() / totalEntries;
            }
        }
        return costPerEntry;
    }

    @Override
    public long getEstimatedEntryCount() {
        long totalEntries = 0;
        for (IndexPlan p : basePlans.values()) {
            if (p != null) {
                totalEntries += p.getEstimatedEntryCount();
            }
        }
        return totalEntries;
    }

    @Override
    public Filter getFilter() {
        return filter;
    }

    @Override
    public void setFilter(Filter filter) {
        this.filter = filter;
    }

    /**
     * Whether any base plan is delayed.
     * 
     * @return true if yes
     */
    @Override
    public boolean isDelayed() {
        for (IndexPlan p : basePlans.values()) {
            if (p != null && p.isDelayed()) {
                return true;
            }
        }
        return false;
    }

    /**
     * Whether any base plan is a full text index.
     * 
     * @return true if yes
     */
    @Override
    public boolean isFulltextIndex() {
        for (IndexPlan p : basePlans.values()) {
            if (p != null && p.isFulltextIndex()) {
                return true;
            }
        }
        return false;
    }

    /**
     * Whether all base plan include node data.
     * 
     * @return true if yes
     */
    @Override
    public boolean includesNodeData() {
        for (IndexPlan p : basePlans.values()) {
            if (p != null && !p.includesNodeData()) {
                return false;
            }
        }
        return true;   
    }

    /**
     * An aggregated query can not sort, as it gets results from a number of
     * indexes.
     * 
     * @return null
     */
    @Override
    public List<OrderEntry> getSortOrder() {
        return null;
    }

    // the following methods probably shouldn't be in the IndexPlan interface
    // as they are only used locally (in the ordered index, or in the lucene index)
    
    @Override
    @CheckForNull
    public PropertyRestriction getPropertyRestriction() {
        return null;
    }

    @Override
    public IndexPlan copy() {
        return null;
    }
    
    @Override
    public NodeState getDefinition() {
        return null;
    }

    @Override
    public String getPathPrefix() {
        return null;
    }

    @Override
    public boolean getSupportsPathRestriction() {
        return false;
    }

    @Override
    @CheckForNull
    public Object getAttribute(String name) {
        return null;
    }

    @Override
    public String getPlanName() {
        StringBuilder name = new StringBuilder();
        boolean first = true;
        for (IndexPlan p : basePlans.values()) {
            if (!first) {
                name.append(",");
            } else {
                first = false;
            }
            name.append(p.getPlanName());
        }
        return name.toString();
    }

}
