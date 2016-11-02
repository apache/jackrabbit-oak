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
package org.apache.jackrabbit.oak.query;

import org.apache.jackrabbit.oak.api.jmx.QueryEngineSettingsMBean;
import org.apache.jackrabbit.oak.commons.jmx.AnnotatedStandardMBean;

/**
 * Settings of the query engine. This instance is an AnnotatedStandardMBean.
 */
public class QueryEngineSettingsMBeanImpl extends AnnotatedStandardMBean 
        implements QueryEngineSettingsMBean {
    
    private final QueryEngineSettings settings;
    
    /**
     * Create a new query engine settings object. Creating the object is
     * relatively slow, and at runtime, as few such objects as possible should
     * be created (ideally, only one per Oak instance). Creating new instances
     * also means they can not be configured using JMX, as one would expect.
     */
    public QueryEngineSettingsMBeanImpl(QueryEngineSettings settings) {
        super(QueryEngineSettingsMBean.class);
        this.settings = settings;
    }

    /**
     * Create a new query engine settings object. Creating the object is
     * relatively slow, and at runtime, as few such objects as possible should
     * be created (ideally, only one per Oak instance). Creating new instances
     * also means they can not be configured using JMX, as one would expect.
     */
    public QueryEngineSettingsMBeanImpl() {
        this(new QueryEngineSettings());
    }

    @Override
    public long getLimitInMemory() {
        return settings.getLimitInMemory();
    }
    
    @Override
    public void setLimitInMemory(long limitInMemory) {
        settings.setLimitInMemory(limitInMemory);
    }
    
    @Override
    public long getLimitReads() {
        return settings.getLimitReads();
    }
    
    @Override
    public void setLimitReads(long limitReads) {
        settings.setLimitReads(limitReads);
    }
    
    @Override
    public boolean getFailTraversal() {
        return settings.getFailTraversal();
    }

    @Override
    public void setFailTraversal(boolean failQueriesWithoutIndex) {
        settings.setFailTraversal(failQueriesWithoutIndex);
    }
    
    public void setFullTextComparisonWithoutIndex(boolean fullTextComparisonWithoutIndex) {
        settings.setFullTextComparisonWithoutIndex(fullTextComparisonWithoutIndex);
    }
    
    public boolean getFullTextComparisonWithoutIndex() {
        return settings.getFullTextComparisonWithoutIndex();
    }
    
    public boolean isSql2Optimisation() {
        return settings.isSql2Optimisation();
    }

    public QueryEngineSettings unwrap() {
        return settings;
    }

    @Override
    public String toString() {
        return settings.toString();
    }
}
