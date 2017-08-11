/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.oak.query.stats;

import javax.management.openmbean.TabularData;

import org.apache.jackrabbit.oak.api.jmx.Description;

public interface QueryStatsMBean {

    String TYPE = "QueryStats";
    
    /**
     * Get the slow queries. Those are the ones that scan more than 100'000
     * nodes, or the configured maximum number of nodes to scan. (Raw execution
     * time is not taken into account, as execution can be slow if the code is
     * not compiled yet.)
     * 
     * @return the slow queries table
     */
    @Description("Get the slow queries (those that scan/traverse over many nodes).")
    TabularData getSlowQueries();
    
    @Description("Get the popular queries (those that take most of the time).")
    TabularData getPopularQueries();

    @Description("Get all data as Json.")
    String asJson();
   
    @Description("Reset the statistics (clear the list of queries).")
    void resetStats();

    /**
     * Whether to capture a thread dump in addition to the thread name.
     * No thread name / thread dump is captures for internal queries.
     * 
     * @param captureStackTraces the new valu
     */
    @Description("Enable / disable capturing the thread dumps (in addition to the thread name).")
    void setCaptureStackTraces(boolean captureStackTraces);
    
    boolean getCaptureStackTraces();
    
}
