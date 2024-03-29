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
package org.apache.jackrabbit.api.jmx;

import javax.management.openmbean.TabularData;

import org.apache.jackrabbit.api.stats.QueryStat;
import org.osgi.annotation.versioning.ProviderType;

/**
 * JMX Bindings for {@link QueryStat}.
 * 
 */
@ProviderType
public interface QueryStatManagerMBean {

    String NAME = "org.apache.jackrabbit:type=QueryStats";

    /**
     * @return a sorted array containing the top
     *         {@link #getSlowQueriesQueueSize()} slowest queries
     */
    TabularData getSlowQueries();

    /**
     * @return a sorted array containing the
     *         {@link #getPopularQueriesQueueSize()} most popular queries
     */
    TabularData getPopularQueries();

    /**
     * @return size of the <b>Slow</b> queue
     */
    int getSlowQueriesQueueSize();

    /**
     * Change the size of the <b>Slow</b> queue
     * 
     * @param size
     *            the new size
     */
    void setSlowQueriesQueueSize(int size);

    /**
     * clears the <b>Slow</b> queue
     */
    void clearSlowQueriesQueue();

    /**
     * @return size of the <b>Popular</b> queue
     */
    int getPopularQueriesQueueSize();

    /**
     * Change the size of the <b>Popular</b> queue
     * 
     * @param size
     *            the new size
     */
    void setPopularQueriesQueueSize(int size);

    /**
     * clears the <b>Popular</b> queue
     */
    void clearPopularQueriesQueue();

}
