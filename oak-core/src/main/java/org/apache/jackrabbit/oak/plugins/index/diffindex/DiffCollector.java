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
package org.apache.jackrabbit.oak.plugins.index.diffindex;

import java.util.Set;

import org.apache.jackrabbit.oak.spi.query.Filter;

/**
 * In charge of collecting the paths of nodes that match a given filter from the
 * diff of the 2 states.
 * 
 */
public interface DiffCollector {

    /**
     * Get the cost for the given filter, and prepare the result if the index
     * can be used.
     * 
     * @param filter the filter
     * @return the cost
     */
    double getCost(Filter filter);

    /**
     * Get the result for this filter.
     * 
     * @param filter the filter
     * @return the result
     */
    Set<String> getResults(Filter filter);

}
