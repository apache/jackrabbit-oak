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

package org.apache.jackrabbit.oak.plugins.index.property;

import static com.google.common.collect.Lists.newArrayList;

import java.util.List;

import org.apache.jackrabbit.oak.spi.query.QueryIndex;
import org.apache.jackrabbit.oak.spi.query.QueryIndexProvider;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.ConfigurationPolicy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Component(
        configurationPolicy = ConfigurationPolicy.REQUIRE,
        service = QueryIndexProvider.class)
public class OrderedPropertyIndexProvider implements QueryIndexProvider {
    private static final Logger LOG = LoggerFactory.getLogger(OrderedPropertyIndexProvider.class);
    private static int hits;
    private static int threshold = OrderedIndex.TRACK_DEPRECATION_EVERY;
    
    @Override
    public List<? extends QueryIndex> getQueryIndexes(NodeState nodeState) {
        if (getHits() % threshold == 0) {
            LOG.warn(OrderedIndex.DEPRECATION_MESSAGE);            
        }
        return newArrayList();
    }
    
    private synchronized int getHits() {
        return hits++;
    }
    
    /**
     * used only for testing purposes. Not thread safe.
     * 
     * @param t
     */
    static void setThreshold(int t) {
        threshold = t;
    }
}
