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
package org.apache.jackrabbit.oak.plugins.observation.filter;

import java.util.List;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.spi.state.ChildNodeEntry;
import org.apache.jackrabbit.oak.spi.state.NodeState;

/**
 * An EventAggregator can be provided via a FilterProvider
 * and is then used to 'aggregate' an event at creation time
 * (ie after filtering).
 * <p>
 * Aggregation in this context means to have the event identifier
 * not be the path (as usual) but one of its parents.
 * This allows to have client code use an aggregating filter
 * and ignore the event paths but only inspect the event
 * identifier which is then the aggregation parent node.
 */
public interface EventAggregator {

    /**
     * Aggregates a property change
     * @return 0 or negative for no aggregation, positive indicating
     * how many levels to aggregate upwards the tree.
     */
    int aggregate(NodeState root, List<ChildNodeEntry> parents, PropertyState propertyState);
    
    /**
     * Aggregates a node change
     * @return 0 or negative for no aggregation, positive indicating
     * how many levels to aggregate upwards the tree.
     */
    int aggregate(NodeState root, List<ChildNodeEntry> parents, ChildNodeEntry childNodeState);
    
}
