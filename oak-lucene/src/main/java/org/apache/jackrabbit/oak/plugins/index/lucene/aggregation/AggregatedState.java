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
package org.apache.jackrabbit.oak.plugins.index.lucene.aggregation;

import static org.apache.jackrabbit.JcrConstants.JCR_PATH;
import static org.apache.jackrabbit.JcrConstants.JCR_UUID;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.spi.state.NodeState;

import com.google.common.collect.ImmutableSet;

public class AggregatedState {

    private final String name;

    private final NodeState state;

    private final Set<String> included;

    private static final Set<String> blacklist = ImmutableSet.of(JCR_PATH,
            JCR_UUID);

    public AggregatedState(String name, NodeState state, Set<String> included) {
        this.name = name;
        this.state = state;
        if (included == null) {
            this.included = ImmutableSet.of();
        } else {
            this.included = included;
        }
    }

    public String getName() {
        return name;
    }

    public NodeState getState() {
        return state;
    }

    private boolean include(String name) {
        if (included.contains(name)) {
            return true;
        }
        if (blacklist.contains(name)) {
            return false;
        }
        return included.isEmpty();
    }

    public Iterable<? extends PropertyState> getProperties() {
        if (included == null || included.isEmpty()) {
            return state.getProperties();
        }

        List<PropertyState> props = new ArrayList<PropertyState>();
        for (PropertyState property : state.getProperties()) {
            String pname = property.getName();
            if (include(pname)) {
                props.add(property);
            }
        }
        return props;
    }

    public NodeState get() {
        return state;
    }

}
