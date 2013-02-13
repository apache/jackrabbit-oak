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
package org.apache.jackrabbit.oak.plugins.segment;

import java.util.Map;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.spi.state.NodeState;

import com.google.common.base.Objects;
import com.google.common.collect.ImmutableMap;

class NodeTemplate {

    private final String ZERO_CHILD_NODES = null;

    private final String MANY_CHILD_NODES = "";

    private final Map<String, PropertyTemplate> properties;

    private final String nodeName;

    NodeTemplate(NodeState state) {
        ImmutableMap.Builder<String, PropertyTemplate> builder =
                ImmutableMap.builder();
        for (PropertyState property : state.getProperties()) {
            builder.put(property.getName(), new PropertyTemplate(property));
        }
        properties = builder.build();

        long count = state.getChildNodeCount();
        if (count == 0) {
            nodeName = ZERO_CHILD_NODES;
        } else if (count == 1) {
            nodeName = state.getChildNodeNames().iterator().next();
        } else {
            nodeName = MANY_CHILD_NODES;
        }
    }

    public int getPropertyCount() {
        return properties.size();
    }

    //------------------------------------------------------------< Object >--

    @Override
    public boolean equals(Object object) {
        if (this == object) {
            return true;
        } else if (object instanceof NodeTemplate) {
            NodeTemplate that = (NodeTemplate) object;
            return properties.equals(that.properties)
                    && Objects.equal(nodeName, that.nodeName);
        } else {
            return false;
        }
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(properties, nodeName);
    }

    @Override
    public String toString() {
        return properties.values() + " : " + nodeName;
    }

}
