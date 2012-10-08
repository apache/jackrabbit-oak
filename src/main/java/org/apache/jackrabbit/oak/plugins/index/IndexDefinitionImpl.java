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
package org.apache.jackrabbit.oak.plugins.index;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.spi.state.NodeState;

public class IndexDefinitionImpl implements IndexDefinition, IndexConstants {

    private final String name;
    private final String type;
    private final String path;
    private final NodeState state;

    public IndexDefinitionImpl(String name, String type, String path,
            NodeState state) {
        this.name = name;
        this.type = type;
        this.path = path;
        this.state = state;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public String getType() {
        return type;
    }

    @Override
    public String getPath() {
        return path;
    }

    @Override
    public boolean isReindex() {
        PropertyState ps = state.getProperty(REINDEX_PROPERTY_NAME);
        return ps != null && ps.getValue(Type.BOOLEAN);
    }

    @Override
    public NodeState getState() {
        return state;
    }

    @Override
    public String toString() {
        return "IndexDefinitionImpl [name=" + name + ", type=" + type
                + ", path=" + path + ", reindex=" + isReindex() + ", state="
                + state + "]";
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((state == null) ? 0 : state.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        IndexDefinitionImpl other = (IndexDefinitionImpl) obj;
        if (state == null) {
            if (other.state != null)
                return false;
        } else if (!state.equals(other.state))
            return false;
        return true;
    }

}
