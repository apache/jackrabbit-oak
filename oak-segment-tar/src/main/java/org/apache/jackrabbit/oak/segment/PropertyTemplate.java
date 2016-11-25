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
package org.apache.jackrabbit.oak.segment;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.jackrabbit.oak.segment.CacheWeights.OBJECT_HEADER_SIZE;

import javax.annotation.Nonnull;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.StringUtils;

import com.google.common.collect.ComparisonChain;

/**
 * A property definition within a template (the property name, the type, and the
 * index within the list of properties for the given node).
 */
public class PropertyTemplate implements Comparable<PropertyTemplate> {

    /**
     * The index of this property within the list of properties in the node
     * template.
     */
    private final int index;

    private final String name;

    private final Type<?> type;

    PropertyTemplate(int index, String name, Type<?> type) {
        this.index = index;
        this.name = checkNotNull(name);
        this.type = checkNotNull(type);
    }

    PropertyTemplate(PropertyState state) {
        checkNotNull(state);
        this.index = 0;
        this.name = state.getName();
        this.type = state.getType();
    }

    public int getIndex() {
        return index;
    }

    public String getName() {
        return name;
    }

    public Type<?> getType() {
        return type;
    }

    //--------------------------------------------------------< Comparable >--

    @Override
    public int compareTo(@Nonnull PropertyTemplate template) {
        checkNotNull(template);
        return ComparisonChain.start()
                .compare(hashCode(), template.hashCode()) // important
                .compare(name, template.name)
                .compare(type, template.type)
                .result();
    }

    //------------------------------------------------------------< Object >--

    @Override
    public boolean equals(Object object) {
        if (this == object) {
            return true;
        } else if (object instanceof PropertyTemplate) {
            PropertyTemplate that = (PropertyTemplate) object;
            return name.equals(that.name) && type.equals(that.type); 
        } else {
            return false;
        }
    }

    @Override
    public int hashCode() {
        return name.hashCode();
    }

    @Override
    public String toString() {
        return name + "(" + type + ")";
    }

    public int estimateMemoryUsage() {
        return OBJECT_HEADER_SIZE + 16 + StringUtils.estimateMemoryUsage(name);
    }

}