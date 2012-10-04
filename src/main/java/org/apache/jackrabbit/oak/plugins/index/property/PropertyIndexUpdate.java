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

import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.jcr.PropertyType;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.CoreValue;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.plugins.memory.StringValue;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

class PropertyIndexUpdate {

    private final String path;

    private final NodeBuilder node;

    private final Map<String, Set<String>> insert;

    private final Map<String, Set<String>> remove;

    public PropertyIndexUpdate(String path, NodeBuilder node) {
        this.path = path;
        this.node = node;
        this.insert = Maps.newHashMap();
        this.remove = Maps.newHashMap();
    }

    public void insert(String path, Iterable<CoreValue> values) {
        Preconditions.checkArgument(path.startsWith(this.path));
        putValues(insert, path.substring(this.path.length()), values);
    }

    public void remove(String path, Iterable<CoreValue> values) {
        Preconditions.checkArgument(path.startsWith(this.path));
        putValues(remove, path.substring(this.path.length()), values);
    }

    private void putValues(
            Map<String, Set<String>> map,
            String path, Iterable<CoreValue> values) {
        for (CoreValue value : values) {
            if (value.getType() != PropertyType.BINARY) {
                String key = PropertyIndex.encode(value);
                Set<String> paths = map.get(key);
                if (paths == null) {
                    paths = Sets.newHashSet();
                    map.put(key, paths);
                }
                paths.add(path);
            }
        }
    }


    public void apply() throws CommitFailedException {
        boolean unique = node.getProperty("unique") != null;
        NodeBuilder index = node.getChildBuilder(":index");

        for (Map.Entry<String, Set<String>> entry : remove.entrySet()) {
            String encoded = entry.getKey();
            Set<String> paths = entry.getValue();
            PropertyState property = index.getProperty(encoded);
            if (property != null) {
                List<CoreValue> values = Lists.newArrayList();
                for (CoreValue value : property.getValues()) {
                    if (!paths.contains(value.getString())) {
                        values.add(value);
                    }
                }
                if (values.isEmpty()) {
                    index.removeProperty(encoded);
                } else {
                    index.setProperty(encoded, values);
                }
            }
        }

        for (Map.Entry<String, Set<String>> entry : insert.entrySet()) {
            String encoded = entry.getKey();
            Set<String> paths = entry.getValue();
            List<CoreValue> values = Lists.newArrayList();
            PropertyState property = index.getProperty(encoded);
            if (property != null) {
                for (CoreValue value : property.getValues()) {
                    values.add(value);
                    paths.remove(value.getString());
                }
            }
            for (String path : paths) {
                values.add(new StringValue(path));
            }
            if (values.isEmpty()) {
                index.removeProperty(encoded);
            } else if (unique && values.size() > 1) {
                throw new CommitFailedException(
                        "Uniqueness constraint violated");
            } else {
                index.setProperty(encoded, values);
            }
        }
    }

}