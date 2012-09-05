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
package org.apache.jackrabbit.oak.plugins.unique;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.Map;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.CoreValue;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.plugins.memory.StringValue;
import org.apache.jackrabbit.oak.spi.commit.Validator;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStateUtils;

import com.google.common.base.Charsets;
import com.google.common.collect.Maps;

public class UniqueIndexValidator implements Validator {

    private final String name;

    private final NodeState index;

    private final String path;

    private final Map<String, String> insert;

    private final Map<String, String> remove;

    public UniqueIndexValidator(String name, NodeState index) {
        this.name = name;
        this.index = index;
        this.path = "";
        this.insert = Maps.newHashMap();
        this.remove = Maps.newHashMap();
    }

    private UniqueIndexValidator(UniqueIndexValidator parent, String name) {
        this.name = parent.name;
        this.index = parent.index;
        if (parent.path.isEmpty()) {
            this.path = name;
        } else {
            this.path = parent.path + "/" + name;
        }
        this.insert = parent.insert;
        this.remove = parent.remove;
    }

    public void apply(NodeBuilder unique) throws CommitFailedException {
        NodeBuilder builder = unique.getChildBuilder(name);

        for (Map.Entry<String, String> entry : remove.entrySet()) {
            String value = encode(entry.getKey());
            PropertyState property = builder.getProperty(value);
            if (property != null) {
                String path = entry.getValue();
                if (property.isArray()
                        || path.equals(property.getValue().getString())) {
                    builder.removeProperty(value);
                }
            }
        }

        for (Map.Entry<String, String> entry : insert.entrySet()) {
            String value = encode(entry.getKey());
            String path = entry.getValue();
            PropertyState property = builder.getProperty(value);
            if (property != null) {
                if (!property.isArray()
                        && !path.equals(property.getValue().getString())) {
                    throw new CommitFailedException(
                            "Uniqueness constraint violation: "
                            + name + " = " + entry.getKey());
                }
            }
            builder.setProperty(value, new StringValue(path));
        }
    }

    private String encode(String value) {
        try {
            return URLEncoder.encode(value, Charsets.UTF_8.name());
        } catch (UnsupportedEncodingException e) {
            return value;
        }
    }

    private void insert(Iterable<CoreValue> values)
            throws CommitFailedException {
        for (CoreValue value : values) {
            if (insert.put(value.getString(), path) != null) {
                throw new CommitFailedException(
                        "Uniqueness constraint violated: "
                        + name + " = " + value.getString());
            }
        }
    }

    private void remove(Iterable<CoreValue> values) {
        for (CoreValue value : values) {
            remove.put(value.getString(), path);
        }
    }

    @Override
    public void propertyAdded(PropertyState after)
            throws CommitFailedException {
        if (name.equals(after.getName())) {
            insert(after.getValues());
        }
    }

    @Override
    public void propertyChanged(PropertyState before, PropertyState after)
            throws CommitFailedException {
        if (name.equals(before.getName())) {
            remove(before.getValues());
            insert(after.getValues());
        }
    }

    @Override
    public void propertyDeleted(PropertyState before)
            throws CommitFailedException {
        if (name.equals(before.getName())) {
            remove(before.getValues());
        }
    }

    @Override
    public Validator childNodeAdded(String name, NodeState after) {
        return descend(name);
    }

    @Override
    public Validator childNodeChanged(
            String name, NodeState before, NodeState after) {
        return descend(name);
    }

    @Override
    public Validator childNodeDeleted(String name, NodeState before) {
        return descend(name);
    }

    private Validator descend(String name) {
        if (NodeStateUtils.isHidden(name)) {
            return null;
        } else {
            return new UniqueIndexValidator(this, name);
        }
    }

}