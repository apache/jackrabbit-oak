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
package org.apache.jackrabbit.oak.util;

import java.util.Arrays;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.List;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import javax.jcr.RepositoryException;
import javax.jcr.Value;
import javax.jcr.ValueFactory;

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.namepath.NameMapper;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.plugins.memory.PropertyStates;
import org.apache.jackrabbit.oak.plugins.value.Conversions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.jackrabbit.oak.api.Type.BOOLEAN;
import static org.apache.jackrabbit.oak.api.Type.DATE;
import static org.apache.jackrabbit.oak.api.Type.LONG;
import static org.apache.jackrabbit.oak.api.Type.NAME;
import static org.apache.jackrabbit.oak.api.Type.NAMES;
import static org.apache.jackrabbit.oak.api.Type.STRING;
import static org.apache.jackrabbit.oak.api.Type.STRINGS;

/**
 * Utility class for accessing and writing typed content of a tree.
 */
public class NodeUtil {

    private static final Logger log = LoggerFactory.getLogger(NodeUtil.class);

    private final NameMapper mapper;

    private final Tree tree;

    public NodeUtil(Tree tree, NameMapper mapper) {
        this.mapper = checkNotNull(mapper);
        this.tree = checkNotNull(tree);
    }

    public NodeUtil(Tree tree) {
        this(tree, NamePathMapper.DEFAULT);
    }

    @Nonnull
    public Tree getTree() {
        return tree;
    }

    @Nonnull
    public String getName() {
        return mapper.getJcrName(tree.getName());
    }

    @CheckForNull
    public NodeUtil getParent() {
        return new NodeUtil(tree.getParent(), mapper);
    }

    public boolean isRoot() {
        return tree.isRoot();
    }

    public boolean hasChild(String name) {
        return tree.getChild(name) != null;
    }

    @CheckForNull
    public NodeUtil getChild(String name) {
        Tree child = tree.getChild(name);
        return (child == null) ? null : new NodeUtil(child, mapper);
    }

    @Nonnull
    public NodeUtil addChild(String name, String primaryNodeTypeName) {
        Tree child = tree.addChild(name);
        NodeUtil childUtil = new NodeUtil(child, mapper);
        childUtil.setName(JcrConstants.JCR_PRIMARYTYPE, primaryNodeTypeName);
        return childUtil;
    }

    @Nonnull
    public NodeUtil getOrAddChild(String name, String primaryTypeName) {
        NodeUtil child = getChild(name);
        return (child != null) ? child : addChild(name, primaryTypeName);
    }

    public boolean hasPrimaryNodeTypeName(String ntName) {
        return ntName.equals(getString(JcrConstants.JCR_PRIMARYTYPE, null));
    }

    public void removeProperty(String name) {
        tree.removeProperty(name);
    }

    public boolean getBoolean(String name) {
        PropertyState property = tree.getProperty(name);
        return property != null && !property.isArray()
                && property.getValue(BOOLEAN);
    }

    public void setBoolean(String name, boolean value) {
        tree.setProperty(name, value);
    }

    @CheckForNull
    public String getString(String name, String defaultValue) {
        PropertyState property = tree.getProperty(name);
        if (property != null && !property.isArray()) {
            return property.getValue(Type.STRING);
        } else {
            return defaultValue;
        }
    }

    public void setString(String name, String value) {
        tree.setProperty(name, value);
    }

    @CheckForNull
    public String[] getStrings(String name) {
        PropertyState property = tree.getProperty(name);
        if (property == null) {
            return null;
        }

        return Iterables.toArray(property.getValue(STRINGS), String.class);
    }

    public void setStrings(String name, String... values) {
        tree.setProperty(name, Arrays.asList(values), STRINGS);
    }

    @CheckForNull
    public String getName(String name) {
        return getName(name, null);
    }

    @CheckForNull
    public String getName(String name, String defaultValue) {
        PropertyState property = tree.getProperty(name);
        if (property != null && !property.isArray()) {
            return mapper.getJcrName(property.getValue(STRING));
        } else {
            return defaultValue;
        }
    }

    public void setName(String name, String value) {
        String oakName = getOakName(value);
        tree.setProperty(name, oakName, NAME);
    }

    @CheckForNull
    public String[] getNames(String name, String... defaultValues) {
        String[] strings = getStrings(name);
        if (strings == null) {
            strings = defaultValues;
        }
        for (int i = 0; i < strings.length; i++) {
            strings[i] = mapper.getJcrName(strings[i]);
        }
        return strings;
    }

    public void setNames(String name, String... values) {
        tree.setProperty(name, Arrays.asList(values), NAMES);
    }

    public void setDate(String name, long time) {
        Calendar cal = GregorianCalendar.getInstance();
        cal.setTimeInMillis(time);
        tree.setProperty(name, Conversions.convert(cal).toDate(), DATE);
    }

    public long getLong(String name, long defaultValue) {
        PropertyState property = tree.getProperty(name);
        if (property != null && !property.isArray()) {
            return property.getValue(LONG);
        } else {
            return defaultValue;
        }
    }

    public void setLong(String name, long value) {
        tree.setProperty(name, value);
    }

    @Nonnull
    public List<NodeUtil> getNodes(String namePrefix) {
        List<NodeUtil> nodes = Lists.newArrayList();
        for (Tree child : tree.getChildren()) {
            if (child.getName().startsWith(namePrefix)) {
                nodes.add(new NodeUtil(child, mapper));
            }
        }
        return nodes;
    }

    public void setValues(String name, Value[] values) {
        try {
            tree.setProperty(PropertyStates.createProperty(name, Arrays.asList(values)));
        }
        catch (RepositoryException e) {
            log.warn("Unable to convert a default value", e);
        }
    }

    public void setValues(String name, String[] values, int type) {
        tree.setProperty(name, Arrays.asList(values), STRINGS);
    }

    @CheckForNull
    public Value[] getValues(String name, ValueFactory vf) {
        PropertyState property = tree.getProperty(name);
        if (property != null) {
            int type = property.getType().tag();
            List<Value> values = Lists.newArrayList();
            for (String value : property.getValue(STRINGS)) {
                try {
                    values.add(vf.createValue(value, type));
                } catch (RepositoryException e) {
                    log.warn("Unable to convert a default value", e);
                }
            }
            return values.toArray(new Value[values.size()]);
        } else {
            return null;
        }
    }

    @Nonnull
    private String getOakName(String jcrName) {
        String oakName = mapper.getOakName(jcrName);
        if (oakName == null) {
            throw new IllegalArgumentException(new RepositoryException("Invalid name:" + jcrName));
        }
        return oakName;
    }

}
