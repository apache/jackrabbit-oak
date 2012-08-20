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

import java.util.ArrayList;
import java.util.List;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import javax.jcr.PropertyType;
import javax.jcr.RepositoryException;
import javax.jcr.Value;
import javax.jcr.ValueFactory;

import com.google.common.collect.Lists;
import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.oak.api.ContentSession;
import org.apache.jackrabbit.oak.api.CoreValue;
import org.apache.jackrabbit.oak.api.CoreValueFactory;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.namepath.NameMapper;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility class for accessing and writing typed content of a tree.
 */
public class NodeUtil {

    private static final Logger log = LoggerFactory.getLogger(NodeUtil.class);

    private final CoreValueFactory factory;

    private final NameMapper mapper;

    private final Tree tree;

    public NodeUtil(Tree tree, CoreValueFactory factory, NameMapper mapper) {
        this.factory = factory;
        this.mapper = mapper;
        this.tree = tree;
    }

    public NodeUtil(Tree tree, CoreValueFactory factory) {
        this.tree = tree;
        this.factory = factory;
        this.mapper = NamePathMapper.DEFAULT;
    }

    public NodeUtil(Tree tree, ContentSession contentSession) {
        this(tree, contentSession.getCoreValueFactory());
    }

    @Nonnull
    public Tree getTree() {
        return tree;
    }

    @Nonnull
    public String getName() {
        return mapper.getJcrName(tree.getName());
    }

    public NodeUtil getParent() {
        return new NodeUtil(tree.getParent(), factory, mapper);
    }

    public boolean hasChild(String name) {
        return tree.getChild(name) != null;
    }

    @CheckForNull
    public NodeUtil getChild(String name) {
        Tree child = tree.getChild(name);
        return (child == null) ? null : new NodeUtil(child, factory, mapper);
    }

    @Nonnull
    public NodeUtil addChild(String name, String primaryNodeTypeName) {
        Tree child = tree.addChild(name);
        NodeUtil childUtil = new NodeUtil(child, factory, mapper);
        childUtil.setName(JcrConstants.JCR_PRIMARYTYPE, primaryNodeTypeName);
        return childUtil;
    }

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
                && property.getValue().getBoolean();
    }

    public void setBoolean(String name, boolean value) {
        tree.setProperty(name, factory.createValue(value));
    }

    public String getString(String name, String defaultValue) {
        PropertyState property = tree.getProperty(name);
        if (property != null && !property.isArray()) {
            return property.getValue().getString();
        } else {
            return defaultValue;
        }
    }

    public void setString(String name, String value) {
        tree.setProperty(name, factory.createValue(value));
    }

    public String[] getStrings(String name) {
        PropertyState property = tree.getProperty(name);
        if (property == null) {
            return null;
        }

        List<CoreValue> values = property.getValues();
        String[] strings = new String[values.size()];
        for (int i = 0; i < strings.length; i++) {
            strings[i] = values.get(i).getString();
        }
        return strings;
    }

    public void setStrings(String name, String... values) {
        List<CoreValue> cvs = new ArrayList<CoreValue>(values.length);
        for (String value : values) {
            cvs.add(factory.createValue(value));
        }
        tree.setProperty(name, cvs);
    }

    public String getName(String name) {
        return getName(name, null);
    }

    public String getName(String name, String defaultValue) {
        PropertyState property = tree.getProperty(name);
        if (property != null && !property.isArray()) {
            return mapper.getJcrName(property.getValue().getString());
        } else {
            return defaultValue;
        }
    }

    public void setName(String name, String value) {
        String oakName = mapper.getOakName(value);
        if (oakName == null) {
            throw new IllegalArgumentException("Invalid name:" + name);
        }

        tree.setProperty(name, factory.createValue(oakName, PropertyType.NAME));
    }

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
        List<CoreValue> cvs = new ArrayList<CoreValue>(values.length);
        for (String value : values) {
            String oakName = mapper.getOakName(value);
            if (oakName == null) {
                throw new IllegalArgumentException(new RepositoryException("Invalid name:" + name));
            }

            cvs.add(factory.createValue(oakName, PropertyType.NAME));
        }
        tree.setProperty(name, cvs);
    }

    public List<NodeUtil> getNodes(String namePrefix) {
        List<NodeUtil> nodes = Lists.newArrayList();
        for (Tree child : tree.getChildren()) {
            if (child.getName().startsWith(namePrefix)) {
                nodes.add(new NodeUtil(child, factory, mapper));
            }
        }
        return nodes;
    }

    public void setValues(String name, Value[] values) {
        List<CoreValue> cvs = Lists.newArrayList();
        for (Value value : values) {
            try {
                cvs.add(factory.createValue(value.getString(), value.getType()));
            } catch (RepositoryException e) {
                log.warn("Unable to convert a default value", e);
            }
        }
        tree.setProperty(name, cvs);
    }

    public Value[] getValues(String name, ValueFactory vf) {
        PropertyState property = tree.getProperty(name);
        if (property != null) {
            List<Value> values = Lists.newArrayList();
            for (CoreValue value : property.getValues()) {
                try {
                    values.add(vf.createValue(value.getString(), value.getType()));
                } catch (RepositoryException e) {
                    log.warn("Unable to convert a default value", e);
                }
            }
            return values.toArray(new Value[values.size()]);
        } else {
            return null;
        }
    }

}
