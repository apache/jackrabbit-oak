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
import javax.annotation.Nullable;
import javax.jcr.RepositoryException;
import javax.jcr.Value;
import javax.jcr.ValueFactory;

import com.google.common.base.Function;
import com.google.common.collect.Lists;
import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.TreeLocation;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.namepath.NameMapper;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.plugins.memory.PropertyStates;
import org.apache.jackrabbit.oak.plugins.value.Conversions;
import org.apache.jackrabbit.util.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkNotNull;
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
    public NameMapper getNameMapper() {
        return mapper;
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

    /**
     * TODO: clean up. workaround for OAK-426
     * <p/>
     * Create the tree at the specified relative path including all missing
     * intermediate trees using the specified {@code primaryTypeName}. This
     * method treats ".." parent element and "." as current element and
     * resolves them accordingly; in case of a relative path containing parent
     * elements this may lead to tree creating outside the tree structure
     * defined by this {@code NodeUtil}.
     *
     * @param relativePath    A relative OAK path that may contain parent and
     *                        current elements.
     * @param primaryTypeName A oak name of a primary node type that is used
     *                        to create the missing trees.
     * @return The node util of the tree at the specified {@code relativePath}.
     */
    @Nonnull
    public NodeUtil getOrAddTree(String relativePath, String primaryTypeName) {
        if (relativePath.indexOf('/') == -1) {
            return getOrAddChild(relativePath, primaryTypeName);
        } else {
            TreeLocation location = LocationUtil.getTreeLocation(tree.getLocation(), relativePath);
            if (location.getTree() == null) {
                NodeUtil target = this;
                for (String segment : Text.explode(relativePath, '/')) {
                    if (PathUtils.denotesParent(segment)) {
                        target = target.getParent();
                    } else if (target.hasChild(segment)) {
                        target = target.getChild(segment);
                    } else if (!PathUtils.denotesCurrent(segment)) {
                        target = target.addChild(segment, primaryTypeName);
                    }
                }
                return target;
            } else {
                return new NodeUtil(location.getTree());
            }
        }
    }

    public boolean hasPrimaryNodeTypeName(String ntName) {
        return ntName.equals(getPrimaryNodeTypeName());
    }

    @CheckForNull
    public String getPrimaryNodeTypeName() {
        return TreeUtil.getPrimaryTypeName(tree);
    }

    public void removeProperty(String name) {
        tree.removeProperty(name);
    }

    public boolean getBoolean(String name) {
        return TreeUtil.getBoolean(tree, name);
    }

    public void setBoolean(String name, boolean value) {
        tree.setProperty(name, value);
    }

    @CheckForNull
    public String getString(String name, @Nullable String defaultValue) {
        String str = TreeUtil.getString(tree, name);
        return (str != null) ? str : defaultValue;
    }

    public void setString(String name, String value) {
        tree.setProperty(name, value);
    }

    @CheckForNull
    public String[] getStrings(String name) {
        return TreeUtil.getStrings(tree, name);
    }

    public void setStrings(String name, String... values) {
        tree.setProperty(name, Arrays.asList(values), STRINGS);
    }

    @CheckForNull
    public String getName(String name) {
        return getName(name, null);
    }

    @CheckForNull
    public String getName(String name, @Nullable String defaultValue) {
        PropertyState property = tree.getProperty(name);
        if (property != null && !property.isArray()) {
            return mapper.getJcrName(property.getValue(STRING));
        } else {
            return defaultValue;
        }
    }

    public void setName(String propertyName, String value) {
        String oakName = getOakName(value);
        tree.setProperty(propertyName, oakName, NAME);
    }

    @CheckForNull
    public String[] getNames(String propertyName, String... defaultValues) {
        String[] strings = getStrings(propertyName);
        if (strings == null) {
            strings = defaultValues;
        }
        for (int i = 0; i < strings.length; i++) {
            strings[i] = mapper.getJcrName(strings[i]);
        }
        return strings;
    }

    public void setNames(String propertyName, String... values) {
        tree.setProperty(propertyName, Lists.transform(Arrays.asList(values), new Function<String, String>() {
            @Override
            public String apply(String jcrName) {
                return getOakName(jcrName);
            }
        }), NAMES);
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
        } catch (RepositoryException e) {
            log.warn("Unable to convert values", e);
        }
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
        String oakName = (jcrName == null) ? null : mapper.getOakNameOrNull(jcrName);
        if (oakName == null) {
            throw new IllegalArgumentException(new RepositoryException("Invalid name:" + jcrName));
        }
        return oakName;
    }
}
