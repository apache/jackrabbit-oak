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
package org.apache.jackrabbit.oak.query.index;

import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.api.CoreValue;
import org.apache.jackrabbit.oak.query.ast.Operator;
import org.apache.jackrabbit.oak.query.ast.SelectorImpl;
import org.apache.jackrabbit.oak.spi.query.Filter;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;
import javax.jcr.PropertyType;

/**
 * A filter or lookup condition.
 */
public class FilterImpl implements Filter {

    /**
     * The selector this filter applies to.
     */
    private final SelectorImpl selector;

    /**
     * Whether the filter is always false.
     */
    private boolean alwaysFalse;

    /**
     *  The path, or "/" (the root node, meaning no filter) if not set.
     */
    private String path = "/";

    private PathRestriction pathRestriction = PathRestriction.ALL_CHILDREN;

    /**
     *  The node type, or null if not set.
     */
    private String nodeType;

    /**
     * The fulltext search conditions, if any.
     */
    private final ArrayList<String> fulltextConditions = new ArrayList<String>();

    private final HashMap<String, PropertyRestriction> propertyRestrictions =
            new HashMap<String, PropertyRestriction>();

    /**
     * Only return distinct values.
     */
    private boolean distinct;

    // TODO support "order by"

    public FilterImpl(SelectorImpl selector) {
        this.selector = selector;
    }

    /**
     * Get the path.
     *
     * @return the path
     */
    @Override
    public String getPath() {
        return path;
    }

    @Override
    public PathRestriction getPathRestriction() {
        return pathRestriction;
    }

    public void setPath(String path) {
        this.path = path;
    }

    public String getNodeType() {
        return nodeType;
    }

    public void setNodeType(String nodeType) {
        this.nodeType = nodeType;
    }

    public boolean isDistinct() {
        return distinct;
    }

    public void setDistinct(boolean distinct) {
        this.distinct = distinct;
    }

    public void setAlwaysFalse() {
        propertyRestrictions.clear();
        nodeType = "";
        path = "/";
        pathRestriction = PathRestriction.EXACT;
        alwaysFalse = true;
    }

    public boolean isAlwaysFalse() {
        return alwaysFalse;
    }

    public SelectorImpl getSelector() {
        return selector;
    }

    @Override
    public Collection<PropertyRestriction> getPropertyRestrictions() {
        return propertyRestrictions.values();
    }

    /**
     * Get the restriction for the given property, if any.
     *
     * @param propertyName the property name
     * @return the restriction or null
     */
    @Override
    public PropertyRestriction getPropertyRestriction(String propertyName) {
        return propertyRestrictions.get(propertyName);
    }

    public boolean testPath(String path) {
        if (isAlwaysFalse()) {
            return false;
        }
        switch (pathRestriction) {
        case EXACT:
            return path.matches(this.path);
        case PARENT:
            return PathUtils.isAncestor(path, this.path);
        case DIRECT_CHILDREN:
            return PathUtils.getParentPath(path).equals(this.path);
        case ALL_CHILDREN:
            return PathUtils.isAncestor(this.path, path);
        default:
            throw new IllegalArgumentException("Unknown path restriction: " + pathRestriction);
        }
    }

    public void restrictPropertyType(String propertyName, Operator operator,
            int propertyType) {
        if (propertyType == PropertyType.UNDEFINED) {
            // not restricted
            return;
        }
        PropertyRestriction x = propertyRestrictions.get(propertyName);
        if (x == null) {
            x = new PropertyRestriction();
            x.propertyName = propertyName;
            propertyRestrictions.put(propertyName, x);
        }
        if (x.propertyType != PropertyType.UNDEFINED && x.propertyType != propertyType) {
            // already restricted to another property type - always false
            setAlwaysFalse();
        }
        x.propertyType = propertyType;
    }

    public void restrictProperty(String propertyName, Operator op, CoreValue value) {
        PropertyRestriction x = propertyRestrictions.get(propertyName);
        if (x == null) {
            x = new PropertyRestriction();
            x.propertyName = propertyName;
            propertyRestrictions.put(propertyName, x);
        }
        CoreValue oldFirst = x.first, oldLast = x.last;
        switch (op) {
        case EQUAL:
            x.first = maxValue(oldFirst, value);
            x.firstIncluding = x.first == oldFirst ? x.firstIncluding : true;
            x.last = minValue(oldLast, value);
            x.lastIncluding = x.last == oldLast ? x.lastIncluding : true;
            break;
        case NOT_EQUAL:
            if (value != null) {
                throw new IllegalArgumentException("NOT_EQUAL only supported for NOT_EQUAL NULL");
            }
            break;
        case GREATER_THAN:
            x.first = maxValue(oldFirst, value);
            x.firstIncluding = false;
            break;
        case GREATER_OR_EQUAL:
            x.first = maxValue(oldFirst, value);
            x.firstIncluding = x.first == oldFirst ? x.firstIncluding : true;
            break;
        case LESS_THAN:
            x.last = minValue(oldLast, value);
            x.lastIncluding = false;
            break;
        case LESS_OR_EQUAL:
            x.last = minValue(oldLast, value);
            x.lastIncluding = x.last == oldLast ? x.lastIncluding : true;
            break;
        case LIKE:
            throw new IllegalArgumentException("LIKE is not supported");
        }
        if (x.first != null && x.last != null) {
            if (x.first.compareTo(x.last) > 0) {
                setAlwaysFalse();
            } else if (x.first.compareTo(x.last) == 0 && (!x.firstIncluding || !x.lastIncluding)) {
                setAlwaysFalse();
            }
        }
    }

    static CoreValue maxValue(CoreValue a, CoreValue b) {
        if (a == null) {
            return b;
        }
        return a.compareTo(b) < 0 ? b : a;
    }

    static CoreValue minValue(CoreValue a, CoreValue b) {
        if (a == null) {
            return b;
        }
        return a.compareTo(b) < 0 ? a : b;
    }

    @Override
    public String toString() {
        StringBuilder buff = new StringBuilder();
        if (alwaysFalse) {
            return "(always false)";
        }
        buff.append("path: ").append(path).append(pathRestriction).append('\n');
        for (Entry<String, PropertyRestriction> p : propertyRestrictions.entrySet()) {
            buff.append("property ").append(p.getKey()).append(": ").append(p.getValue()).append('\n');
        }
        return buff.toString();
    }

    public void restrictPath(String addedPath, PathRestriction addedPathRestriction) {
        if (addedPath == null) {
            // currently unknown (prepare time)
            addedPath = "/";
        }
        // calculating the intersection of path restrictions
        // this is ugly code, but I don't currently see a radically simpler method
        switch (addedPathRestriction) {
        case PARENT:
            switch (pathRestriction) {
            case PARENT:
                // ignore as it's fast anyway
                // (would need to loop to find a common ancestor)
                break;
            case EXACT:
            case ALL_CHILDREN:
            case DIRECT_CHILDREN:
                if (!PathUtils.isAncestor(path, addedPath)) {
                    setAlwaysFalse();
                }
                break;
            }
            pathRestriction = PathRestriction.PARENT;
            path = addedPath;
            break;
        case EXACT:
            switch (pathRestriction) {
            case PARENT:
                if (!PathUtils.isAncestor(addedPath, path)) {
                    setAlwaysFalse();
                }
                break;
            case EXACT:
                if (!addedPath.equals(path)) {
                    setAlwaysFalse();
                }
                break;
            case ALL_CHILDREN:
                if (!PathUtils.isAncestor(path, addedPath)) {
                    setAlwaysFalse();
                }
                break;
            case DIRECT_CHILDREN:
                if (!PathUtils.getParentPath(addedPath).equals(path)) {
                    setAlwaysFalse();
                }
                break;
            }
            path = addedPath;
            pathRestriction = PathRestriction.EXACT;
            break;
        case ALL_CHILDREN:
            switch (pathRestriction) {
            case PARENT:
            case EXACT:
                if (!PathUtils.isAncestor(addedPath, path)) {
                    setAlwaysFalse();
                }
                break;
            case ALL_CHILDREN:
                if (PathUtils.isAncestor(path, addedPath)) {
                    path = addedPath;
                } else if (!path.equals(addedPath) && !PathUtils.isAncestor(addedPath, path)) {
                    setAlwaysFalse();
                }
                break;
            case DIRECT_CHILDREN:
                if (!path.equals(addedPath) && !PathUtils.isAncestor(addedPath, path)) {
                    setAlwaysFalse();
                }
                break;
            }
            break;
        case DIRECT_CHILDREN:
            switch (pathRestriction) {
            case PARENT:
                if (!PathUtils.isAncestor(addedPath, path)) {
                    setAlwaysFalse();
                }
                break;
            case EXACT:
                if (!PathUtils.getParentPath(path).equals(addedPath)) {
                    setAlwaysFalse();
                }
                break;
            case ALL_CHILDREN:
                if (!path.equals(addedPath) && !PathUtils.isAncestor(path, addedPath)) {
                    setAlwaysFalse();
                } else {
                    path = addedPath;
                    pathRestriction = PathRestriction.DIRECT_CHILDREN;
                }
                break;
            case DIRECT_CHILDREN:
                if (!path.equals(addedPath)) {
                    setAlwaysFalse();
                }
                break;
            }
            break;
        }
    }

    @Override
    public List<String> getFulltextConditions() {
        // TODO support fulltext conditions on certain properties
        return fulltextConditions;
    }

    public void restrictFulltextCondition(String condition) {
        fulltextConditions.add(condition);
    }

}
