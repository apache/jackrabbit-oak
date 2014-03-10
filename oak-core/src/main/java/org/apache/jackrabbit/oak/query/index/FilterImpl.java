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

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.jcr.PropertyType;

import org.apache.jackrabbit.oak.api.PropertyValue;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.query.ast.JoinConditionImpl;
import org.apache.jackrabbit.oak.query.ast.NativeFunctionImpl;
import org.apache.jackrabbit.oak.query.ast.Operator;
import org.apache.jackrabbit.oak.query.ast.SelectorImpl;
import org.apache.jackrabbit.oak.query.fulltext.FullTextExpression;
import org.apache.jackrabbit.oak.spi.query.Filter;

/**
 * A filter or lookup condition.
 */
public class FilterImpl implements Filter {

    /**
     * The selector this filter applies to.
     */
    private final SelectorImpl selector;
    
    private final String queryStatement;

    /**
     * Whether the filter is always false.
     */
    private boolean alwaysFalse;

    /**
     * inherited from the selector, duplicated here so it can be over-written by
     * other filters
     */
    private boolean matchesAllTypes;

    /**
     *  The path, or "/" (the root node, meaning no filter) if not set.
     */
    private String path = "/";
    
    private PathRestriction pathRestriction = PathRestriction.NO_RESTRICTION;

    /**
     * Additional path restrictions whose values are known only at runtime,
     * for example paths set by other (join-) selectors.
     */
    private String pathPlan;

    /**
     * The fulltext search conditions, if any.
     */
    private final ArrayList<String> fulltextConditions = new ArrayList<String>();
    
    private FullTextExpression fullTextConstraint;

    private final HashMap<String, PropertyRestriction> propertyRestrictions =
            new HashMap<String, PropertyRestriction>();

    /**
     * Only return distinct values.
     */
    private boolean distinct;
    
    /**
     * Set during the prepare phase of a query.
     */
    private boolean preparing;

    // TODO support "order by"
    
    public FilterImpl() {
        this(null, null);
    }

    /**
     * Create a filter.
     * 
     * @param selector the selector for the given filter
     * @param queryStatement the query statement
     */
    public FilterImpl(SelectorImpl selector, String queryStatement) {
        this.selector = selector;
        this.queryStatement = queryStatement;
        this.matchesAllTypes = selector != null ? selector.matchesAllTypes()
                : false;
    }

    public FilterImpl(Filter filter) {
        FilterImpl impl = (FilterImpl) filter;
        this.alwaysFalse = impl.alwaysFalse;
        this.distinct = impl.distinct;
        this.fullTextConstraint = impl.fullTextConstraint;
        this.matchesAllTypes = impl.matchesAllTypes;
        this.path = impl.path;
        this.pathRestriction = impl.pathRestriction;
        this.propertyRestrictions.putAll(impl.propertyRestrictions);
        this.queryStatement = impl.queryStatement;
        this.selector = impl.selector;
        this.matchesAllTypes = selector != null ? selector.matchesAllTypes()
                : false;
    }

    public void setPreparing(boolean preparing) {
        this.preparing = preparing;
    }
    
    public boolean isPreparing() {
        return preparing;
    }
    
    /**
     * Whether the given selector is already prepared during the prepare phase
     * of a join. That means, check whether the passed selector can already
     * provide data.
     * 
     * @param selector the selector to test
     * @return true if it is already prepared
     */
    public boolean isPrepared(SelectorImpl selector) {
        return selector.isPrepared();
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
    
    @Override
    public String getPathPlan() {
        StringBuilder buff = new StringBuilder();
        String p = path;
        if (PathUtils.denotesRoot(path)) {
            p = "";
        }
        buff.append(p).append(pathRestriction);
        if (pathPlan != null) {
            buff.append(" && ").append(pathPlan);
        }
        return buff.toString();
    }

    public void setPath(String path) {
        this.path = path;
    }

    public boolean isDistinct() {
        return distinct;
    }

    public void setDistinct(boolean distinct) {
        this.distinct = distinct;
    }

    public void setAlwaysFalse() {
        propertyRestrictions.clear();
        path = "/";
        pathRestriction = PathRestriction.EXACT;
        alwaysFalse = true;
    }

    @Override
    public boolean isAlwaysFalse() {
        return alwaysFalse;
    }

    public SelectorImpl getSelector() {
        return selector;
    }

    @Override
    public boolean matchesAllTypes() {
        return matchesAllTypes;
    }

    @Override @Nonnull
    public Set<String> getSupertypes() {
        return selector.getSupertypes();
    }

    @Override @Nonnull
    public Set<String> getPrimaryTypes() {
        return selector.getPrimaryTypes();
    }

    @Override @Nonnull
    public Set<String> getMixinTypes() {
        return selector.getMixinTypes();
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
        case NO_RESTRICTION:
            return true;
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
        PropertyRestriction x = addRestricition(propertyName);
        if (x.propertyType != PropertyType.UNDEFINED && x.propertyType != propertyType) {
            // already restricted to another property type - always false
            setAlwaysFalse();
        }
        x.propertyType = propertyType;
    }
    
    public void restrictPropertyAsList(String propertyName, List<PropertyValue> list) {
        PropertyRestriction x = addRestricition(propertyName);
        if (x.list == null) {
            x.list = list;
        } else {
            x.list.retainAll(list);
        }
    }

    public void restrictProperty(String propertyName, Operator op, PropertyValue v) {
        PropertyRestriction x = addRestricition(propertyName);
        PropertyValue oldFirst = x.first;
        PropertyValue oldLast = x.last;
        switch (op) {
        case EQUAL:
            if (x.first != null && x.last == x.first && x.firstIncluding && x.lastIncluding) {
                // we keep the old equality condition if there is one;
                // we can not use setAlwaysFalse, as this would not be correct
                // for multi-valued properties:
                // unlike in databases, "x = 1 and x = 2" can match a node
                // if x is a multi-valued property with value {1, 2}
            } else {
                // all other conditions (range conditions) are replaced with this one
                // (we can not use setAlwaysFalse for the same reason as above)
                x.first = x.last = v;
                x.firstIncluding = x.lastIncluding = true;
            }
            break;
        case NOT_EQUAL:
            if (v != null) {
                throw new IllegalArgumentException("NOT_EQUAL only supported for NOT_EQUAL NULL");
            }
            break;
        case GREATER_THAN:
            // we don't narrow the range because of multi-valued properties
            if (x.first == null) {
                x.first = maxValue(oldFirst, v);
                x.firstIncluding = false;
            }
            break;
        case GREATER_OR_EQUAL:
            // we don't narrow the range because of multi-valued properties
            if (x.first == null) {
                x.first = maxValue(oldFirst, v);
                x.firstIncluding = x.first == oldFirst ? x.firstIncluding : true;
            }
            break;
        case LESS_THAN:
            // we don't narrow the range because of multi-valued properties
            if (x.last == null) {
                x.last = minValue(oldLast, v);
                x.lastIncluding = false;
            }
            break;
        case LESS_OR_EQUAL:
            // we don't narrow the range because of multi-valued properties
            if (x.last == null) {
                x.last = minValue(oldLast, v);
                x.lastIncluding = x.last == oldLast ? x.lastIncluding : true;
            }
            break;
        case LIKE:
            // we don't narrow the range because of multi-valued properties
            if (x.first == null) {
                // LIKE is handled in the fulltext index
                x.isLike = true;
                x.first = v;
            }
            break;
        case IN:
            
        }
        if (x.first != null && x.last != null) {
            if (x.first.compareTo(x.last) > 0) {
                setAlwaysFalse();
            } else if (x.first.compareTo(x.last) == 0 && (!x.firstIncluding || !x.lastIncluding)) {
                setAlwaysFalse();
            }
        }
    }
    
    private PropertyRestriction addRestricition(String propertyName) {
        PropertyRestriction x = propertyRestrictions.get(propertyName);
        if (x == null) {
            x = new PropertyRestriction();
            x.propertyName = propertyName;
            propertyRestrictions.put(propertyName, x);
        }
        return x;
    }
    
    static PropertyValue maxValue(PropertyValue a, PropertyValue b) {
        if (a == null) {
            return b;
        }
        return a.compareTo(b) < 0 ? b : a;
    }

    static PropertyValue minValue(PropertyValue a, PropertyValue b) {
        if (a == null) {
            return b;
        }
        return a.compareTo(b) <= 0 ? a : b;
    }

    @Override
    public String toString() {
        if (alwaysFalse) {
            return "Filter(always false)";
        }
        StringBuilder buff = new StringBuilder();
        buff.append("Filter(");
        if (queryStatement != null) {
            buff.append("query=").append(queryStatement);
        }
        if (fullTextConstraint != null) {
            buff.append("fullText=").append(fullTextConstraint);
        }
        buff.append(", path=").append(getPathPlan());
        if (!propertyRestrictions.isEmpty()) {
            buff.append(", property=[");
            Iterator<Entry<String, PropertyRestriction>> iterator = propertyRestrictions
                    .entrySet().iterator();
            while (iterator.hasNext()) {
                Entry<String, PropertyRestriction> p = iterator.next();
                buff.append(p.getKey()).append("=").append(p.getValue());
                if (iterator.hasNext()) {
                    buff.append(", ");
                }
            }
            buff.append("]");
        }
        buff.append(")");
        return buff.toString();
    }

    public void restrictPath(String addedPath, PathRestriction addedPathRestriction) {
        if (addedPath == null) {
            // currently unknown (prepare time)
            addedPath = "/";
        }
        if (addedPath.startsWith(JoinConditionImpl.SPECIAL_PATH_PREFIX)) {
            // not a real path, that means we only adapt the plan 
            // and that's it
            if (pathPlan == null) {
                pathPlan = "";
            } else {
                pathPlan += " && ";
            }
            pathPlan += addedPath + addedPathRestriction;
            return;
        }
        
        // calculating the intersection of path restrictions
        // this is ugly code, but I don't currently see a radically simpler method
        switch (addedPathRestriction) {
        case NO_RESTRICTION:
            break;
        case PARENT:
            switch (pathRestriction) {
            case NO_RESTRICTION:
                break;
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
            case NO_RESTRICTION:
                break;
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
            case NO_RESTRICTION:
                path = addedPath;
                pathRestriction = PathRestriction.ALL_CHILDREN;
                break;
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
            case NO_RESTRICTION:
                path = addedPath;
                pathRestriction = PathRestriction.DIRECT_CHILDREN;
                break;
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
    
    public void setFullTextConstraint(FullTextExpression constraint) {
        this.fullTextConstraint = constraint;
    }
    
    @Override
    public FullTextExpression getFullTextConstraint() {
        return fullTextConstraint;
    }
    
    @Override
    public boolean containsNativeConstraint() {
        for (String p : propertyRestrictions.keySet()) {
            if (p.startsWith(NativeFunctionImpl.NATIVE_PREFIX)) {
                return true;
            }
        }
        return false;
    }

    @Override
    @Nullable
    public String getQueryStatement() {
        return queryStatement;
    }

    public void setMatchesAllTypes(boolean matchesAllTypes) {
        this.matchesAllTypes = matchesAllTypes;
    }

}
