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
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.jcr.PropertyType;
import javax.jcr.Session;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ListMultimap;
import org.apache.jackrabbit.oak.api.PropertyValue;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.spi.query.QueryLimits;
import org.apache.jackrabbit.oak.query.ast.JoinConditionImpl;
import org.apache.jackrabbit.oak.query.ast.NativeFunctionImpl;
import org.apache.jackrabbit.oak.query.ast.Operator;
import org.apache.jackrabbit.oak.query.ast.SelectorImpl;
import org.apache.jackrabbit.oak.spi.query.fulltext.FullTextExpression;
import org.apache.jackrabbit.oak.spi.query.Filter;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.PermissionProvider;

/**
 * A filter or lookup condition.
 */
public class FilterImpl implements Filter {

    /**
     * The selector this filter applies to.
     */
    private final SelectorImpl selector;
    
    private final String queryStatement;
    
    private final QueryLimits settings;

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

    /**
     * The list of restrictions for each property. A restriction may be x=1.
     * <p>
     * Each property may have multiple restrictions, which means all
     * restrictions must apply, for example x=1 and x=2. For this case, only
     * multi-valued properties match if it contains both the values 1 and 2.
     */
    private final ListMultimap<String, PropertyRestriction> propertyRestrictions =
            ArrayListMultimap.create();

    /**
     * Only return distinct values.
     */
    private boolean distinct;
    
    /**
     * Set during the prepare phase of a query.
     */
    private boolean preparing;

    // TODO support "order by"

    /**
     * Create a new filter instance that is used for unit testing. This method
     * is relatively slow, because it creates a new query engine setting object.
     * Therefore, it is only to be used for testing. At runtime, the
     * public constructor should be used instead.
     * 
     * @return the filter
     */
    public static FilterImpl newTestInstance() {
        return new FilterImpl();
    }
    
    private FilterImpl() {
        this(null, null, new QueryLimits() {

            @Override
            public long getLimitInMemory() {
                return Long.MAX_VALUE;
            }

            @Override
            public long getLimitReads() {
                return Long.MAX_VALUE;
            }

            @Override
            public boolean getFullTextComparisonWithoutIndex() {
                return false;
            }

            @Override
            public boolean getFailTraversal() {
                return false;
            }
            
        });
    }

    /**
     * Create a filter.
     * 
     * @param selector the selector for the given filter
     * @param queryStatement the query statement
     */
    public FilterImpl(SelectorImpl selector, String queryStatement, QueryLimits settings) {
        this.selector = selector;
        this.queryStatement = queryStatement;
        this.matchesAllTypes = selector != null ? selector.matchesAllTypes()
                : false;
        this.settings = settings;
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
        this.settings = filter.getQueryLimits();
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

    @Override @Nullable
    public String getNodeType() {
        if (selector == null) {
            return null;
        }
        return selector.getNodeType();
    }

    @Override
    public boolean matchesAllTypes() {
        return matchesAllTypes;
    }

    @Override @Nonnull
    public Set<String> getSupertypes() {
        if (selector == null) {
            return Collections.emptySet();
        }
        return selector.getSupertypes();
    }

    @Override @Nonnull
    public Set<String> getPrimaryTypes() {
        if (selector == null) {
            return Collections.emptySet();
        }        
        return selector.getPrimaryTypes();
    }

    @Override @Nonnull
    public Set<String> getMixinTypes() {
        if (selector == null) {
            return Collections.emptySet();
        }             
        return selector.getMixinTypes();
    }

    @Override
    public Collection<PropertyRestriction> getPropertyRestrictions() {
        return propertyRestrictions.values();
    }

    @Override
    public PropertyRestriction getPropertyRestriction(String propertyName) {
        List<PropertyRestriction> list = propertyRestrictions.get(propertyName);
        if (list.isEmpty()) {
            return null;
        } else if (list.size() == 1) {
            return list.get(0);
        }
        int bestSort = -1;
        PropertyRestriction best = null;
        for (PropertyRestriction x : list) {
            int sort = x.sortOrder();
            if (sort > bestSort) {
                bestSort = sort;
                best = x;
            }
        }
        return best;
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
            return PathUtils.getParentPath(this.path).equals(path);
        case DIRECT_CHILDREN:
            return PathUtils.getParentPath(path).equals(this.path);
        case ALL_CHILDREN:
            return PathUtils.isAncestor(this.path, path);
        default:
            throw new IllegalArgumentException("Unknown path restriction: " + pathRestriction);
        }
    }

    public void restrictPropertyAsList(String propertyName, List<PropertyValue> list) {
        PropertyRestriction x = new PropertyRestriction();
        x.propertyName = propertyName;
        x.list = list;
        addRestriction(x);
    }
    
    public void restrictProperty(String propertyName, Operator op, PropertyValue v) {
        restrictProperty(propertyName, op, v, PropertyType.UNDEFINED);
    }

    public void restrictProperty(String propertyName, Operator op, PropertyValue v, int propertyType) {
        PropertyRestriction x = new PropertyRestriction();
        x.propertyName = propertyName;
        x.propertyType = propertyType;
        switch (op) {
        case EQUAL:
            x.first = x.last = v;
            x.firstIncluding = x.lastIncluding = true;
            break;
        case NOT_EQUAL:
            if (v != null) {
                throw new IllegalArgumentException("NOT_EQUAL only supported for NOT_EQUAL NULL");
            }
            break;
        case GREATER_THAN:
            x.first = v;
            x.firstIncluding = false;
            break;
        case GREATER_OR_EQUAL:
            x.first = v;
            x.firstIncluding = true;
            break;
        case LESS_THAN:
            x.last = v;
            x.lastIncluding = false;
            break;
        case LESS_OR_EQUAL:
            x.last = v;
            x.lastIncluding = true;
            break;
        case LIKE:
            // LIKE is handled in the fulltext index
            x.isLike = true;
            x.first = v;
            break;
        }
        addRestriction(x);
    }
    
    /**
     * Add a restriction for the given property, unless the exact same
     * restriction is already set.
     * 
     * @param restriction the restriction to add
     */
    private void addRestriction(PropertyRestriction restriction) {
        List<PropertyRestriction> list = getPropertyRestrictions(restriction.propertyName);
        for (PropertyRestriction old : list) {
            if (old.equals(restriction)) {
                return;
            }
        }
        list.add(restriction);
    }
    
    @Override
    public List<PropertyRestriction> getPropertyRestrictions(String propertyName) {
        return propertyRestrictions.get(propertyName);
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
            buff.append(" fullText=").append(fullTextConstraint);
        }
        buff.append(", path=").append(getPathPlan());
        if (!propertyRestrictions.isEmpty()) {
            buff.append(", property=[");
            Iterator<Entry<String, Collection<PropertyRestriction>>> iterator = 
                    new TreeMap<String, Collection<PropertyRestriction>>(propertyRestrictions
                    .asMap()).entrySet().iterator();
            while (iterator.hasNext()) {
                Entry<String, Collection<PropertyRestriction>> p = iterator.next();
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

    @Override
    public QueryLimits getQueryLimits() {
        return settings;
    }

    @Override
    public boolean isAccessible(String path) {
        PermissionProvider permissionProvider = selector.getQuery().getExecutionContext().getPermissionProvider();
        return permissionProvider != null && permissionProvider.isGranted(path, Session.ACTION_READ);
    }

}
