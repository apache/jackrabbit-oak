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
package org.apache.jackrabbit.oak.query.ast;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.collect.Sets.newHashSet;
import static org.apache.jackrabbit.JcrConstants.JCR_ISMIXIN;
import static org.apache.jackrabbit.JcrConstants.JCR_MIXINTYPES;
import static org.apache.jackrabbit.JcrConstants.JCR_NODETYPENAME;
import static org.apache.jackrabbit.JcrConstants.JCR_PRIMARYTYPE;
import static org.apache.jackrabbit.JcrConstants.NT_BASE;
import static org.apache.jackrabbit.oak.api.Type.NAME;
import static org.apache.jackrabbit.oak.api.Type.NAMES;
import static org.apache.jackrabbit.oak.plugins.nodetype.NodeTypeConstants.REP_MIXIN_SUBTYPES;
import static org.apache.jackrabbit.oak.plugins.nodetype.NodeTypeConstants.REP_NAMED_SINGLE_VALUED_PROPERTIES;
import static org.apache.jackrabbit.oak.plugins.nodetype.NodeTypeConstants.REP_PRIMARY_SUBTYPES;
import static org.apache.jackrabbit.oak.plugins.nodetype.NodeTypeConstants.REP_SUPERTYPES;

import java.util.ArrayList;
import java.util.Set;

import javax.annotation.Nonnull;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.PropertyValue;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.query.QueryImpl;
import org.apache.jackrabbit.oak.query.fulltext.FullTextExpression;
import org.apache.jackrabbit.oak.query.index.FilterImpl;
import org.apache.jackrabbit.oak.spi.query.Cursor;
import org.apache.jackrabbit.oak.spi.query.Cursors;
import org.apache.jackrabbit.oak.spi.query.Filter;
import org.apache.jackrabbit.oak.spi.query.IndexRow;
import org.apache.jackrabbit.oak.spi.query.PropertyValues;
import org.apache.jackrabbit.oak.spi.query.QueryIndex;
import org.apache.jackrabbit.oak.spi.state.NodeState;

import com.google.common.collect.ImmutableSet;

/**
 * A selector within a query.
 */
public class SelectorImpl extends SourceImpl {

    // TODO possibly support using multiple indexes (using index intersection / index merge)
    protected QueryIndex index;

    /**
     * the node type associated with the {@link #nodeTypeName}
     */
    private final NodeState nodeType;

    private final String selectorName;

    private final String nodeTypeName;

    private final boolean matchesAllTypes;

    /**
     * All of the matching supertypes, or empty if the {@link #matchesAllTypes}
     * flag is set
     */
    private final Set<String> supertypes;

    /**
     * All of the matching primary subtypes, or empty if the
     * {@link #matchesAllTypes} flag is set
     */
    private final Set<String> primaryTypes;

    /**
     * All of the matching mixin types, or empty if the {@link #matchesAllTypes}
     * flag is set
     */
    private final Set<String> mixinTypes;

    private Cursor cursor;
    private IndexRow currentRow;
    private int scanCount;
    
    private Tree lastTree;
    private String lastPath;

    /**
     * The selector condition can be evaluated when the given selector is
     * evaluated. For example, for the query
     * "select * from nt:base a inner join nt:base b where a.x = 1 and b.y = 2",
     * the condition "a.x = 1" can be evaluated when evaluating selector a. The
     * other part of the condition can't be evaluated until b is available.
     */
    private ConstraintImpl selectorCondition;

    public SelectorImpl(NodeState nodeType, String selectorName) {
        this.nodeType = checkNotNull(nodeType);
        this.selectorName = checkNotNull(selectorName);

        this.nodeTypeName = nodeType.getName(JCR_NODETYPENAME);
        this.matchesAllTypes = NT_BASE.equals(nodeTypeName);

        if (!this.matchesAllTypes) {
            this.supertypes = newHashSet(nodeType.getNames(REP_SUPERTYPES));
            supertypes.add(nodeTypeName);

            this.primaryTypes = newHashSet(nodeType
                    .getNames(REP_PRIMARY_SUBTYPES));
            this.mixinTypes = newHashSet(nodeType.getNames(REP_MIXIN_SUBTYPES));
            if (nodeType.getBoolean(JCR_ISMIXIN)) {
                mixinTypes.add(nodeTypeName);
            } else {
                primaryTypes.add(nodeTypeName);
            }
        } else {
            this.supertypes = ImmutableSet.of();
            this.primaryTypes = ImmutableSet.of();
            this.mixinTypes = ImmutableSet.of();
        }
    }

    public String getSelectorName() {
        return selectorName;
    }

    public boolean matchesAllTypes() {
        return matchesAllTypes;
    }

    /**
     * @return all of the matching supertypes, or empty if the
     *         {@link #matchesAllTypes} flag is set
     */
    @Nonnull
    public Set<String> getSupertypes() {
        return supertypes;
    }

    /**
     * @return all of the matching primary subtypes, or empty if the
     *         {@link #matchesAllTypes} flag is set
     */
    @Nonnull
    public Set<String> getPrimaryTypes() {
        return primaryTypes;
    }

    /**
     * @return all of the matching mixin types, or empty if the
     *         {@link #matchesAllTypes} flag is set
     */
    @Nonnull
    public Set<String> getMixinTypes() {
        return mixinTypes;
    }

    public Iterable<String> getWildcardColumns() {
        return nodeType.getNames(REP_NAMED_SINGLE_VALUED_PROPERTIES);
    }

    @Override
    boolean accept(AstVisitor v) {
        return v.visit(this);
    }

    @Override
    public String toString() {
        return quote(nodeTypeName) + " as " + quote(selectorName);
    }

    public boolean isPrepared() {
        return index != null;
    }

    @Override
    public void prepare() {
        if (queryConstraint != null) {
            queryConstraint.restrictPushDown(this);
        }
        if (!outerJoinLeftHandSide && !outerJoinRightHandSide) {
            for (JoinConditionImpl c : allJoinConditions) {
                c.restrictPushDown(this);
            }
        }
        index = query.getBestIndex(createFilter(true));
    }

    @Override
    public void execute(NodeState rootState) {
        if (index != null) {
            cursor = index.query(createFilter(false), rootState);
        } else {
            cursor = Cursors.newPathCursor(new ArrayList<String>());
        }
    }

    @Override
    public String getPlan(NodeState rootState) {
        StringBuilder buff = new StringBuilder();
        buff.append(toString());
        buff.append(" /* ");
        if (index != null) {
            buff.append(index.getPlan(createFilter(true), rootState));
        } else {
            buff.append("no-index");
        }
        if (selectorCondition != null) {
            buff.append(" where ").append(selectorCondition);
        }
        buff.append(" */");
        return buff.toString();
    }

    /**
     * Create the filter condition for planning or execution.
     * 
     * @param preparing whether a filter for the prepare phase should be made 
     * @return the filter
     */
    @Override
    public Filter createFilter(boolean preparing) {
        FilterImpl f = new FilterImpl(this, query.getStatement());
        f.setPreparing(preparing);
        if (joinCondition != null) {
            joinCondition.restrict(f);
        }
        // rep:excerpt handling: create a (fake) restriction
        // "rep:excerpt is not null" to let the index know that
        // we will need the excerpt
        for (ColumnImpl c : query.getColumns()) {
            if (c.getSelector() == this) {
                if (c.getColumnName().equals("rep:excerpt")) {
                    f.restrictProperty("rep:excerpt", Operator.NOT_EQUAL, null);
                }
            }
        }
        
        // all conditions can be pushed to the selectors -
        // except in some cases to "outer joined" selectors,
        // but the exceptions are handled in the condition
        // itself.
        // An example where it *is* a problem:
        //  "select * from a left outer join b on a.x = b.y
        // where b.y is null" - in this case the selector b
        // must not use an index condition on "y is null"
        // (".. is null" must be written as "not .. is not null").
        if (queryConstraint != null) {
            queryConstraint.restrict(f);
            FullTextExpression ft = queryConstraint.getFullTextConstraint(this);
            f.setFullTextConstraint(ft);
        }

        return f;
    }

    @Override
    public boolean next() {
        while (cursor != null && cursor.hasNext()) {
            scanCount++;
            currentRow = cursor.next();
            if (isParent) {
                // we must not check whether the _parent_ is readable
                // for joins of type
                // "select [b].[jcr:primaryType]
                // from [nt:base] as [a] 
                // inner join [nt:base] as [b] 
                // on isdescendantnode([b], [a]) 
                // where [b].[jcr:path] = $path"
                // because if we did, we would filter out
                // correct results
            } else {
                // we must check whether the _child_ is readable
                // (even if no properties are read) for joins of type
                // "select [a].[jcr:primaryType]
                // from [nt:base] as [a] 
                // inner join [nt:base] as [b] 
                // on isdescendantnode([b], [a]) 
                // where [a].[jcr:path] = $path"
                // because not checking would reveal existence
                // of the child node
                Tree tree = getTree(currentRow.getPath());
                if (tree == null || !tree.exists()) {
                    continue;
                }
            }
            if (!matchesAllTypes && !evaluateTypeMatch()) {
                continue;
            }
            if (selectorCondition != null && !selectorCondition.evaluate()) {
                continue;
            }
            if (joinCondition != null && !joinCondition.evaluate()) {
                continue;
            }
            return true;
        }
        cursor = null;
        currentRow = null;
        return false;
    }

    private boolean evaluateTypeMatch() {
        Tree tree = getTree(currentRow.getPath());
        if (tree == null || !tree.exists()) {
            return false;
        }
        PropertyState primary = tree.getProperty(JCR_PRIMARYTYPE);
        if (primary != null && primary.getType() == NAME) {
            String name = primary.getValue(NAME);
            if (primaryTypes.contains(name)) {
                return true;
            }
        }

        PropertyState mixins = tree.getProperty(JCR_MIXINTYPES);
        if (mixins != null && mixins.getType() == NAMES) {
            for (String name : mixins.getValue(NAMES)) {
                if (mixinTypes.contains(name)) {
                    return true;
                }
            }
        }
        // no matches found
        return false; 
    }

    /**
     * Get the current absolute Oak path (normalized).
     *
     * @return the path
     */
    public String currentPath() {
        return cursor == null ? null : currentRow.getPath();
    }
    
    /**
     * Get the tree at the current path.
     * 
     * @return the current tree, or null
     */
    public Tree currentTree() {
        String path = currentPath();
        if (path == null) {
            return null;
        }
        return getTree(path);
    }
    
    /**
     * Get the tree at the given path.
     * 
     * @param path the path
     * @return the tree, or null
     */
    Tree getTree(String path) {
        if (lastPath == null || !path.equals(lastPath)) {
            lastTree = query.getTree(path);
            lastPath = path;
        }
        return lastTree;
    }

    /**
     * The value for the given selector for the current node.
     * 
     * @param propertyName the JCR (not normalized) property name
     * @return the property value
     */
    public PropertyValue currentProperty(String propertyName) {
        String pn = normalizePropertyName(propertyName);
        return currentOakProperty(pn);
    }
    
    public PropertyValue currentOakProperty(String propertyName) {
        boolean relative = propertyName.indexOf('/') >= 0;
        Tree t = currentTree();
        if (relative) {
            for (String p : PathUtils.elements(PathUtils.getParentPath(propertyName))) {
                if (t == null) {
                    return null;
                }
                if (p.equals("..")) {
                    t = t.isRoot() ? null : t.getParent();
                } else if (p.equals(".")) {
                    // same node
                } else {
                    t = t.getChild(p);
                }
            }
            propertyName = PathUtils.getName(propertyName);
        }
        if (t == null || !t.exists()) {
            return null;
        }
        if (propertyName.equals(QueryImpl.JCR_PATH)) {
            String path = currentPath();
            String local = getLocalPath(path);
            if (local == null) {
                // not a local path
                return null;
            }
            return PropertyValues.newString(local);
        } else if (propertyName.equals(QueryImpl.JCR_SCORE)) {
            return currentRow.getValue(QueryImpl.JCR_SCORE);
        } else if (propertyName.equals(QueryImpl.REP_EXCERPT)) {
            return currentRow.getValue(QueryImpl.REP_EXCERPT);
        }
        return PropertyValues.create(t.getProperty(propertyName));
    }

    @Override
    public void init(QueryImpl query) {
        // nothing to do
    }

    @Override
    public SelectorImpl getSelector(String selectorName) {
        if (selectorName.equals(this.selectorName)) {
            return this;
        }
        return null;
    }

    public long getScanCount() {
        return scanCount;
    }

    public void restrictSelector(ConstraintImpl constraint) {
        if (selectorCondition == null) {
            selectorCondition = constraint;
        } else {
            selectorCondition = new AndImpl(selectorCondition, constraint);
        }
    }
    
    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        } else if (!(this instanceof SelectorImpl)) {
            return false;
        }
        return selectorName.equals(((SelectorImpl) other).selectorName);
    }
    
    @Override
    public int hashCode() {
        return selectorName.hashCode();
    }

    @Override
    protected void setParent(JoinConditionImpl joinCondition) {
        if (joinCondition.isParent(this)) {
            isParent = true;
        }
    }

}
