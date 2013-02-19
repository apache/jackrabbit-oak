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

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import javax.annotation.CheckForNull;
import javax.jcr.RepositoryException;
import javax.jcr.nodetype.NoSuchNodeTypeException;
import javax.jcr.nodetype.NodeTypeManager;

import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.PropertyValue;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.plugins.nodetype.NodeTypeConstants;
import org.apache.jackrabbit.oak.plugins.nodetype.ReadOnlyNodeTypeManager;
import org.apache.jackrabbit.oak.query.Query;
import org.apache.jackrabbit.oak.query.index.FilterImpl;
import org.apache.jackrabbit.oak.spi.query.Cursor;
import org.apache.jackrabbit.oak.spi.query.Filter;
import org.apache.jackrabbit.oak.spi.query.IndexRow;
import org.apache.jackrabbit.oak.spi.query.PropertyValues;
import org.apache.jackrabbit.oak.spi.query.QueryIndex;
import org.apache.jackrabbit.oak.spi.state.NodeState;

import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;

/**
 * A selector within a query.
 */
public class SelectorImpl extends SourceImpl {

    // TODO possibly support using multiple indexes (using index intersection / index merge)
    protected QueryIndex index;

    private final String nodeTypeName, selectorName;
    private Cursor cursor;
    private IndexRow currentRow;
    private int scanCount;

    /**
     * Names of all matching node type names encountered so far.
     */
    private final Set<String> matchingTypes;

    /**
     * Names of all <em>non-matching</em> node type names encountered so far.
     */
    private final Set<String> nonMatchingTypes;

    /**
     * The selector condition can be evaluated when the given selector is
     * evaluated. For example, for the query
     * "select * from nt:base a inner join nt:base b where a.x = 1 and b.y = 2",
     * the condition "a.x = 1" can be evaluated when evaluating selector a. The
     * other part of the condition can't be evaluated until b is available.
     */
    private ConstraintImpl selectorCondition;

    public SelectorImpl(String nodeTypeName, String selectorName) {
        this.nodeTypeName = nodeTypeName;
        this.selectorName = selectorName;

        if (JcrConstants.NT_BASE.equals(nodeTypeName)) {
            matchingTypes = null;
            nonMatchingTypes = null;
        } else {
            matchingTypes = Sets.newHashSet();
            matchingTypes.add(nodeTypeName);
            nonMatchingTypes = Sets.newHashSet();
            nonMatchingTypes.add(JcrConstants.NT_BASE);
        }
    }

    public String getSelectorName() {
        return selectorName;
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
        cursor = index.query(createFilter(false), rootState);
    }

    @Override
    public String getPlan(NodeState rootState) {
        StringBuilder buff = new StringBuilder();
        buff.append(toString());
        buff.append(" /* ").append(index.getPlan(createFilter(true), rootState));
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
    private Filter createFilter(boolean preparing) {
        FilterImpl f = new FilterImpl(this, query.getStatement());
        f.setPreparing(preparing);
        validateNodeType();
        f.setNodeType(nodeTypeName);
        if (joinCondition != null) {
            joinCondition.restrict(f);
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
        }

        return f;
    }
    
    private void validateNodeType() {
        if (!JcrConstants.NT_BASE.equals(nodeTypeName)) {
            try {
                // Check both the syntactic validity of the type name
                // and the existence of the named type in one call
                getNodeTypeManager().getNodeType(nodeTypeName);
            } catch (NoSuchNodeTypeException e) {
                // TODO: QueryManagerImpl.executeQuery() expects an
                // IllegalArgumentException to signal an invalid query.
                // This is a bit troublesome since any method could throw
                // that exception as a result of some internal programming
                // error or some other inconsistency that has nothing to
                // do with the validity of the query. A better solution
                // would be to use some checked exception or explicit
                // return value to signal whether the query is valid or not.
                throw new IllegalArgumentException(
                        "Unknown node type: " + nodeTypeName, e);
            } catch (RepositoryException e) {
                throw new RuntimeException(
                        "Unable to evaluate node type constraints", e);
            }
        }
    }

    @Override
    public boolean next() {
        while (cursor != null && cursor.hasNext()) {
            scanCount++;
            currentRow = cursor.next();
            Tree tree = getTree(currentRow.getPath());
            if (tree == null) {
                continue;
            }
            if (nodeTypeName != null
                    && !nodeTypeName.equals(JcrConstants.NT_BASE)
                    && !evaluateTypeMatch(tree)) {
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

    private boolean evaluateTypeMatch(Tree tree) {
        if (JcrConstants.NT_BASE.equals(nodeTypeName)) {
            return true; // shortcut for a common case
        }

        Set<String> types = Sets.newHashSet();

        PropertyState primary = tree.getProperty(JcrConstants.JCR_PRIMARYTYPE);
        if (primary != null && primary.getType() == Type.NAME) {
            String name = primary.getValue(Type.NAME);
            if (matchingTypes.contains(name)) {
                return true;
            } else if (!nonMatchingTypes.contains(name)) {
                types.add(name);
            }
        }

        PropertyState mixins = tree.getProperty(JcrConstants.JCR_MIXINTYPES);
        if (mixins != null && mixins.getType() == Type.NAMES) {
            for (String name : mixins.getValue(Type.NAMES)) {
                if (matchingTypes.contains(name)) {
                    return true;
                } else if (!nonMatchingTypes.contains(name)) {
                    types.add(name);
                }
            }
        }

        if (!types.isEmpty()) {
            try {
                NodeTypeManager manager = getNodeTypeManager();
                for (String type : types) {
                    if (manager.getNodeType(type).isNodeType(nodeTypeName)) {
                        matchingTypes.add(type);
                        return true;
                    } else {
                        nonMatchingTypes.add(type);
                        // continue iterating
                    }
                }
            } catch (RepositoryException e) {
                throw new RuntimeException(
                        "Unable to evaluate node type constraints", e);
            }
        }

        return false; // no matches found
    }

    private NodeTypeManager getNodeTypeManager() {
        return new ReadOnlyNodeTypeManager() {
            @Override @CheckForNull
            protected Tree getTypes() {
                return getTree(NodeTypeConstants.NODE_TYPES_PATH);
            }
        };
    }

    /**
     * Get the current absolute path (including workspace name)
     *
     * @return the path
     */
    public String currentPath() {
        return cursor == null ? null : currentRow.getPath();
    }

    public PropertyValue currentProperty(String propertyName) {
        boolean relative = propertyName.indexOf('/') >= 0;
        if (cursor == null) {
            return null;
        }
        IndexRow r = currentRow;
        if (r == null) {
            return null;
        }
        // TODO support pseudo-properties such as jcr:score using
        // r.getValue(columnName)
        String path = r.getPath();
        if (path == null) {
            return null;
        }
        Tree t = getTree(path);
        if (relative) {
            for (String p : PathUtils.elements(PathUtils.getParentPath(propertyName))) {
                if (t == null) {
                    return null;
                }
                if (p.equals("..")) {
                    t = t.getParent();
                } else if (p.equals(".")) {
                    // same node
                } else {
                    t = t.getChild(p);
                }
            }
            propertyName = PathUtils.getName(propertyName);
        }
        if (t == null) {
            return null;
        }
        if (propertyName.equals(Query.JCR_PATH)) {
            String local = getLocalPath(path);
            if (local == null) {
                // not a local path
                return null;
            }
            return PropertyValues.newString(local);
        }
        if (propertyName.equals("*")) {
            // TODO currently all property values are converted to strings - 
            // this doesn't play well with the idea that the types may be different
            List<String> values = new ArrayList<String>();
            for (PropertyState p : t.getProperties()) {
                Iterables.addAll(values, p.getValue(Type.STRINGS));
            }
            // "*"
            return PropertyValues.newString(values);
        } 
        return PropertyValues.create(t.getProperty(propertyName));
    }

    @Override
    public void init(Query query) {
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

}
