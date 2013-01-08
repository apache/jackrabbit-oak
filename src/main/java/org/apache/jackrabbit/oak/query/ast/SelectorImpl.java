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

import static org.apache.jackrabbit.oak.api.Type.STRINGS;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import javax.annotation.CheckForNull;
import javax.jcr.RepositoryException;
import javax.jcr.nodetype.NodeType;
import javax.jcr.nodetype.NodeTypeIterator;
import javax.jcr.nodetype.NodeTypeManager;

import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.PropertyValue;
import org.apache.jackrabbit.oak.api.Tree;
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

import com.google.common.collect.ImmutableSet;

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
     * Iterable over selected node type and its subtypes
     */
    private Iterable<NodeType> nodeTypes;

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


    @Override
    public void prepare() {
        if (queryConstraint != null) {
            queryConstraint.restrictPushDown(this);
        }
        for (JoinConditionImpl c : allJoinConditions) {
            c.restrictPushDown(this);
        }
        index = query.getBestIndex(createFilter());
    }

    @Override
    public void execute(NodeState rootState) {
        cursor = index.query(createFilter(), rootState);
    }

    @Override
    public String getPlan(NodeState rootState) {
        StringBuilder buff = new StringBuilder();
        buff.append(toString());
        buff.append(" /* ").append(index.getPlan(createFilter(), rootState));
        if (selectorCondition != null) {
            buff.append(" where ").append(selectorCondition);
        }
        buff.append(" */");
        return buff.toString();
    }

    private Filter createFilter() {
        FilterImpl f = new FilterImpl(this, query.getStatement());
        validateNodeType(nodeTypeName);
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
    
    private void validateNodeType(String nodeType) {
        // this looks a bit weird, but it should be correct - the code
        // assumes that paths and node type names have the same format
        // restrictions (characters such as "[" are not allowed and so on)
        query.validatePath(nodeType);
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
        Set<String> primary =
                getStrings(tree, JcrConstants.JCR_PRIMARYTYPE);
        Set<String> mixins =
                getStrings(tree, JcrConstants.JCR_MIXINTYPES);

        try {
            for (NodeType type : getNodeTypes()) {
                if (evaluateTypeMatch(type, primary, mixins)) {
                    return true;
                }
            }
        } catch (RepositoryException e) {
            throw new RuntimeException(
                    "Unable to evaluate node type constraints", e);
        }

        return false;
    }

    private static Set<String> getStrings(Tree tree, String name) {
        ImmutableSet.Builder<String> builder = ImmutableSet.builder();
        PropertyState property = tree.getProperty(name);
        if (property != null) {
            for (String value : property.getValue(STRINGS)) {
                builder.add(value);
            }
        }
        return builder.build();
    }

    private static boolean evaluateTypeMatch(
            NodeType type, Set<String> primary, Set<String> mixins) {
        String name = type.getName();
        if (type.isMixin()) {
            return mixins.contains(name);
        } else {
            return primary.contains(name);
        }
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

    private Iterable<NodeType> getNodeTypes() throws RepositoryException {
        if (nodeTypes == null) {
            List<NodeType> types = new ArrayList<NodeType>();
            NodeTypeManager manager = new ReadOnlyNodeTypeManager() {
                @Override @CheckForNull
                protected Tree getTypes() {
                    return getTree(NodeTypeConstants.NODE_TYPES_PATH);
                }
            };
            NodeType type = manager.getNodeType(nodeTypeName);
            types.add(type);

            NodeTypeIterator it = type.getSubtypes();
            while (it.hasNext()) {
                types.add(it.nextNodeType());
            }
            nodeTypes = types;
        }
        return nodeTypes;
    }
}
