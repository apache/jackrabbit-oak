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

import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.mk.api.MicroKernel;
import org.apache.jackrabbit.oak.api.CoreValue;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.plugins.memory.SinglePropertyState;
import org.apache.jackrabbit.oak.query.Query;
import org.apache.jackrabbit.oak.query.index.FilterImpl;
import org.apache.jackrabbit.oak.spi.query.Cursor;
import org.apache.jackrabbit.oak.spi.query.IndexRow;
import org.apache.jackrabbit.oak.spi.query.QueryIndex;
import org.apache.jackrabbit.oak.spi.state.NodeState;

/**
 * A selector within a query.
 */
public class SelectorImpl extends SourceImpl {

    private static final String JCR_PRIMARY_TYPE = "jcr:primaryType";

    private static final String TYPE_BASE = "nt:base";

    // TODO possibly support using multiple indexes (using index intersection / index merge)
    protected QueryIndex index;

    private final String nodeTypeName, selectorName;
    private Cursor cursor;
    private int scanCount;

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
    public void prepare(MicroKernel mk) {
        if (queryConstraint != null) {
            queryConstraint.restrictPushDown(this);
        }
        for (JoinConditionImpl c : allJoinConditions) {
            c.restrictPushDown(this);
        }
        index = query.getBestIndex(createFilter());
    }

    @Override
    public void execute(String revisionId, NodeState root) {
        cursor = index.query(createFilter(), revisionId, root);
    }

    @Override
    public String getPlan() {
        StringBuilder buff = new StringBuilder();
        buff.append(toString());
        buff.append(" /* ").append(index.getPlan(createFilter()));
        if (selectorCondition != null) {
            buff.append(" where ").append(selectorCondition);
        }
        buff.append(" */");
        return buff.toString();
    }

    private FilterImpl createFilter() {
        FilterImpl f = new FilterImpl(this);
        f.setNodeType(nodeTypeName);
        if (joinCondition != null) {
            joinCondition.restrict(f);
        }
        if (!outerJoin) {
            // for outer joins, query constraints can't be applied to the
            // filter, because that would alter the result
            if (queryConstraint != null) {
                queryConstraint.restrict(f);
            }
        }
        return f;
    }

    @Override
    public boolean next() {
        while (true) {
            if (!nextNode()) {
                return false;
            }
            if (selectorCondition != null && !selectorCondition.evaluate()) {
                continue;
            }
            if (joinCondition != null && !joinCondition.evaluate()) {
                continue;
            }
            return true;
        }
    }

    private boolean nextNode() {
        if (cursor == null) {
            return false;
        }
        scanCount++;
        while (true) {
            boolean result = cursor.next();
            if (!result) {
                return false;
            }
            if (nodeTypeName.equals(TYPE_BASE)) {
                return true;
            }
            Tree tree = getTree(cursor.currentRow().getPath());
            if (tree == null) {
                return false;
            }
            PropertyState p = tree.getProperty(JCR_PRIMARY_TYPE);
            if (p == null) {
                return true;
            }
            CoreValue v = p.getValue();
            // TODO node type matching
            if (nodeTypeName.equals(v.getString())) {
                return true;
            }
            PropertyState m = tree.getProperty(JcrConstants.JCR_MIXINTYPES);
            if (m != null) {
                for (CoreValue value : m.getValues()) {
                    if (nodeTypeName.equals(value.getString())) {
                        return true;
                    }
                }
            }
        }
    }

    /**
     * Get the current absolute path (including workspace name)
     *
     * @return the path
     */
    public String currentPath() {
        return cursor == null ? null : cursor.currentRow().getPath();
    }

    public PropertyState currentProperty(String propertyName) {
        if (propertyName.equals(Query.JCR_PATH)) {
            String p = currentPath();
            if (p == null) {
                return null;
            }
            String local = getLocalPath(p);
            if (local == null) {
                // not a local path
                return null;
            }
            CoreValue v = query.getValueFactory().createValue(local);
            return new SinglePropertyState(Query.JCR_PATH, v);
        }
        if (cursor == null) {
            return null;
        }
        IndexRow r = cursor.currentRow();
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
        return t == null ? null : t.getProperty(propertyName);
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
