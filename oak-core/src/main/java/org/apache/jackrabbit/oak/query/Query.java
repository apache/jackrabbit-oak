/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0 Unless required by applicable law
 * or agreed to in writing, software distributed under the License is
 * distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language
 * governing permissions and limitations under the License.
 */
package org.apache.jackrabbit.oak.query;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import org.apache.jackrabbit.mk.api.MicroKernel;
import org.apache.jackrabbit.oak.api.CoreValue;
import org.apache.jackrabbit.oak.api.CoreValueFactory;
import org.apache.jackrabbit.oak.query.ast.AstVisitorBase;
import org.apache.jackrabbit.oak.query.ast.BindVariableValueImpl;
import org.apache.jackrabbit.oak.query.ast.ChildNodeImpl;
import org.apache.jackrabbit.oak.query.ast.ChildNodeJoinConditionImpl;
import org.apache.jackrabbit.oak.query.ast.ColumnImpl;
import org.apache.jackrabbit.oak.query.ast.ComparisonImpl;
import org.apache.jackrabbit.oak.query.ast.ConstraintImpl;
import org.apache.jackrabbit.oak.query.ast.DescendantNodeImpl;
import org.apache.jackrabbit.oak.query.ast.DescendantNodeJoinConditionImpl;
import org.apache.jackrabbit.oak.query.ast.EquiJoinConditionImpl;
import org.apache.jackrabbit.oak.query.ast.FullTextSearchScoreImpl;
import org.apache.jackrabbit.oak.query.ast.LengthImpl;
import org.apache.jackrabbit.oak.query.ast.LiteralImpl;
import org.apache.jackrabbit.oak.query.ast.LowerCaseImpl;
import org.apache.jackrabbit.oak.query.ast.NodeLocalNameImpl;
import org.apache.jackrabbit.oak.query.ast.NodeNameImpl;
import org.apache.jackrabbit.oak.query.ast.OrderingImpl;
import org.apache.jackrabbit.oak.query.ast.PropertyExistenceImpl;
import org.apache.jackrabbit.oak.query.ast.PropertyValueImpl;
import org.apache.jackrabbit.oak.query.ast.SameNodeImpl;
import org.apache.jackrabbit.oak.query.ast.SameNodeJoinConditionImpl;
import org.apache.jackrabbit.oak.query.ast.SelectorImpl;
import org.apache.jackrabbit.oak.query.ast.SourceImpl;
import org.apache.jackrabbit.oak.query.ast.UpperCaseImpl;
import org.apache.jackrabbit.oak.query.index.FilterImpl;
import org.apache.jackrabbit.oak.spi.QueryIndex;

/**
 * Represents a parsed query. Lifecycle: use the constructor to create a new
 * object. Call init() to initialize the bind variable map.
 */
public class Query {

    final SourceImpl source;
    final ConstraintImpl constraint;
    final HashMap<String, CoreValue> bindVariableMap = new HashMap<String, CoreValue>();
    final ArrayList<SelectorImpl> selectors = new ArrayList<SelectorImpl>();

    private MicroKernel mk;
    private QueryEngineImpl queryEngine;
    private final OrderingImpl[] orderings;
    private ColumnImpl[] columns;
    private boolean explain;
    private long limit;
    private long offset;
    private boolean prepared;
    private final CoreValueFactory valueFactory;

    Query(SourceImpl source, ConstraintImpl constraint, OrderingImpl[] orderings,
          ColumnImpl[] columns, CoreValueFactory valueFactory) {
        this.source = source;
        this.constraint = constraint;
        this.orderings = orderings;
        this.columns = columns;
        this.valueFactory = valueFactory;
    }

    public void init() {

        final Query query = this;

        new AstVisitorBase() {

            @Override
            public boolean visit(BindVariableValueImpl node) {
                node.setQuery(query);
                bindVariableMap.put(node.getBindVariableName(), null);
                return true;
            }

            @Override
            public boolean visit(ChildNodeImpl node) {
                node.setQuery(query);
                node.bindSelector(source);
                return true;
            }

            @Override
            public boolean visit(ChildNodeJoinConditionImpl node) {
                node.setQuery(query);
                node.bindSelector(source);
                return true;
            }

            @Override
            public boolean visit(ColumnImpl node) {
                node.setQuery(query);
                return true;
            }

            @Override
            public boolean visit(DescendantNodeImpl node) {
                node.setQuery(query);
                node.bindSelector(source);
                return true;
            }

            @Override
            public boolean visit(DescendantNodeJoinConditionImpl node) {
                node.setQuery(query);
                node.bindSelector(source);
                return true;
            }

            @Override
            public boolean visit(EquiJoinConditionImpl node) {
                node.setQuery(query);
                node.bindSelector(source);
                return true;
            }

            @Override
            public boolean visit(FullTextSearchScoreImpl node) {
                node.setQuery(query);
                return true;
            }

            @Override
            public boolean visit(LiteralImpl node) {
                node.setQuery(query);
                return true;
            }

            @Override
            public boolean visit(NodeLocalNameImpl node) {
                node.setQuery(query);
                node.bindSelector(source);
                return true;
            }

            @Override
            public boolean visit(NodeNameImpl node) {
                node.setQuery(query);
                node.bindSelector(source);
                return true;
            }

            @Override
            public boolean visit(PropertyExistenceImpl node) {
                node.setQuery(query);
                node.bindSelector(source);
                return true;
            }

            @Override
            public boolean visit(PropertyValueImpl node) {
                node.setQuery(query);
                node.bindSelector(source);
                return true;
            }

            @Override
            public boolean visit(SameNodeImpl node) {
                node.setQuery(query);
                node.bindSelector(source);
                return true;
            }

            @Override
            public boolean visit(SameNodeJoinConditionImpl node) {
                node.setQuery(query);
                node.bindSelector(source);
                return true;
            }

            @Override
            public boolean visit(SelectorImpl node) {
                selectors.add(node);
                node.setQuery(query);
                return true;
            }

            @Override
            public boolean visit(LengthImpl node) {
                node.setQuery(query);
                return super.visit(node);
            }

            @Override
            public boolean visit(UpperCaseImpl node) {
                node.setQuery(query);
                return super.visit(node);
            }

            @Override
            public boolean visit(LowerCaseImpl node) {
                node.setQuery(query);
                return super.visit(node);
            }

            @Override
            public boolean visit(ComparisonImpl node) {
                node.setQuery(query);
                return super.visit(node);
            }

        }.visit(this);
        source.init(this);
        source.setQueryConstraint(constraint);
        for (ColumnImpl column : columns) {
            column.bindSelector(source);
        }
    }

    public ColumnImpl[] getColumns() {
        return columns;
    }

    public ConstraintImpl getConstraint() {
        return constraint;
    }

    public OrderingImpl[] getOrderings() {
        return orderings;
    }

    public SourceImpl getSource() {
        return source;
    }

    void bindValue(String varName, CoreValue value) {
        bindVariableMap.put(varName, value);
    }

    public void setMicroKernel(MicroKernel mk) {
        this.mk = mk;
    }

    public void setLimit(long limit) {
        this.limit = limit;
    }

    public void setOffset(long offset) {
        this.offset = offset;
    }

    public CoreValueFactory getValueFactory() {
        return valueFactory;
    }

    public void setExplain(boolean explain) {
        this.explain = explain;
    }

    public ResultImpl executeQuery(String revisionId) {
        return new ResultImpl(this, revisionId);
    }

    Iterator<ResultRowImpl> getRows(String revisionId) {
        prepare();
        Iterator<ResultRowImpl> it;
        if (explain) {
            String plan = source.getPlan();
            columns = new ColumnImpl[] { new ColumnImpl("explain", "plan", "plan")};
            ResultRowImpl r = new ResultRowImpl(this, new String[0], new CoreValue[] { getValueFactory().createValue(plan) }, null);
            it = Arrays.asList(r).iterator();
        } else {
            it = new RowIterator(revisionId);
            if (orderings != null) {
                // TODO "order by" is not necessary if the used index returns
                // rows in the same order
                ArrayList<ResultRowImpl> list = new ArrayList<ResultRowImpl>();
                while (it.hasNext()) {
                    ResultRowImpl r = it.next();
                    list.add(r);
                }
                Collections.sort(list);
                it = list.iterator();
            }
        }
        return it;
    }

    public int compareRows(CoreValue[] orderValues, CoreValue[] orderValues2) {
        int comp = 0;
        for (int i = 0, size = orderings.length; i < size; i++) {
            CoreValue a = orderValues[i];
            CoreValue b = orderValues2[i];
            if (a == null || b == null) {
                if (a == b) {
                    comp = 0;
                } else if (a == null) {
                    // TODO order by: nulls first, last, low or high?
                    comp = 1;
                } else {
                    comp = -1;
                }
            } else {
                comp = a.compareTo(b);
            }
            if (comp != 0) {
                if (orderings[i].isDescending()) {
                    comp = -comp;
                }
                break;
            }
        }
        return comp;
    }

    void prepare() {
        if (prepared) {
            return;
        }
        source.prepare(mk);
    }

    class RowIterator implements Iterator<ResultRowImpl> {

        private final String revisionId;
        private ResultRowImpl current;
        private boolean started, end;

        RowIterator(String revisionId) {
            this.revisionId = revisionId;
        }

        private void fetchNext() {
            if (end) {
                return;
            }
            if (!started) {
                source.execute(revisionId);
                started = true;
            }
            while (true) {
                if (source.next()) {
                    if (constraint == null || constraint.evaluate()) {
                        current = currentRow();
                        break;
                    }
                } else {
                    current = null;
                    end = true;
                    break;
                }
            }
        }

        @Override
        public boolean hasNext() {
            if (end) {
                return false;
            }
            if (current == null) {
                fetchNext();
            }
            return !end;
        }

        @Override
        public ResultRowImpl next() {
            if (end) {
                return null;
            }
            if (current == null) {
                fetchNext();
            }
            ResultRowImpl r = current;
            current = null;
            return r;
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }

    }

    ResultRowImpl currentRow() {
        int selectorCount = selectors.size();
        String[] paths = new String[selectorCount];
        for (int i = 0; i < selectorCount; i++) {
            SelectorImpl s = selectors.get(i);
            paths[i] = s.currentPath();
        }
        int columnCount = columns.length;
        CoreValue[] values = new CoreValue[columnCount];
        for (int i = 0; i < columnCount; i++) {
            ColumnImpl c = columns[i];
            values[i] = c.currentValue();
        }
        CoreValue[] orderValues;
        if (orderings == null) {
            orderValues = null;
        } else {
            int size = orderings.length;
            orderValues = new CoreValue[size];
            for (int i = 0; i < size; i++) {
                orderValues[i] = orderings[i].getOperand().currentValue();
            }
        }
        return new ResultRowImpl(this, paths, values, orderValues);
    }

    public int getSelectorIndex(String selectorName) {
        for (int i = 0, size = selectors.size(); i < size; i++) {
            if (selectors.get(i).getSelectorName().equals(selectorName)) {
                return i;
            }
        }
        throw new IllegalArgumentException("Unknown selector: " + selectorName);
    }

    public int getColumnIndex(String columnName) {
        for (int i = 0, size = columns.length; i < size; i++) {
            ColumnImpl c = columns[i];
            if (c.getColumnName().equals(columnName)) {
                return i;
            }
        }
        throw new IllegalArgumentException("Column not found: " + columnName);
    }

    public long getLimit() {
        return limit;
    }

    public long getOffset() {
        return offset;
    }

    public CoreValue getBindVariableValue(String bindVariableName) {
        CoreValue v = bindVariableMap.get(bindVariableName);
        if (v == null) {
            throw new IllegalArgumentException("Bind variable value not set: " + bindVariableName);
        }
        return v;
    }

    public List<SelectorImpl> getSelectors() {
        return Collections.unmodifiableList(selectors);
    }

    public List<String> getBindVariableNames() {
        return new ArrayList<String>(bindVariableMap.keySet());
    }

    public void setQueryEngine(QueryEngineImpl queryEngine) {
        this.queryEngine = queryEngine;
    }

    public QueryIndex getBestIndex(FilterImpl filter) {
        return queryEngine.getBestIndex(filter);
    }

}
