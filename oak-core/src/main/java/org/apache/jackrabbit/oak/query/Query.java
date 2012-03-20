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
import org.apache.jackrabbit.mk.api.MicroKernel;
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

/**
 * Represents a parsed query. Lifecycle: use the constructor to create a new
 * object. Call init() to initialize the bind variable map.
 */
public class Query {

    final SourceImpl source;
    final ConstraintImpl constraint;
    final HashMap<String, Value> bindVariableMap = new HashMap<String, Value>();
    final ArrayList<SelectorImpl> selectors = new ArrayList<SelectorImpl>();

    private MicroKernel mk;
    private final OrderingImpl[] orderings;
    private final ColumnImpl[] columns;
    private boolean explain;
    private long limit;
    private long offset;
    private boolean prepared;
    private final ValueFactory valueFactory = new ValueFactory();

    Query(SourceImpl source, ConstraintImpl constraint, OrderingImpl[] orderings,
            ColumnImpl[] columns) {
        this.source = source;
        this.constraint = constraint;
        this.orderings = orderings;
        this.columns = columns;
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

    void bindValue(String varName, Value value) {
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

    public ValueFactory getValueFactory() {
        return valueFactory;
    }

    public void setExplain(boolean explain) {
        this.explain = explain;
    }

    public Iterator<Row> executeQuery(String revisionId) {
        prepare();
        if (explain) {
            String plan = source.getPlan();
            Row r = new Row(this, null, new Value[] { valueFactory.createValue(plan) }, null);
            return Arrays.asList(r).iterator();
        }
        RowIterator it = new RowIterator(revisionId);
        if (orderings == null) {
            return it;
        }
        // TODO "order by" is not necessary if the used index returns rows in the same order
        ArrayList<Row> list = new ArrayList<Row>();
        while (it.hasNext()) {
            Row r = it.next();
            list.add(r);
        }
        Collections.sort(list);
        return list.iterator();
    }

    public int compareRows(Value[] orderValues, Value[] orderValues2) {
        int comp = 0;
        for (int i = 0, size = orderings.length; i < size; i++) {
            Value a = orderValues[i];
            Value b = orderValues2[i];
            // TODO order by: currently use string compare
            String as = a == null ? null : a.getString();
            String bs = b == null ? null : b.getString();
            if (as == null || bs == null) {
                if (as == bs) {
                    comp = 0;
                } else if (as == null) {
                    // TODO order by: nulls first, last, low or high?
                    comp = 1;
                }
            } else {
                comp = as.compareTo(bs);
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

    private void prepare() {
        if (prepared) {
            return;
        }
        source.prepare(mk);
    }

    class RowIterator implements Iterator<Row> {

        private final String revisionId;
        private Row current;
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
        public Row next() {
            if (end) {
                return null;
            }
            if (current == null) {
                fetchNext();
            }
            Row r = current;
            current = null;
            return r;
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }

    }

    Row currentRow() {
        int selectorCount = selectors.size();
        String[] paths = new String[selectorCount];
        for (int i = 0; i < selectorCount; i++) {
            SelectorImpl s = selectors.get(i);
            paths[i] = s.currentPath();
        }
        int columnCount = columns.length;
        Value[] values = new Value[columnCount];
        for (int i = 0; i < columnCount; i++) {
            ColumnImpl c = columns[i];
            values[i] = c.currentValue();
        }
        Value[] orderValues;
        if (orderings == null) {
            orderValues = null;
        } else {
            int size = orderings.length;
            orderValues = new Value[size];
            for (int i = 0; i < size; i++) {
                orderValues[i] = orderings[i].getOperand().currentValue();
            }
        }
        return new Row(this, paths, values, orderValues);
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

}
