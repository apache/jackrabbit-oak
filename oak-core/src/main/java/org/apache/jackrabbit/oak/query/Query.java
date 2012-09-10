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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import javax.jcr.PropertyType;
import org.apache.jackrabbit.mk.api.MicroKernel;
import org.apache.jackrabbit.oak.api.ContentSession;
import org.apache.jackrabbit.oak.api.CoreValue;
import org.apache.jackrabbit.oak.api.CoreValueFactory;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
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
import org.apache.jackrabbit.oak.query.ast.FullTextSearchImpl;
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
import org.apache.jackrabbit.oak.spi.query.QueryIndex;
import org.apache.jackrabbit.oak.spi.state.NodeState;

/**
 * Represents a parsed query. Lifecycle: use the constructor to create a new
 * object. Call init() to initialize the bind variable map.
 */
public class Query {

    final SourceImpl source;
    final ConstraintImpl constraint;
    final HashMap<String, CoreValue> bindVariableMap = new HashMap<String, CoreValue>();
    final HashMap<String, Integer> selectorIndexes = new HashMap<String, Integer>();
    final ArrayList<SelectorImpl> selectors = new ArrayList<SelectorImpl>();

    private MicroKernel mk;
    private QueryEngineImpl queryEngine;
    private final OrderingImpl[] orderings;
    private ColumnImpl[] columns;
    private boolean explain, measure;
    private long limit = Long.MAX_VALUE;
    private long offset;
    private long size = -1;
    private boolean prepared;
    private final CoreValueFactory valueFactory;
    private ContentSession session;
    private NamePathMapper namePathMapper;

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
            public boolean visit(FullTextSearchImpl node) {
                node.setQuery(query);
                node.bindSelector(source);
                return super.visit(node);
            }

            @Override
            public boolean visit(FullTextSearchScoreImpl node) {
                node.setQuery(query);
                node.bindSelector(source);
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
                String name = node.getSelectorName();
                if (selectorIndexes.put(name, selectors.size()) != null) {
                    throw new IllegalArgumentException("Two selectors with the same name: " + name);
                }
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
        source.setQueryConstraint(constraint);
        source.init(this);
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

    public void setMeasure(boolean measure) {
        this.measure = measure;
    }

    public ResultImpl executeQuery(String revisionId, NodeState root) {
        return new ResultImpl(this, revisionId, root);
    }

    Iterator<ResultRowImpl> getRows(String revisionId, NodeState root) {
        prepare();
        Iterator<ResultRowImpl> it;
        if (explain) {
            String plan = source.getPlan();
            columns = new ColumnImpl[] { new ColumnImpl("explain", "plan", "plan")};
            ResultRowImpl r = new ResultRowImpl(this,
                    new String[0],
                    new CoreValue[] { getValueFactory().createValue(plan) },
                    null);
            it = Arrays.asList(r).iterator();
        } else {
            it = new RowIterator(revisionId, root, limit, offset);
            long resultCount = 0;
            if (orderings != null) {
                // TODO "order by" is not necessary if the used index returns
                // rows in the same order
                ArrayList<ResultRowImpl> list = new ArrayList<ResultRowImpl>();
                while (it.hasNext()) {
                    ResultRowImpl r = it.next();
                    list.add(r);
                }
                resultCount = size = list.size();
                Collections.sort(list);
                it = list.iterator();
            } else if (measure) {
                while (it.hasNext()) {
                    resultCount++;
                    it.next();
                }
            }
            if (measure) {
                columns = new ColumnImpl[] {
                        new ColumnImpl("measure", "selector", "selector"),
                        new ColumnImpl("measure", "scanCount", "scanCount")
                };
                ArrayList<ResultRowImpl> list = new ArrayList<ResultRowImpl>();
                ResultRowImpl r = new ResultRowImpl(this,
                        new String[0],
                        new CoreValue[] {
                            getValueFactory().createValue("query"),
                            getValueFactory().createValue(resultCount),
                            },
                        null);
                list.add(r);
                for (SelectorImpl selector : selectors) {
                    r = new ResultRowImpl(this,
                            new String[0],
                            new CoreValue[] {
                                getValueFactory().createValue(selector.getSelectorName()),
                                getValueFactory().createValue(selector.getScanCount()),
                                },
                            null);
                    list.add(r);
                }
                it = list.iterator();
            }
        }
        return it;
    }

    public int compareRows(CoreValue[][] orderValues, CoreValue[][] orderValues2) {
        int comp = 0;
        for (int i = 0, size = orderings.length; i < size; i++) {
            CoreValue[] a = orderValues[i];
            CoreValue[] b = orderValues2[i];
            if (a == null || b == null) {
                if (a == b) {
                    comp = 0;
                } else if (a == null) {
                    // TODO order by: nulls first (it looks like), or low?
                    comp = -1;
                } else {
                    comp = 1;
                }
            } else {
                comp = compareValues(a, b);
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

    public static int compareValues(CoreValue[] orderValues, CoreValue[] orderValues2) {
        int l1 = orderValues.length;
        int l2 = orderValues2.length;
        int len = Math.max(l1, l2);
        for (int i = 0; i < len; i++) {
            CoreValue a = i < l1 ? orderValues[i] : null;
            CoreValue b = i < l2 ? orderValues2[i] : null;
            int comp;
            if (a == null) {
                comp = 1;
            } else if (b == null) {
                comp = -1;
            } else {
                comp = a.compareTo(b);
            }
            if (comp != 0) {
                return comp;
            }
        }
        return 0;
    }

    public static int getType(PropertyState p, int ifUnknown) {
        if (!p.isArray()) {
            return p.getValue().getType();
        }
        Iterator<CoreValue> it = p.getValues().iterator();
        if (it.hasNext()) {
            return it.next().getType();
        }
        return ifUnknown;
    }

    public CoreValue convert(CoreValue v, int targetType) {
        // TODO support full set of conversion features defined in the JCR spec
        // at 3.6.4 Property Type Conversion
        // re-use existing code if possible
        int sourceType = v.getType();
        if (sourceType == targetType) {
            return v;
        }
        CoreValueFactory vf = getValueFactory();
        switch (sourceType) {
        case PropertyType.STRING:
            switch(targetType) {
            case PropertyType.BINARY:
                try {
                    byte[] data = v.getString().getBytes("UTF-8");
                    return vf.createValue(new ByteArrayInputStream(data));
                } catch (IOException e) {
                    // I don't know in what case that could really occur
                    // except if UTF-8 isn't supported
                    throw new IllegalArgumentException(v.getString(), e);
                }
            }
            return vf.createValue(v.getString(), targetType);
        }
        switch (targetType) {
        case PropertyType.STRING:
            return vf.createValue(v.getString());
        case PropertyType.BOOLEAN:
            return vf.createValue(v.getBoolean());
        case PropertyType.DATE:
            return vf.createValue(v.getString(), PropertyType.DATE);
        case PropertyType.LONG:
            return vf.createValue(v.getLong());
        case PropertyType.DOUBLE:
            return vf.createValue(v.getDouble());
        case PropertyType.DECIMAL:
            return vf.createValue(v.getString(), PropertyType.DECIMAL);
        case PropertyType.NAME:
            return vf.createValue(getOakPath(v.getString()), PropertyType.NAME);
        case PropertyType.PATH:
            return vf.createValue(v.getString(), PropertyType.PATH);
        case PropertyType.REFERENCE:
            return vf.createValue(v.getString(), PropertyType.REFERENCE);
        case PropertyType.WEAKREFERENCE:
            return vf.createValue(v.getString(), PropertyType.WEAKREFERENCE);
        case PropertyType.URI:
            return vf.createValue(v.getString(), PropertyType.URI);
        case PropertyType.BINARY:
            try {
                byte[] data = v.getString().getBytes("UTF-8");
                return vf.createValue(new ByteArrayInputStream(data));
            } catch (IOException e) {
                // I don't know in what case that could really occur
                // except if UTF-8 isn't supported
                throw new IllegalArgumentException(v.getString(), e);
            }
        }
        throw new IllegalArgumentException("Unknown property type: " + targetType);
    }

    public String getOakPath(String jcrPath) {
        NamePathMapper m = getNamePathMapper();
        if (m == null) {
            // to simplify testing, a getNamePathMapper isn't required
            return jcrPath;
        }
        String p = m.getOakPath(jcrPath);
        if (p == null) {
            throw new IllegalArgumentException("Not a valid JCR path: " + jcrPath);
        }
        return p;
    }

    void prepare() {
        if (prepared) {
            return;
        }
        prepared = true;
        source.prepare(mk);
    }

    /**
     * An iterator over result rows.
     */
    class RowIterator implements Iterator<ResultRowImpl> {

        private final String revisionId;
        private final NodeState root;
        private ResultRowImpl current;
        private boolean started, end;
        private long limit, offset, rowIndex;

        RowIterator(String revisionId, NodeState root, long limit, long offset) {
            this.revisionId = revisionId;
            this.root = root;
            this.limit = limit;
            this.offset = offset;
        }

        private void fetchNext() {
            if (end) {
                return;
            }
            if (rowIndex >= limit) {
                end = true;
                return;
            }
            if (!started) {
                source.execute(revisionId, root);
                started = true;
            }
            while (true) {
                if (source.next()) {
                    if (constraint == null || constraint.evaluate()) {
                        if (offset > 0) {
                            offset--;
                            continue;
                        }
                        current = currentRow();
                        rowIndex++;
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
            PropertyState p = c.currentProperty();
            values[i] = p == null ? null : p.getValue();
        }
        CoreValue[][] orderValues;
        if (orderings == null) {
            orderValues = null;
        } else {
            int size = orderings.length;
            orderValues = new CoreValue[size][];
            for (int i = 0; i < size; i++) {
                PropertyState p = orderings[i].getOperand().currentProperty();
                CoreValue[] x;
                if (p == null) {
                    x = null;
                } else if (p.isArray()) {
                    List<CoreValue> list = p.getValues();
                    x = list.toArray(new CoreValue[list.size()]);
                } else {
                    x = new CoreValue[] { p.getValue() };
                }
                orderValues[i] = x;
            }
        }
        return new ResultRowImpl(this, paths, values, orderValues);
    }

    public int getSelectorIndex(String selectorName) {
        Integer index = selectorIndexes.get(selectorName);
        if (index == null) {
            throw new IllegalArgumentException("Unknown selector: " + selectorName);
        }
        return index;
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

    public String getWorkspaceName() {
        return session.getWorkspaceName();
    }

    public void setQueryEngine(QueryEngineImpl queryEngine) {
        this.queryEngine = queryEngine;
    }

    public QueryIndex getBestIndex(FilterImpl filter) {
        return queryEngine.getBestIndex(filter);
    }

    public void setSession(ContentSession session) {
        this.session = session;
    }

    public void setNamePathMapper(NamePathMapper namePathMapper) {
        this.namePathMapper = namePathMapper;
    }

    public NamePathMapper getNamePathMapper() {
        return namePathMapper;
    }

    public Tree getTree(String path) {
        return session.getCurrentRoot().getTree(path);
    }

    @Override
    public String toString() {
        StringBuilder buff = new StringBuilder();
        buff.append("select ");
        int i = 0;
        for (ColumnImpl c : columns) {
            if (i++ > 0) {
                buff.append(", ");
            }
            buff.append(c);
        }
        buff.append(" from ").append(source);
        if (constraint != null) {
            buff.append(" where ").append(constraint);
        }
        if (orderings != null) {
            buff.append(" order by ");
            i = 0;
            for (OrderingImpl o : orderings) {
                if (i++ > 0) {
                    buff.append(", ");
                }
                buff.append(o);
            }
        }
        return buff.toString();
    }

    public long getSize() {
        return size;
    }

}
