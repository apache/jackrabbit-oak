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

import org.apache.jackrabbit.oak.api.PropertyValue;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.namepath.JcrPathParser;
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
import org.apache.jackrabbit.oak.spi.query.Filter;
import org.apache.jackrabbit.oak.spi.query.PropertyValues;
import org.apache.jackrabbit.oak.spi.query.QueryIndex;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Represents a parsed query. Lifecycle: use the constructor to create a new
 * object. Call init() to initialize the bind variable map. If the query is
 * re-executed, a new instance is created.
 */
public class Query {
    
    /**
     * The "jcr:path" pseudo-property.
     */
    // TODO jcr:path isn't an official feature, support it?
    public static final String JCR_PATH = "jcr:path";

    /**
     * The "jcr:score" pseudo-property.
     */
    public static final String JCR_SCORE = "jcr:score";

    private static final Logger LOG = LoggerFactory.getLogger(QueryEngineImpl.class);

    final SourceImpl source;
    final String statement;
    final ConstraintImpl constraint;
    final HashMap<String, PropertyValue> bindVariableMap = new HashMap<String, PropertyValue>();
    final HashMap<String, Integer> selectorIndexes = new HashMap<String, Integer>();
    final ArrayList<SelectorImpl> selectors = new ArrayList<SelectorImpl>();

    private QueryEngineImpl queryEngine;
    private final OrderingImpl[] orderings;
    private ColumnImpl[] columns;
    private boolean explain, measure;
    private long limit = Long.MAX_VALUE;
    private long offset;
    private long size = -1;
    private boolean prepared;
    private Root root;
    private NamePathMapper namePathMapper;

    Query(String statement, SourceImpl source, ConstraintImpl constraint, OrderingImpl[] orderings,
          ColumnImpl[] columns) {
        this.statement = statement;
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

    void bindValue(String varName, PropertyValue value) {
        bindVariableMap.put(varName, value);
    }

    public void setLimit(long limit) {
        this.limit = limit;
    }

    public void setOffset(long offset) {
        this.offset = offset;
    }

    public void setExplain(boolean explain) {
        this.explain = explain;
    }

    public void setMeasure(boolean measure) {
        this.measure = measure;
    }

    public ResultImpl executeQuery(NodeState root) {
        return new ResultImpl(this, root);
    }

    Iterator<ResultRowImpl> getRows(NodeState root) {
        prepare();
        Iterator<ResultRowImpl> it;
        if (explain) {
            String plan = source.getPlan(root);
            columns = new ColumnImpl[] { new ColumnImpl("explain", "plan", "plan")};
            ResultRowImpl r = new ResultRowImpl(this,
                    new String[0], 
                    new PropertyValue[] { PropertyValues.newString(plan)},
                    null);
            it = Arrays.asList(r).iterator();
        } else {
            if (LOG.isDebugEnabled()) {
                LOG.debug("plan: " + source.getPlan(root));
            }
            if (orderings == null) {
                // can apply limit and offset directly
                it = new RowIterator(root, limit, offset);
            } else {
                // read and order first; skip and limit afterwards
                it = new RowIterator(root, Long.MAX_VALUE, 0);
            }
            long readCount = 0;
            if (orderings != null) {
                // TODO "order by" is not necessary if the used index returns
                // rows in the same order
                    
                // avoid overflow (both offset and limit could be Long.MAX_VALUE)
                int keep = (int) Math.min(Integer.MAX_VALUE, 
                        Math.min(Integer.MAX_VALUE, offset) + 
                        Math.min(Integer.MAX_VALUE, limit));
                
                ArrayList<ResultRowImpl> list = new ArrayList<ResultRowImpl>();
                while (it.hasNext()) {
                    readCount++;
                    ResultRowImpl r = it.next();
                    list.add(r);
                    // from time to time, sort and truncate
                    // this should results in O(n*log(2*keep)) operations,
                    // which is close to the optimum O(n*log(keep))
                    if (list.size() > keep * 2) {
                        // remove tail entries right now, to save memory
                        Collections.sort(list);
                        keepFirst(list, keep);
                    }
                }
                Collections.sort(list);
                keepFirst(list, keep);
                
                it = list.iterator();
                // skip the head (this is more efficient than removing
                // if there are many entries)
                for (int i = 0; i < offset && it.hasNext(); i++) {
                    it.next();
                }
                size = list.size() - offset;
            } else if (measure) {
                while (it.hasNext()) {
                    readCount++;
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
                        new PropertyValue[] {
                                PropertyValues.newString("query"),
                                PropertyValues.newLong(readCount)
                            },
                        null);
                list.add(r);
                for (SelectorImpl selector : selectors) {
                    r = new ResultRowImpl(this,
                            new String[0],
                            new PropertyValue[] {
                                    PropertyValues.newString(selector.getSelectorName()),
                                    PropertyValues.newLong(selector.getScanCount()),
                                },
                            null);
                    list.add(r);
                }
                it = list.iterator();
            }
        }
        return it;
    }
    
    /**
     * Truncate a list.
     * 
     * @param list the list
     * @param keep the maximum number of entries to keep
     */
    private static void keepFirst(ArrayList<ResultRowImpl> list, int keep) {
        while (list.size() > keep) {
            // remove the entries starting at the end, 
            // to avoid n^2 performance
            list.remove(list.size() - 1);
        }        
    }

    public int compareRows(PropertyValue[] orderValues,
            PropertyValue[] orderValues2) {
        int comp = 0;
        for (int i = 0, size = orderings.length; i < size; i++) {
            PropertyValue a = orderValues[i];
            PropertyValue b = orderValues2[i];
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
        prepared = true;
        source.prepare();
    }

    /**
     * An iterator over result rows.
     */
    class RowIterator implements Iterator<ResultRowImpl> {

        private final NodeState root;
        private ResultRowImpl current;
        private boolean started, end;
        private long limit, offset, rowIndex;

        RowIterator(NodeState root, long limit, long offset) {
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
                source.execute(root);
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
        PropertyValue[] values = new PropertyValue[columnCount];
        for (int i = 0; i < columnCount; i++) {
            ColumnImpl c = columns[i];
            values[i] = c.currentProperty();
        }
        PropertyValue[] orderValues;
        if (orderings == null) {
            orderValues = null;
        } else {
            int size = orderings.length;
            orderValues = new PropertyValue[size];
            for (int i = 0; i < size; i++) {
                orderValues[i] = orderings[i].getOperand().currentProperty();
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
            String cn = c.getColumnName();
            if (cn != null && cn.equals(columnName)) {
                return i;
            }
        }
        throw new IllegalArgumentException("Column not found: " + columnName);
    }

    public PropertyValue getBindVariableValue(String bindVariableName) {
        PropertyValue v = bindVariableMap.get(bindVariableName);
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

    public QueryIndex getBestIndex(Filter filter) {
        return queryEngine.getBestIndex(this, filter);
    }

    public void setRoot(Root root) {
        this.root = root;
    }

    public void setNamePathMapper(NamePathMapper namePathMapper) {
        this.namePathMapper = namePathMapper;
    }

    public NamePathMapper getNamePathMapper() {
        return namePathMapper;
    }

    public Tree getTree(String path) {
        return root.getTree(path);
    }
    
    /**
     * Validate a path is syntactically correct.
     * 
     * @param path the path to validate
     */
    public void validatePath(String path) {
        if (!JcrPathParser.validate(path)) {
            throw new IllegalArgumentException("Invalid path: " + path);
        }
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

    public String getStatement() {
        return statement;
    }

}
