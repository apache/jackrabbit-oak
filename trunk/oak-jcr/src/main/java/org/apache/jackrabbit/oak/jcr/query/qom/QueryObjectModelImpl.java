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
package org.apache.jackrabbit.oak.jcr.query.qom;

import java.util.HashMap;
import javax.jcr.ItemNotFoundException;
import javax.jcr.Node;
import javax.jcr.RepositoryException;
import javax.jcr.Value;
import javax.jcr.ValueFactory;
import javax.jcr.query.Query;
import javax.jcr.query.QueryResult;
import javax.jcr.query.qom.Column;
import javax.jcr.query.qom.Constraint;
import javax.jcr.query.qom.Ordering;
import javax.jcr.query.qom.QueryObjectModel;
import javax.jcr.query.qom.Source;
import org.apache.jackrabbit.oak.jcr.query.QueryManagerImpl;

/**
 * The implementation of the corresponding JCR interface.
 */
public class QueryObjectModelImpl implements QueryObjectModel {

    private final Source source;
    private final Constraint constraint;
    private final HashMap<String, Value> bindVariableMap = new HashMap<String, Value>();
    private final QueryManagerImpl queryManager;
    private final ValueFactory valueFactory;
    private final Ordering[] orderings;
    private final Column[] columns;
    private long limit = Long.MAX_VALUE;
    private long offset;
    private boolean parsed;
    private String storedQueryPath;

    public QueryObjectModelImpl(QueryManagerImpl queryManager,
            ValueFactory valueFactory, Source source, Constraint constraint,
            Ordering[] orderings, Column[] columns) {
        this.queryManager = queryManager;
        this.valueFactory = valueFactory;
        this.source = source;
        this.constraint = constraint;
        this.orderings = orderings;
        this.columns = columns;
    }

    public void bindVariables() {
        if (constraint != null) {
            ((ConstraintImpl) constraint).bindVariables(this);
        }
    }

    @Override
    public Column[] getColumns() {
        return columns == null ? new Column[0] : columns;
    }

    @Override
    public Constraint getConstraint() {
        return constraint;
    }

    @Override
    public Ordering[] getOrderings() {
        return orderings == null ? new Ordering[0] : orderings;
    }

    @Override
    public Source getSource() {
        return source;
    }

    @Override
    public String[] getBindVariableNames() throws RepositoryException {
        parse();
        String[] names = new String[bindVariableMap.size()];
        bindVariableMap.keySet().toArray(names);
        return names;
    }

    @Override
    public void setLimit(long limit) {
        this.limit = limit;
    }

    @Override
    public void setOffset(long offset) {
        this.offset = offset;
    }

    public ValueFactory getValueFactory() {
        return valueFactory;
    }

    @Override
    public void bindValue(String varName, Value value) throws RepositoryException {
        parse();
        if (!bindVariableMap.containsKey(varName)) {
            throw new IllegalArgumentException("Variable name " + varName +
                    " is not a valid variable in this query");
        }
        bindVariableMap.put(varName, value);
    }

    private void parse() throws RepositoryException {
        if (parsed) {
            return;
        }
        String[] names = queryManager.createQuery(getStatement(), Query.JCR_SQL2).
                getBindVariableNames();
        for (String n : names) {
            bindVariableMap.put(n, null);
        }
        parsed = true;
    }

    @Override
    public QueryResult execute() throws RepositoryException {
        return queryManager.executeQuery(getStatement(), Query.JCR_SQL2,
                limit, offset, bindVariableMap);
    }

    @Override
    public String getLanguage() {
        return Query.JCR_JQOM;
    }

    @Override
    public String getStatement() {
        StringBuilder buff = new StringBuilder();
        buff.append("select ");
        int i;
        if (columns != null && columns.length > 0) {
            i = 0;
            for (Column c : columns) {
                if (i++ > 0) {
                    buff.append(", ");
                }
                buff.append(c);
            }
        } else {
            buff.append("*");
        }
        buff.append(" from ");
        buff.append(source);
        if (constraint != null) {
            buff.append(" where ");
            buff.append(constraint);
        }
        if (orderings != null && orderings.length > 0) {
            buff.append(" order by ");
            i = 0;
            for (Ordering o : orderings) {
                if (i++ > 0) {
                    buff.append(", ");
                }
                buff.append(o);
            }
        }
        return buff.toString();
    }

    @Override
    public String getStoredQueryPath() throws RepositoryException {
        if (storedQueryPath == null) {
            throw new ItemNotFoundException("Not a stored query");
        }
        return storedQueryPath;
    }

    @Override
    public Node storeAsNode(String absPath) throws RepositoryException {
        Node n = queryManager.createQuery(getStatement(), Query.JCR_JQOM).
                storeAsNode(absPath);
        storedQueryPath = n.getPath();
        return n;
    }

    public void addBindVariable(BindVariableValueImpl var) {
        this.bindVariableMap.put(var.getBindVariableName(), null);
    }

    @Override
    public String toString() {
        return getStatement();
    }

}
