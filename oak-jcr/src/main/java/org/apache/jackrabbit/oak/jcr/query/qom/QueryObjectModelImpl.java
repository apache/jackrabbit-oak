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

import java.util.ArrayList;
import java.util.HashMap;
import javax.jcr.Node;
import javax.jcr.RepositoryException;
import javax.jcr.Value;
import javax.jcr.ValueFactory;
import javax.jcr.query.InvalidQueryException;
import javax.jcr.query.QueryResult;
import javax.jcr.query.qom.Column;
import javax.jcr.query.qom.Constraint;
import javax.jcr.query.qom.Ordering;
import javax.jcr.query.qom.QueryObjectModel;
import javax.jcr.query.qom.Selector;
import javax.jcr.query.qom.Source;
import org.apache.jackrabbit.commons.SimpleValueFactory;

/**
 * The implementation of the corresponding JCR interface.
 */
public class QueryObjectModelImpl implements QueryObjectModel {

    final Source source;
    final Constraint constraint;
    final HashMap<String, Value> bindVariableMap = new HashMap<String, Value>();
    final ArrayList<Selector> selectors = new ArrayList<Selector>();

    private final Ordering[] orderings;
    private final Column[] columns;
    private long limit;
    private long offset;
    private final ValueFactory valueFactory = new SimpleValueFactory();

    public QueryObjectModelImpl(Source source, Constraint constraint, Ordering[] orderings,
            Column[] columns) {
        this.source = source;
        this.constraint = constraint;
        this.orderings = orderings;
        this.columns = columns;
    }

    public void bindVariables() {
        ((ConstraintImpl) constraint).bindVariables(this);
    }

    @Override
    public Column[] getColumns() {
        return columns;
    }

    @Override
    public Constraint getConstraint() {
        return constraint;
    }

    @Override
    public Ordering[] getOrderings() {
        return orderings;
    }

    @Override
    public Source getSource() {
        return source;
    }

    @Override
    public String[] getBindVariableNames() {
        String[] array = new String[bindVariableMap.size()];
        array = bindVariableMap.keySet().toArray(array);
        return array;
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
    public void bindValue(String arg0, javax.jcr.Value arg1) throws IllegalArgumentException,
            RepositoryException {
        // TODO Auto-generated method stub

    }

    @Override
    public QueryResult execute() throws InvalidQueryException, RepositoryException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public String getLanguage() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public String getStatement() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public String getStoredQueryPath() throws RepositoryException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Node storeAsNode(String arg0) throws RepositoryException {
        // TODO Auto-generated method stub
        return null;
    }

    public void addBindVariable(BindVariableValueImpl var) {
        this.bindVariableMap.put(var.getBindVariableName(), null);
    }

}
