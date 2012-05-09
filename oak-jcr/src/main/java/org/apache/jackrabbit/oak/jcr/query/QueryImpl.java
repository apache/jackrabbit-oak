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
package org.apache.jackrabbit.oak.jcr.query;

import java.util.HashMap;
import java.util.List;
import javax.jcr.Node;
import javax.jcr.PathNotFoundException;
import javax.jcr.RepositoryException;
import javax.jcr.UnsupportedRepositoryOperationException;
import javax.jcr.Value;
import javax.jcr.query.InvalidQueryException;
import javax.jcr.query.Query;
import javax.jcr.query.QueryResult;

/**
 * The implementation of the corresponding JCR interface.
 */
public class QueryImpl implements Query {

    private final QueryManagerImpl manager;
    private final HashMap<String, Value> bindVariableMap = new HashMap<String, Value>();
    private final String language;
    private final String statement;
    private long limit, offset;
    private boolean parsed;

    QueryImpl(QueryManagerImpl manager, String statement, String language) {
        this.manager = manager;
        this.statement = statement;
        this.language = language;
    }

    @Override
    public void bindValue(String varName, Value value) throws RepositoryException {
        parse();
        if (!bindVariableMap.containsKey(varName)) {
            throw new IllegalArgumentException("Variable name " + varName + " is not a valid variable in this query");
        }
        bindVariableMap.put(varName, value);
    }

    private void parse() throws InvalidQueryException {
        if (parsed) {
            return;
        }
        List<String> names = manager.parse(statement, language);
        for (String n : names) {
            bindVariableMap.put(n, null);
        }
    }

    @Override
    public QueryResult execute() throws RepositoryException {
        return manager.executeQuery(statement, language, bindVariableMap, limit, offset);
    }

    @Override
    public String[] getBindVariableNames() throws RepositoryException {
        parse();
        String[] names = new String[bindVariableMap.size()];
        bindVariableMap.keySet().toArray(names);
        return names;
    }

    @Override
    public String getLanguage() {
        return language;
    }

    @Override
    public String getStatement() {
        return statement;
    }

    @Override
    public String getStoredQueryPath() throws RepositoryException {
        // TODO not implemented yet
        return null;
    }

    @Override
    public void setLimit(long limit) {
        this.limit = limit;
    }

    @Override
    public void setOffset(long offset) {
        this.offset = offset;
    }

    @Override
    public Node storeAsNode(String absPath) throws RepositoryException {
        manager.ensureIsAlive();
        if (manager.getSessionDelegate().getNode(absPath) == null) {
            throw new PathNotFoundException("The specified path does not exist: " + absPath);
        }
        // TODO not implemented yet
        throw new UnsupportedRepositoryOperationException("This feature is not supported");
    }

}
