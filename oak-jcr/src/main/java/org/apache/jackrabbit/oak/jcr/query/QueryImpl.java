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

import javax.annotation.Nonnull;
import javax.jcr.ItemNotFoundException;
import javax.jcr.Node;
import javax.jcr.PathNotFoundException;
import javax.jcr.RepositoryException;
import javax.jcr.Value;
import javax.jcr.ValueFactory;
import javax.jcr.query.InvalidQueryException;
import javax.jcr.query.Query;
import javax.jcr.query.QueryResult;
import javax.jcr.version.VersionException;

import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.jcr.delegate.NodeDelegate;
import org.apache.jackrabbit.oak.jcr.session.NodeImpl;
import org.apache.jackrabbit.oak.jcr.session.SessionContext;
import org.apache.jackrabbit.oak.jcr.session.operation.SessionOperation;

/**
 * The implementation of the corresponding JCR interface.
 */
public class QueryImpl implements Query {

    private final SessionContext sessionContext;
    private final QueryManagerImpl manager;
    private final HashMap<String, Value> bindVariableMap = new HashMap<String, Value>();
    private final String language;
    private final String statement;
    private long limit = Long.MAX_VALUE;
    private long offset;
    private boolean parsed;
    private String storedQueryPath;

    QueryImpl(QueryManagerImpl manager, String statement, String language, SessionContext sessionContext) {
        this.sessionContext = sessionContext;
        this.manager = manager;
        this.statement = statement;
        this.language = language;
    }

    void setStoredQueryPath(String storedQueryPath) {
        this.storedQueryPath = storedQueryPath;
    }

    @Override
    public void bindValue(String varName, Value value) throws RepositoryException {
        parse();
        if (!bindVariableMap.containsKey(varName)) {
            throw new IllegalArgumentException("Variable name " + varName + " is not a valid variable in this query");
        }
        bindVariableMap.put(varName, value);
    }

    private void parse() throws InvalidQueryException, RepositoryException {
        if (parsed) {
            return;
        }
        List<String> names = sessionContext.getSessionDelegate().perform(
                new SessionOperation<List<String>>("parse") {
                    @Nonnull
                    @Override
                    public List<String> perform() throws RepositoryException {
                        return manager.parse(statement, language);
                    }
                });
        
        for (String n : names) {
            bindVariableMap.put(n, null);
        }
        parsed = true;
    }

    @Override
    public QueryResult execute() throws RepositoryException {
        return sessionContext.getSessionDelegate().perform(
                new SessionOperation<QueryResult>("execute") {
                    @Nonnull
                    @Override
                    public QueryResult perform() throws RepositoryException {
                        return manager.executeQuery(statement, language, limit,
                                offset, bindVariableMap);
                    }
                });
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
        if (storedQueryPath == null) {
            throw new ItemNotFoundException("Not a stored query");
        }
        return storedQueryPath;
    }

    @Override
    public void setLimit(long limit) {
        if (limit < 0) {
            throw new IllegalArgumentException("Limit may not be negative, is: " + limit);
        }
        this.limit = limit;
    }

    @Override
    public void setOffset(long offset) {
        if (offset < 0) {
            throw new IllegalArgumentException("Offset may not be negative, is: " + offset);
        }
        this.offset = offset;
    }

    @Override
    public Node storeAsNode(String absPath) throws RepositoryException {
        manager.ensureIsAlive();
        String oakPath = sessionContext.getOakPathOrThrow(absPath);
        String parent = PathUtils.getParentPath(oakPath);
        NodeDelegate parentDelegate = sessionContext.getSessionDelegate().getNode(parent);
        if (parentDelegate == null) {
            throw new PathNotFoundException("The specified path does not exist: " + parent);
        }
        Node parentNode = NodeImpl.createNode(parentDelegate, sessionContext);
        if (!parentNode.isCheckedOut()) {
            throw new VersionException("Cannot store query. Node at " +
                    absPath + " is checked in.");
        }
        String nodeName = PathUtils.getName(oakPath);
        ValueFactory vf = sessionContext.getValueFactory();
        Node n = parentNode.addNode(nodeName, JcrConstants.NT_QUERY);
        n.setProperty(JcrConstants.JCR_STATEMENT, vf.createValue(statement));
        n.setProperty(JcrConstants.JCR_LANGUAGE, vf.createValue(language));
        setStoredQueryPath(oakPath);
        return n;
    }

}
