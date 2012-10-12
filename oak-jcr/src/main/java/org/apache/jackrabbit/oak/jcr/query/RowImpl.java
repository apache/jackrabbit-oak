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

import javax.jcr.Node;
import javax.jcr.RepositoryException;
import javax.jcr.Value;
import javax.jcr.query.Row;

import org.apache.jackrabbit.oak.api.PropertyValue;
import org.apache.jackrabbit.oak.api.ResultRow;

/**
 * The implementation of the corresponding JCR interface.
 */
public class RowImpl implements Row {

    private final QueryResultImpl result;
    private final ResultRow row;

    public RowImpl(QueryResultImpl result, ResultRow row) {
        this.result = result;
        this.row = row;
    }

    @Override
    public Node getNode() throws RepositoryException {
        return result.getNode(getPath());
    }

    @Override
    public Node getNode(String selectorName) throws RepositoryException {
        return result.getNode(getPath(selectorName));
    }

    @Override
    public String getPath() throws RepositoryException {
        try {
            return result.getLocalPath(row.getPath());
        } catch (IllegalArgumentException e) {
            throw new RepositoryException(e);
        }
    }

    @Override
    public String getPath(String selectorName) throws RepositoryException {
        try {
            return result.getLocalPath(row.getPath(selectorName));
        } catch (IllegalArgumentException e) {
            throw new RepositoryException(e);
        }
    }

    @Override
    public double getScore() throws RepositoryException {
        // TODO row score
        return 0;
    }

    @Override
    public double getScore(String selectorName) throws RepositoryException {
        // TODO row score
        return 0;
    }

    @Override
    public Value getValue(String columnName) throws RepositoryException {
        try {
            return result.createValue(row.getValue(columnName));
        } catch (IllegalArgumentException e) {
            throw new RepositoryException(e);
        }
    }

    @Override
    public Value[] getValues() throws RepositoryException {
        PropertyValue[] values = row.getValues();
        int len = values.length;
        Value[] v2 = new Value[values.length];
        for (int i = 0; i < len; i++) {
            v2[i] = result.createValue(values[i]);
        }
        return v2;
    }

}
