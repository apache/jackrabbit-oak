/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.oak.query;

import java.text.ParseException;
import java.util.List;
import java.util.Map;

import org.apache.jackrabbit.oak.api.ContentSession;
import org.apache.jackrabbit.oak.api.PropertyValue;
import org.apache.jackrabbit.oak.api.Result;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.SessionQueryEngine;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.spi.query.QueryIndexProvider;
import org.apache.jackrabbit.oak.spi.state.NodeState;

/**
 * The query engine implementation bound to a session.
 */
public abstract class SessionQueryEngineImpl implements SessionQueryEngine {

    private final QueryIndexProvider indexProvider;

    public SessionQueryEngineImpl(QueryIndexProvider indexProvider) {
        this.indexProvider = indexProvider;
    }

    /**
     * The implementing class must return the current root {@link NodeState}
     * associated with the {@link ContentSession}.
     *
     * @return the current root {@link NodeState}.
     */
    protected abstract NodeState getRootNodeState();

    /**
     * The implementing class must return the root associated with the
     * {@link ContentSession}.
     *
     * @return the root associated with the {@link ContentSession}.
     */
    protected abstract Root getRoot();

    @Override
    public List<String> getSupportedQueryLanguages() {
        return createQueryEngine().getSupportedQueryLanguages();
    }

    @Override
    public List<String> getBindVariableNames(String statement, String language)
            throws ParseException {
        return createQueryEngine().getBindVariableNames(statement, language);
    }

    @Override
    public Result executeQuery(String statement, String language, long limit,
            long offset, Map<String, ? extends PropertyValue> bindings,
            NamePathMapper namePathMapper) throws ParseException {
        return createQueryEngine().executeQuery(statement, language, limit,
                offset, bindings, getRoot(), namePathMapper);
    }

    private QueryEngineImpl createQueryEngine() {
        return new QueryEngineImpl(getRootNodeState(), indexProvider);
    }
}
