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

import org.apache.jackrabbit.oak.api.PropertyValue;
import org.apache.jackrabbit.oak.api.Result;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.SessionQueryEngine;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.spi.query.QueryIndexProvider;
import org.apache.jackrabbit.oak.spi.state.NodeStore;

/**
 * The query engine implementation bound to a session.
 */
public class SessionQueryEngineImpl implements SessionQueryEngine {

    private final QueryEngineImpl queryEngine;

    public SessionQueryEngineImpl(NodeStore store,
            QueryIndexProvider indexProvider) {
        this.queryEngine = new QueryEngineImpl(store, indexProvider);
    }

    @Override
    public List<String> getSupportedQueryLanguages() {
        return queryEngine.getSupportedQueryLanguages();
    }

    @Override
    public List<String> getBindVariableNames(String statement, String language)
            throws ParseException {
        return queryEngine.getBindVariableNames(statement, language);
    }

    @Override
    public Result executeQuery(String statement, String language, long limit,
            long offset, Map<String, ? extends PropertyValue> bindings, Root root,
            NamePathMapper namePathMapper) throws ParseException {
        return queryEngine.executeQuery(statement, language, limit, offset,
                bindings, root, namePathMapper);
    }

}
