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
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.jackrabbit.oak.api.PropertyValue;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.query.index.TraversingIndex;
import org.apache.jackrabbit.oak.spi.query.Filter;
import org.apache.jackrabbit.oak.spi.query.QueryIndex;
import org.apache.jackrabbit.oak.spi.query.QueryIndexProvider;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The query engine implementation.
 */
public class QueryEngineImpl {

    static final String SQL2 = "JCR-SQL2";
    static final String SQL = "sql";
    static final String XPATH = "xpath";
    static final String JQOM = "JCR-JQOM";

    private static final Logger LOG = LoggerFactory.getLogger(QueryEngineImpl.class);

    private final NodeState root;
    private final QueryIndexProvider indexProvider;

    public QueryEngineImpl(NodeState root, QueryIndexProvider indexProvider) {
        this.root = root;
        this.indexProvider = indexProvider;
    }

    public List<String> getSupportedQueryLanguages() {
        return Arrays.asList(SQL2, SQL, XPATH, JQOM);
    }

    /**
     * Parse the query (check if it's valid) and get the list of bind variable names.
     *
     * @param statement
     * @param language
     * @return the list of bind variable names
     * @throws ParseException
     */
    public List<String> getBindVariableNames(String statement, String language) throws ParseException {
        Query q = parseQuery(statement, language);
        return q.getBindVariableNames();
    }

    private Query parseQuery(String statement, String language) throws ParseException {
        Query q;
        if (LOG.isDebugEnabled()) {
            LOG.debug(language + ": " + statement);
        }
        if (SQL2.equals(language) || JQOM.equals(language)) {
            SQL2Parser parser = new SQL2Parser();
            q = parser.parse(statement);
        } else if (SQL.equals(language)) {
            SQL2Parser parser = new SQL2Parser();
            parser.setSupportSQL1(true);
            q = parser.parse(statement);
        } else if (XPATH.equals(language)) {
            XPathToSQL2Converter converter = new XPathToSQL2Converter();
            String sql2 = converter.convert(statement);
            SQL2Parser parser = new SQL2Parser();
            if (LOG.isDebugEnabled()) {
                LOG.debug("XPath > SQL2: " + sql2);
            }
            try {
                q = parser.parse(sql2);
            } catch (ParseException e) {
                throw new ParseException(statement + " converted to SQL-2 " + e.getMessage(), 0);
            }
        } else {
            throw new ParseException("Unsupported language: " + language, 0);
        }
        return q;
    }

    public ResultImpl executeQuery(String statement, String language, 
            long limit, long offset, Map<String, ? extends PropertyValue> bindings,
            Root root,
            NamePathMapper namePathMapper) throws ParseException {
        Query q = parseQuery(statement, language);
        q.setRoot(root);
        q.setNamePathMapper(namePathMapper);
        q.setLimit(limit);
        q.setOffset(offset);
        if (bindings != null) {
            for (Entry<String, ? extends PropertyValue> e : bindings.entrySet()) {
                q.bindValue(e.getKey(), e.getValue());
            }
        }
        q.setQueryEngine(this);
        q.prepare();
        return q.executeQuery(this.root);
    }

    public QueryIndex getBestIndex(Filter filter) {
        QueryIndex best = null;
        double bestCost = Double.MAX_VALUE;
        for (QueryIndex index : getIndexes()) {
            double cost = index.getCost(filter, root);
            if (LOG.isDebugEnabled()) {
                LOG.debug("cost for " + index + " is " + cost);
            }            
            if (cost < bestCost) {
                bestCost = cost;
                best = index;
            }
        }
        if (best == null) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("no indexes found - using TraversingIndex; indexProvider: " + indexProvider);
            }
            best = new TraversingIndex();
        }
        return best;
    }

    private List<? extends QueryIndex> getIndexes() {
        return indexProvider.getQueryIndexes(root);
    }

}
