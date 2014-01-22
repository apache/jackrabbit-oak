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

import static com.google.common.collect.ImmutableSet.of;
import static org.apache.jackrabbit.JcrConstants.JCR_SYSTEM;
import static org.apache.jackrabbit.oak.plugins.nodetype.NodeTypeConstants.JCR_NODE_TYPES;

import java.text.ParseException;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.jackrabbit.oak.api.PropertyValue;
import org.apache.jackrabbit.oak.api.QueryEngine;
import org.apache.jackrabbit.oak.api.Result;
import org.apache.jackrabbit.oak.namepath.LocalNameMapper;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.namepath.NamePathMapperImpl;
import org.apache.jackrabbit.oak.query.xpath.XPathToSQL2Converter;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The query engine implementation.
 */
public abstract class QueryEngineImpl implements QueryEngine {

    static final String SQL2 = "JCR-SQL2";
    static final String SQL = "sql";
    static final String XPATH = "xpath";
    static final String JQOM = "JCR-JQOM";

    static final String NO_LITERALS = "-noLiterals";

    static final Logger LOG = LoggerFactory.getLogger(QueryEngineImpl.class);
    
    private static final Set<String> SUPPORTED_LANGUAGES = of(
            SQL2,  SQL2  + NO_LITERALS,
            SQL,   SQL   + NO_LITERALS,
            XPATH, XPATH + NO_LITERALS,
            JQOM);

    /**
     * Whether fallback to the traversing index is supported if no other index
     * is available. This is enabled by default and can be disabled for testing
     * purposes.
     */
    private boolean traversalFallback = true;

    /**
     * @return Execution context for a single query execution.
     */
    protected abstract ExecutionContext getExecutionContext();

    @Override
    public Set<String> getSupportedQueryLanguages() {
        return SUPPORTED_LANGUAGES;
    }

    /**
     * Parse the query (check if it's valid) and get the list of bind variable names.
     *
     * @param statement query statement
     * @param language query language
     * @param mappings namespace prefix mappings
     * @return the list of bind variable names
     * @throws ParseException
     */
    @Override
    public List<String> getBindVariableNames(
            String statement, String language, Map<String, String> mappings)
            throws ParseException {
        Query q = parseQuery(statement, language, getExecutionContext(), mappings);
        return q.getBindVariableNames();
    }

    private static Query parseQuery(
            String statement, String language, ExecutionContext context,
            final Map<String, String> mappings) throws ParseException {
        LOG.debug("Parsing {} statement: {}", language, statement);

        NamePathMapper mapper = new NamePathMapperImpl(
                new LocalNameMapper(context.getRootTree()) {
                    @Override
                    public Map<String, String> getSessionLocalMappings() {
                        return mappings;
                    }
                });

        NodeState types = context.getBaseState()
                .getChildNode(JCR_SYSTEM)
                .getChildNode(JCR_NODE_TYPES);

        SQL2Parser parser = new SQL2Parser(mapper, types);
        if (language.endsWith(NO_LITERALS)) {
            language = language.substring(0, language.length() - NO_LITERALS.length());
            parser.setAllowNumberLiterals(false);
            parser.setAllowTextLiterals(false);
        }
        if (SQL2.equals(language) || JQOM.equals(language)) {
            return parser.parse(statement);
        } else if (SQL.equals(language)) {
            parser.setSupportSQL1(true);
            return parser.parse(statement);
        } else if (XPATH.equals(language)) {
            XPathToSQL2Converter converter = new XPathToSQL2Converter();
            String sql2 = converter.convert(statement);
            LOG.debug("XPath > SQL2: {}", sql2);
            try {
                // OAK-874: No artificial XPath selector name in wildcards
                parser.setIncludeSelectorNameInWildcardColumns(false);
                return parser.parse(sql2);
            } catch (ParseException e) {
                ParseException e2 = new ParseException(
                        statement + " converted to SQL-2 " + e.getMessage(), 0);
                e2.initCause(e);
                throw e2;
            }
        } else {
            throw new ParseException("Unsupported language: " + language, 0);
        }
    }
    
    @Override
    public Result executeQuery(
            String statement, String language, long limit, long offset,
            Map<String, ? extends PropertyValue> bindings,
            Map<String, String> mappings) throws ParseException {
        if (limit < 0) {
            throw new IllegalArgumentException("Limit may not be negative, is: " + limit);
        }
        if (offset < 0) {
            throw new IllegalArgumentException("Offset may not be negative, is: " + offset);
        }

        // avoid having to deal with null arguments
        if (bindings == null) {
            bindings = NO_BINDINGS;
        }
        if (mappings == null) {
            mappings = NO_MAPPINGS;
        }

        ExecutionContext context = getExecutionContext();
        Query q = parseQuery(statement, language, context, mappings);
        q.setExecutionContext(context);
        q.setLimit(limit);
        q.setOffset(offset);
        if (bindings != null) {
            for (Entry<String, ? extends PropertyValue> e : bindings.entrySet()) {
                q.bindValue(e.getKey(), e.getValue());
            }
        }
        q.setTraversalFallback(traversalFallback);
        q.prepare();
        return q.executeQuery();
    }

    protected void setTraversalFallback(boolean traversal) {
        this.traversalFallback = traversal;
    }

}
