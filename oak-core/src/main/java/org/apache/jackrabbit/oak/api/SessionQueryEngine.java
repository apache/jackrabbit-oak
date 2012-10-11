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
package org.apache.jackrabbit.oak.api;

import java.text.ParseException;
import java.util.List;
import java.util.Map;

import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.value.PropertyValue;

/**
 * The query engine allows to parse and execute queries.
 * <p>
 * What query languages are supported depends on the registered query parsers.
 */
public interface SessionQueryEngine {

    /**
     * Get the list of supported query languages.
     *
     * @return the supported query languages
     */
    List<String> getSupportedQueryLanguages();

    /**
     * Parse the query (check if it's valid) and get the list of bind variable names.
     *
     * @param statement
     * @param language
     * @return the list of bind variable names
     * @throws ParseException
     */
    List<String> getBindVariableNames(String statement, String language) throws ParseException;

    /**
     * Execute a query and get the result.
     *
     * @param statement the query statement
     * @param language the language
     * @param limit the maximum result set size
     * @param offset the number of rows to skip
     * @param bindings the bind variable value bindings
     * @param root the root to use
     * @param namePathMapper the name and path mapper to use
     * @return the result
     * @throws ParseException if the statement could not be parsed
     * @throws IllegalArgumentException if there was an error executing the query
     */
    Result executeQuery(String statement, String language,
            long limit, long offset, Map<String, ? extends PropertyValue> bindings,
            Root root, NamePathMapper namePathMapper) throws ParseException;

    // TODO pass namespace mapping
    // TODO pass node type information (select * from [xyz] is supposed to return at least the mandatory columns for xyz)

}
