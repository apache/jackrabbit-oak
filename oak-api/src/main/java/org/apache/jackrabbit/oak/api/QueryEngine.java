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

import static java.util.Collections.emptyMap;

import java.text.ParseException;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.osgi.annotation.versioning.ProviderType;

/**
 * The query engine allows to parse and execute queries.
 * <p>
 * What query languages are supported depends on the registered query parsers.
 */
@ProviderType
public interface QueryEngine {
    
    /**
     * The suffix for internal SQL-2 statements. Those are logged with trace
     * level instead of debug level.
     */
    String INTERNAL_SQL2_QUERY = " /* oak-internal */";

    /**
     * Empty set of variables bindings. Useful as an argument to
     * {@link #executeQuery(String, String, long, long, Map, Map)} when
     * there are no variables in a query.
     */
    Map<String, PropertyValue> NO_BINDINGS = emptyMap();

    /**
     * Empty set of namespace prefix mappings. Useful as an argument to
     * {@link #getBindVariableNames(String, String, Map)} and
     * {@link #executeQuery(String, String, long, long, Map, Map)} when
     * there are no local namespace mappings.
     */
    Map<String, String> NO_MAPPINGS = emptyMap();

    /**
     * Get the set of supported query languages.
     *
     * @return the supported query languages
     */
    Set<String> getSupportedQueryLanguages();

    /**
     * Parse the query (check if it's valid) and get the list of bind variable names.
     *
     * @param statement query statement
     * @param language query language
     * @param mappings namespace prefix mappings
     * @return the list of bind variable names
     * @throws ParseException
     */
    List<String> getBindVariableNames(
            String statement, String language, Map<String, String> mappings)
            throws ParseException;

    /**
     * Execute a query and get the result.
     *
     * @param statement the query statement
     * @param language the language
     * @param limit the maximum result set size (may not be negative)
     * @param offset the number of rows to skip (may not be negative)
     * @param bindings the bind variable value bindings
     * @param mappings namespace prefix mappings
     * @return the result
     * @throws ParseException if the statement could not be parsed
     * @throws IllegalArgumentException if there was an error executing the query
     */
    Result executeQuery(
            String statement, String language, long limit, long offset,
            Map<String, ? extends PropertyValue> bindings,
            Map<String, String> mappings) throws ParseException;
    
    /**
     * Execute a query and get the result.
     * This is a convenience method: no limit, and offset 0.
     *
     * @param statement the query statement
     * @param language the language
     * @param bindings the bind variable value bindings
     * @param mappings namespace prefix mappings
     * @return the result
     * @throws ParseException if the statement could not be parsed
     * @throws IllegalArgumentException if there was an error executing the query
     */
    Result executeQuery(
            String statement, String language,
            Map<String, ? extends PropertyValue> bindings,
            Map<String, String> mappings) throws ParseException;    

}
