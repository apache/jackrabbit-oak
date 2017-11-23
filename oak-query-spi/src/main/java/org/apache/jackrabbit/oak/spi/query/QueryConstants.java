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

package org.apache.jackrabbit.oak.spi.query;

public abstract class QueryConstants {

    /**
     * Name of the property restriction used to express query performed
     * via NAME and LOCALNAME functions
     */
    public static final String RESTRICTION_LOCAL_NAME = ":localname";

    /**
     * Name of the property restriction used to express query performed
     * via NAME and LOCALNAME functions
     */
    public static final String RESTRICTION_NAME = ":name";

    /**
     * The prefix for restrictions for function-based indexes, for example
     * upper(propertyName). Syntax: "function*expression". In order to support
     * all kinds of expressions in the future (including nested expressions and
     * so on), the format for the expression is written in the Polish notation
     * (the RPN, reversed), with "*" as delimiter (as property names may not
     * contain "*"), and "@" in front of each property name to distinguish
     * between property names and functions. Literals are quoted. Examples: The
     * expression "lower(lastName)" is converted to "function*lower {@literal @}
     * lastName". The expression "lower(lastName)" is converted to
     * "lower(upper(lastName))" is converted to "function*lower*upper*
     * {@literal @}lastName". The condition
     * "firstName+' '+lastName = 'Tim Cook'" would be "function*+*+ {@literal @}
     * firstName*' ' {@literal @}lastName.
     */
    public static final String FUNCTION_RESTRICTION_PREFIX = "function*";

    public static final String SEARCH_ROOT_PATH = "/jcr:root";
    /**
     * The "jcr:path" pseudo-property.
     */
    // TODO jcr:path isn't an official feature, support it?
    public static final String JCR_PATH = "jcr:path";

    /**
     * The "jcr:score" pseudo-property.
     */
    public static final String JCR_SCORE = "jcr:score";

    /**
     * The "rep:excerpt" pseudo-property.
     */
    public static final String REP_EXCERPT = "rep:excerpt";

    /**
     * The "rep:facet" pseudo-property.
     */
    public static final String REP_FACET = "rep:facet";

    /**
     * The "oak:explainScore" pseudo-property.
     */
    public static final String OAK_SCORE_EXPLANATION = "oak:scoreExplanation";

    /**
     * The "rep:spellcheck" pseudo-property.
     */
    public static final String REP_SPELLCHECK = "rep:spellcheck()";

    /**
     * The "rep:suggest" pseudo-property.
     */
    public static final String REP_SUGGEST = "rep:suggest()";
}