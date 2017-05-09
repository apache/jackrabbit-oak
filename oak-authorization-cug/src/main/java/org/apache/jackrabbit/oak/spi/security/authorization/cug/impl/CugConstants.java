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
package org.apache.jackrabbit.oak.spi.security.authorization.cug.impl;

/**
 * Constants for the Closed User Group (CUG) feature.
 */
interface CugConstants {

    /**
     * The name of the mixin type that defines the CUG policy node.
     */
    String MIX_REP_CUG_MIXIN = "rep:CugMixin";

    /**
     * The primary node type name of the CUG policy node.
     */
    String NT_REP_CUG_POLICY = "rep:CugPolicy";

    /**
     * The name of the CUG policy node.
     */
    String REP_CUG_POLICY = "rep:cugPolicy";

    /**
     * The name of the hidden property that stores information about nested
     * CUG policy nodes.
     */
    String HIDDEN_NESTED_CUGS = ":nestedCugs";

    /**
     * The name of the hidden property that stores information about the number
     * of CUG roots located close to the root node.
     */
    String HIDDEN_TOP_CUG_CNT = ":topCugCnt";

    /**
     * The name of the property that stores the principal names that are allowed
     * to access the restricted area defined by the CUG (closed user group).
     */
    String REP_PRINCIPAL_NAMES = "rep:principalNames";

    /**
     * Name of the configuration option that specifies the subtrees that allow
     * to define closed user groups.
     *
     * <ul>
     *     <li>Value Type: String</li>
     *     <li>Default: -</li>
     *     <li>Multiple: true</li>
     * </ul>
     */
    String PARAM_CUG_SUPPORTED_PATHS = "cugSupportedPaths";

    /**
     * Name of the configuration option that specifies if CUG content must
     * be respected for permission evaluation.
     *
     * <ul>
     *     <li>Value Type: boolean</li>
     *     <li>Default: false</li>
     *     <li>Multiple: false</li>
     * </ul>
     */
    String PARAM_CUG_ENABLED = "cugEnabled";
}