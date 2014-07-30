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
package org.apache.jackrabbit.oak.security.authorization.cug.impl;

/**
 * CugConstants... TODO
 */
interface CugConstants {

    String MIX_REP_CUG_MIXIN = "rep:CugMixin";

    String NT_REP_CUG_POLICY = "rep:CugPolicy";

    String REP_CUG_POLICY = "rep:cugPolicy";

    String REP_PRINCIPAL_NAMES = "rep:principalNames";

    String PARAM_CUG_SUPPORTED_PATHS = "cugSupportedPaths";

    String PARAM_CUG_ENABLED = "cugEnabled";

    /**
     *
     */
    String PARAM_CUG_EXCLUDE = "cugExclude";

}