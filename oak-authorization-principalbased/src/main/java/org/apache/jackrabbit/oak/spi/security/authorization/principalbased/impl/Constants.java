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
package org.apache.jackrabbit.oak.spi.security.authorization.principalbased.impl;

interface Constants {

    /**
     * The name of the mixin type that defines the principal based access control policy node.
     */
    String MIX_REP_PRINCIPAL_BASED_MIXIN = "rep:PrincipalBasedMixin";

    /**
     * The primary node type name of the principal based access control policy node.
     */
    String NT_REP_PRINCIPAL_POLICY = "rep:PrincipalPolicy";

    /**
     * The primary node type name of the entries inside the principal based access control policy node.
     */
    String NT_REP_PRINCIPAL_ENTRY = "rep:PrincipalEntry";

    /**
     * The primary node type name of the restrictions node associated with entries inside the principal based access control policy node.
     */
    String NT_REP_RESTRICTIONS = "rep:Restrictions";

    /**
     * The name of the principal based access control policy node.
     */
    String REP_PRINCIPAL_POLICY = "rep:principalPolicy";

    /**
     * The name of the mandatory principal name property associated with the principal based access control policy.
     */
    String REP_PRINCIPAL_NAME = "rep:principalName";

    /**
     * The name of the mandatory path property of a given entry in a principal based access control policy.
     * It will store an absolute path or empty string for the repository-level
     */
    String REP_EFFECTIVE_PATH = "rep:effectivePath";

    /**
     * The name of the mandatory principal property of a given entry in a principal based access control policy.
     */
    String REP_PRIVILEGES = "rep:privileges";

    /**
     * The name of the optional restriction node associated with a given entry in a principal based access control policy.
     */
    String REP_RESTRICTIONS = "rep:restrictions";

    /**
     * Value to be used for the {@code rep:effectivePath} property in case of repository level permissions (analog to passing
     * null to {@code AccessControlManager.getEffectivePolicies(String)}.
     */
    String REPOSITORY_PERMISSION_PATH = "";
}