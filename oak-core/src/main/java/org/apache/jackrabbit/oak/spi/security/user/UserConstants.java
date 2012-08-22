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
package org.apache.jackrabbit.oak.spi.security.user;

/**
 * UserConstants...
 */
public interface UserConstants {

    String NT_REP_AUTHORIZABLE = "rep:Authorizable";
    String NT_REP_AUTHORIZABLE_FOLDER = "rep:AuthorizableFolder";
    String NT_REP_USER = "rep:User";
    String NT_REP_GROUP = "rep:Group";
    String NT_REP_MEMBERS = "rep:Members";
    String REP_PRINCIPAL_NAME = "rep:principalName";
    String REP_AUTHORIZABLE_ID = "rep:authorizableId";
    String REP_PASSWORD = "rep:password";
    String REP_DISABLED = "rep:disabled";
    String REP_MEMBERS = "rep:members";
    String REP_IMPERSONATORS = "rep:impersonators";

    String DEFAULT_USER_PATH = "/rep:security/rep:authorizables/rep:users";
    String DEFAULT_GROUP_PATH = "/rep:security/rep:authorizables/rep:groups";
    int DEFAULT_DEPTH = 2;

    int SEARCH_TYPE_USER = 1;

    /**
     * Filter flag indicating that only <code>Group</code>s should be searched
     * and returned.
     */
    int SEARCH_TYPE_GROUP = 2;

    /**
     * Filter flag indicating that all <code>Authorizable</code>s should be
     * searched.
     */
    int SEARCH_TYPE_AUTHORIZABLE = 3;

}