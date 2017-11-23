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
package org.apache.jackrabbit.oak.spi.security.authentication.token;

import java.util.Set;

import com.google.common.collect.ImmutableSet;
import org.apache.jackrabbit.oak.spi.nodetype.NodeTypeConstants;

public interface TokenConstants {

    /**
     * Constant for the token attribute passed with valid simple credentials to
     * trigger the generation of a new token.
     */
    String TOKEN_ATTRIBUTE = ".token";
    String TOKEN_ATTRIBUTE_EXPIRY = "rep:token.exp";
    String TOKEN_ATTRIBUTE_KEY = "rep:token.key";

    String TOKENS_NODE_NAME = ".tokens";
    String TOKENS_NT_NAME = NodeTypeConstants.NT_REP_UNSTRUCTURED;

    String TOKEN_NT_NAME = "rep:Token";

    Set<String> RESERVED_ATTRIBUTES = ImmutableSet.of(
            TOKEN_ATTRIBUTE,
            TOKEN_ATTRIBUTE_EXPIRY,
            TOKEN_ATTRIBUTE_KEY);

    Set<String> TOKEN_PROPERTY_NAMES = ImmutableSet.of(TOKEN_ATTRIBUTE_EXPIRY, TOKEN_ATTRIBUTE_KEY);
}