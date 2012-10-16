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
package org.apache.jackrabbit.oak.security.authentication;

import java.security.Principal;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nonnull;

import org.apache.jackrabbit.oak.api.AuthInfo;

/**
 * Default implementation of the AuthInfo interface.
 */
public class AuthInfoImpl implements AuthInfo {

    private final String userID;
    private final Map<String,?> attributes;
    private final Set<Principal> principals;

    public AuthInfoImpl(String userID, Map<String, ?> attributes, Set<? extends Principal> principals) {
        this.userID = userID;
        this.attributes = (attributes == null) ? Collections.<String, Object>emptyMap() : attributes;
        this.principals = Collections.unmodifiableSet(principals);
    }

    //-----------------------------------------------------------< AuthInfo >---
    @Override
    public String getUserID() {
        return userID;
    }

    @Nonnull
    @Override
    public String[] getAttributeNames() {
        return attributes.keySet().toArray(new String[attributes.size()]);
    }

    @Override
    public Object getAttribute(String attributeName) {
        return attributes.get(attributeName);
    }

    @Override
    public Set<Principal> getPrincipals() {
        return principals;
    }
}