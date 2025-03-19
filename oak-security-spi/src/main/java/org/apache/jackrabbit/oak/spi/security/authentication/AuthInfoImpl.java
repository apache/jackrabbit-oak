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
package org.apache.jackrabbit.oak.spi.security.authentication;

import java.security.Principal;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.StringJoiner;

import javax.jcr.SimpleCredentials;
import javax.security.auth.Subject;

import org.apache.jackrabbit.guava.common.collect.ImmutableSet;
import org.apache.jackrabbit.oak.api.AuthInfo;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Default implementation of the AuthInfo interface.
 */
public final class AuthInfoImpl implements AuthInfo {

    private final String userID;
    private final Map<String,?> attributes;
    private final Set<Principal> principals;

    public AuthInfoImpl(@Nullable String userID, @Nullable Map<String, ?> attributes,
                        @Nullable Set<? extends Principal> principals) {
        this(userID, attributes, (Iterable) principals);
    }

    public AuthInfoImpl(@Nullable String userID, @Nullable Map<String, ?> attributes,
                        @Nullable Iterable<? extends Principal> principals) {
        this.userID = userID;
        this.attributes = (attributes == null) ? Collections.emptyMap() : attributes;
        this.principals = (principals == null) ? Collections.emptySet() : ImmutableSet.copyOf(principals);
    }

    public static AuthInfo createFromSubject(@NotNull Subject subject) {
        Set<AuthInfo> infoSet = subject.getPublicCredentials(AuthInfo.class);
        if (infoSet.isEmpty()) {
            Set<SimpleCredentials> scs = subject.getPublicCredentials(SimpleCredentials.class);
            String userId = (scs.isEmpty()) ? null : scs.iterator().next().getUserID();
            return new AuthInfoImpl(userId, null, subject.getPrincipals());
        } else {
            return infoSet.iterator().next();
        }
    }

    @Override
    public String toString() {
        return new StringJoiner(", ", AuthInfoImpl.class.getSimpleName() + "[", "]")
                .add("userID=" + userID)
                .add("attributes=" + attributes)
                .add("principals=" + principals)
                .toString();
    }

    //-----------------------------------------------------------< AuthInfo >---
    @Override
    public String getUserID() {
        return userID;
    }

    @NotNull
    @Override
    public String[] getAttributeNames() {
        return attributes.keySet().toArray(new String[0]);
    }

    @Override
    public Object getAttribute(String attributeName) {
        return attributes.get(attributeName);
    }

    @NotNull
    @Override
    public Set<Principal> getPrincipals() {
        return principals;
    }
}
