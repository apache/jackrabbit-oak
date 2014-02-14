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
package org.apache.jackrabbit.oak.security.authentication.ldap.impl;

import java.util.Map;

import javax.annotation.Nonnull;

import org.apache.jackrabbit.oak.spi.security.authentication.external.ExternalGroup;
import org.apache.jackrabbit.oak.spi.security.authentication.external.ExternalIdentityException;
import org.apache.jackrabbit.oak.spi.security.authentication.external.ExternalIdentityRef;

public class LdapGroup extends LdapIdentity implements ExternalGroup {

    private Map<String, ExternalIdentityRef> members;

    public LdapGroup(LdapIdentityProvider provider, ExternalIdentityRef ref, String id, String path) {
        super(provider, ref, id, path);
    }

    @Nonnull
    @Override
    public Iterable<ExternalIdentityRef> getDeclaredMembers() throws ExternalIdentityException {
        if (members == null) {
            members = provider.getDeclaredMemberRefs(ref);
        }
        return members.values();
    }
}
