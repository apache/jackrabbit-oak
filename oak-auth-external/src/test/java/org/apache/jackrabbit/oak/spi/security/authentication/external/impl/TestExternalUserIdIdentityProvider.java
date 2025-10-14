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
package org.apache.jackrabbit.oak.spi.security.authentication.external.impl;

import org.apache.jackrabbit.oak.spi.security.authentication.AbstractLoginModule;
import org.apache.jackrabbit.oak.spi.security.authentication.credentials.AbstractCredentials;
import org.apache.jackrabbit.oak.spi.security.authentication.credentials.CredentialsSupport;
import org.apache.jackrabbit.oak.spi.security.authentication.external.ExternalGroup;
import org.apache.jackrabbit.oak.spi.security.authentication.external.ExternalIdentity;
import org.apache.jackrabbit.oak.spi.security.authentication.external.ExternalIdentityException;
import org.apache.jackrabbit.oak.spi.security.authentication.external.ExternalIdentityProvider;
import org.apache.jackrabbit.oak.spi.security.authentication.external.ExternalIdentityRef;
import org.apache.jackrabbit.oak.spi.security.authentication.external.ExternalUser;
import org.apache.jackrabbit.oak.spi.security.authentication.token.TokenConstants;
import org.jetbrains.annotations.NotNull;

import javax.jcr.Credentials;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

public class TestExternalUserIdIdentityProvider implements ExternalIdentityProvider, CredentialsSupport {
    
    private final String name;

    public TestExternalUserIdIdentityProvider(final String name) {
        this.name = name;
    }


    //-------------------------------------< ExternalIdentityProvider >---

    @Override
    public ExternalUser authenticate(@NotNull Credentials credentials) {
        if (credentials instanceof TestExternalUserIdCredentials) {
            TestExternalUserIdCredentials oAuthCredentials = (TestExternalUserIdCredentials) credentials;
            return new TestExternalUser(oAuthCredentials);
        }
        return null;
    }

    @Override
    public ExternalGroup getGroup(@NotNull String name) throws ExternalIdentityException {
        return null;
    }

    @Override
    public ExternalIdentity getIdentity(@NotNull ExternalIdentityRef ref) throws ExternalIdentityException {
        if (isForeignRef(ref)) {
            return null;
        } else if (ref instanceof TestGroupExternalIdentityRef) {
            return new TestExternalUserIdExternalGroup(ref);
        }
        
        return null;
    }

    @Override
    public @NotNull String getName() {
        return name;
    }

    @Override
    public ExternalUser getUser(@NotNull String userId) throws ExternalIdentityException {
        return null;
    }

    @Override
    public @NotNull Iterator<ExternalUser> listUsers() throws ExternalIdentityException {
        //return an empty iterator
        return Collections.emptyIterator();
    }

    @Override
    public @NotNull Iterator<ExternalGroup> listGroups() throws ExternalIdentityException {
        //return an empty iterator
        return Collections.emptyIterator();
    }

    //-----------------------------------------    PRIVATE METHODS---
    
    private boolean isForeignRef(ExternalIdentityRef ref) {
        if (ref == null) {
            return false;
        }
        
        String provider = ref.getProviderName();
        // the part that supports null or empty provider strings is taken from the LDAP idp code
        return !(provider == null || provider.isEmpty() || getName().equals(ref.getProviderName()));
    }
    
    //-----------------------------------------< CredentialsSupport >---

    @Override
    public @NotNull Set<Class> getCredentialClasses() {
        return Collections.singleton(TestExternalUserIdCredentials.class);
    }
    
    @Override
    public String getUserId(@NotNull Credentials credentials) {
        if (credentials instanceof TestExternalUserIdCredentials) {
            return ((TestExternalUserIdCredentials)credentials).getUserId();
        } else {
            return null;
        }
    }

    @Override
    public @NotNull Map<String, ?> getAttributes(@NotNull Credentials credentials) {
        if (credentials instanceof TestExternalUserIdCredentials) {
            HashMap<String, Object> attrs = new HashMap<>();
            attrs.put(TokenConstants.TOKEN_ATTRIBUTE, "");
            attrs.put(TokenConstants.EXTERNAL_ID_ATTRIBUTE, ((TestExternalUserIdCredentials) credentials).getAttribute(TokenConstants.EXTERNAL_ID_ATTRIBUTE));
            attrs.put(AbstractLoginModule.SHARED_KEY_LOGIN_NAME, ((TestExternalUserIdCredentials) credentials).getAttribute(AbstractLoginModule.SHARED_KEY_LOGIN_NAME));
            return attrs;
        } else {
            return Collections.emptyMap();
        }
    }

    @Override
    public boolean setAttributes(@NotNull Credentials credentials, @NotNull Map<String, ?> attributes) {
        if (credentials instanceof TestExternalUserIdCredentials) {
            ((TestExternalUserIdCredentials)credentials).setAttributes((Map<String, Object>) attributes);
            return true;
        } else {
            return false;
        }
    }
    
    //-----------------------------------------< OAuthExternalUser >---
    
    class TestExternalUser implements ExternalUser {
        
        final AbstractCredentials externalCredentials;

        TestExternalUser(AbstractCredentials externalCredentials) {
            this.externalCredentials = externalCredentials;
        }

        @Override
        public @NotNull ExternalIdentityRef getExternalId() {
            return new ExternalIdentityRef((String) Objects.requireNonNull(externalCredentials.getAttribute(TokenConstants.EXTERNAL_ID_ATTRIBUTE)),
                    getName());
        }

        @Override
        public @NotNull String getId() {
            return externalCredentials.getUserId();
        }

        @Override
        public @NotNull String getPrincipalName() {
            return externalCredentials.getUserId();
        }

        /* 
         * The intermediate path is extracted 
         * from the id. It tries to be backward 
         * compatible with the previous AEM 
         * OAuth implementation
         */
        @Override
        public String getIntermediatePath() {
            String id = getId();
            
            if (id.length() > 4) {
                id = id.substring(id.indexOf("-")+1);
                if (id.length() > 4) {
                  return id.substring(0,4);  
                } else {
                    return id;
                }
            } 
            return id;
        }

        @Override
        public @NotNull Iterable<ExternalIdentityRef> getDeclaredGroups()
                throws ExternalIdentityException {
            Iterable<ExternalIdentityRef> groups = getGroups();
            if (groups!= null) {
                List<ExternalIdentityRef> list = new ArrayList<>();
                for (ExternalIdentityRef ref:groups) {
                    list.add(new TestGroupExternalIdentityRef(ref.getId(), getName()));
                }
                return Collections.unmodifiableList(list);
            } else {
                return Collections.emptyList();
            }
        }

        @Override
        public @NotNull Map<String, ?> getProperties() {
            return externalCredentials.getAttributes();
        }
        
        Iterable<ExternalIdentityRef> getGroups() {
            Collection<?> values = getProperties().values();
            for (Object o: values) {
                if (o instanceof ExternalUser) {
                    ExternalUser externalUser = (ExternalUser) o;
                    if (getExternalId().getId().equals(externalUser.getExternalId().getId())) {
                        try {
                            return externalUser.getDeclaredGroups();
                        } catch (ExternalIdentityException e) {
                            return null;
                        }
                    }
                }
            }
            return null;
        }
    }
    
    //-----------------------------------------< OAuthExternalGroup >---
    
    static class TestExternalUserIdExternalGroup implements ExternalGroup {

        final ExternalIdentityRef ref;
        
        public TestExternalUserIdExternalGroup(ExternalIdentityRef ref) {
            this.ref = ref;
        }

        @Override
        public @NotNull ExternalIdentityRef getExternalId() {
            return ref;
        }

        @Override
        public @NotNull String getId() {
            return ref.getId();
        }

        @Override
        public @NotNull String getPrincipalName() {
            return ref.getId();
        }

        @Override
        public String getIntermediatePath() {
            return null;
        }

        @Override
        public @NotNull Iterable<ExternalIdentityRef> getDeclaredGroups()
                throws ExternalIdentityException {
            //not supporting nested groups for now
            return Collections.emptyList();
        }

        @Override
        public @NotNull Map<String, ?> getProperties() {
            return Collections.emptyMap();
        }

        @Override
        public @NotNull Iterable<ExternalIdentityRef> getDeclaredMembers() {
            return Collections.emptyList();
        }
    }
    
    static class TestGroupExternalIdentityRef extends ExternalIdentityRef {
        public TestGroupExternalIdentityRef(String id, String providerName) {
            super(id, providerName);
        }
    }

}
