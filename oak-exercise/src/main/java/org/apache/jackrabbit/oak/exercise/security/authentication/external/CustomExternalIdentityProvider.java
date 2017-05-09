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
package org.apache.jackrabbit.oak.exercise.security.authentication.external;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.jcr.Credentials;
import javax.jcr.SimpleCredentials;
import javax.security.auth.login.LoginException;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import org.apache.felix.scr.annotations.Activate;
import org.apache.felix.scr.annotations.Component;
import org.apache.felix.scr.annotations.Modified;
import org.apache.felix.scr.annotations.Properties;
import org.apache.felix.scr.annotations.Property;
import org.apache.felix.scr.annotations.Service;
import org.apache.jackrabbit.oak.spi.security.ConfigurationParameters;
import org.apache.jackrabbit.oak.spi.security.authentication.external.ExternalGroup;
import org.apache.jackrabbit.oak.spi.security.authentication.external.ExternalIdentity;
import org.apache.jackrabbit.oak.spi.security.authentication.external.ExternalIdentityException;
import org.apache.jackrabbit.oak.spi.security.authentication.external.ExternalIdentityProvider;
import org.apache.jackrabbit.oak.spi.security.authentication.external.ExternalIdentityRef;
import org.apache.jackrabbit.oak.spi.security.authentication.external.ExternalUser;
import org.apache.jackrabbit.util.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Component(metatype = true,
        label = "Apache Jackrabbit Oak CustomExternalIdentityProvider",
        immediate = true
)
@Service
@Properties({
        @Property(name = "externalidentities",
                label = "External Identities",
                value = "testUser,a,b,c",
                cardinality = Integer.MAX_VALUE)
})
public class CustomExternalIdentityProvider implements ExternalIdentityProvider {

    private static final Logger log = LoggerFactory.getLogger(CustomExternalIdentityProvider.class);

    private Map<String, Set<String>> userGroupMap = new HashMap<String, Set<String>>();
    private Set<String> groupIds = new HashSet<String>();

    public CustomExternalIdentityProvider() {};


    //----------------------------------------------------< SCR integration >---
    @SuppressWarnings("UnusedDeclaration")
    @Activate
    public void activate(Map<String, Object> properties) {
        ConfigurationParameters config = ConfigurationParameters.of(properties);
        for (String entry : config.getConfigValue("externalidentities", Collections.<String>emptySet())) {
            String[] strs = Text.explode(entry, ',', false);
            String uid = strs[0].trim();
            Set<String> declaredGroups = new HashSet<String>();
            if (strs.length > 1) {
                for (int i = 1; i < strs.length; i++) {
                    groupIds.add(strs[i]);
                    declaredGroups.add(strs[i]);

                }
            }
            userGroupMap.put(uid, declaredGroups);
        }
        log.info("activated IDP: " + getName());
    }

    @SuppressWarnings("UnusedDeclaration")
    @Modified
    public void modified(Map<String, Object> properties) {
        activate(properties);
        log.info("modified IDP: " + getName());
    }

    @Nonnull
    @Override
    public String getName() {
        return "CustomExternalIdentityProvider";
    }

    @Override
    public ExternalIdentity getIdentity(@Nonnull ExternalIdentityRef ref) throws ExternalIdentityException {
        if (getName().equals(ref.getProviderName())) {
            String id = ref.getId();
            ExternalIdentity ei = getUser(id);
            if (ei == null) {
                ei = getGroup(id);
            }
            return ei;
        } else {
            return null;
        }
    }

    @Override
    public ExternalUser getUser(@Nonnull final String userId) throws ExternalIdentityException {
        if (userGroupMap.containsKey(userId)) {
            return new ExternalUser() {

                @Nonnull
                @Override
                public ExternalIdentityRef getExternalId() {
                    return new ExternalIdentityRef(userId, getName());
                }

                @Nonnull
                @Override
                public String getId() {
                    return userId;
                }

                @Nonnull
                @Override
                public String getPrincipalName() {
                    return "p_" + getExternalId().getString();
                }

                @Override
                public String getIntermediatePath() {
                    return null;
                }

                @Nonnull
                @Override
                public Iterable<ExternalIdentityRef> getDeclaredGroups() throws ExternalIdentityException {
                    Set<String> groupIds = userGroupMap.get(userId);
                    if (groupIds == null || groupIds.isEmpty()) {
                        return ImmutableSet.of();
                    } else {
                        return Iterables.transform(groupIds, new Function<String, ExternalIdentityRef>() {
                            @Nullable
                            @Override
                            public ExternalIdentityRef apply(String input) {
                                return new ExternalIdentityRef(input, getName());
                            }
                        });
                    }
                }

                @Nonnull
                @Override
                public Map<String, ?> getProperties() {
                    return ImmutableMap.of();
                }
            };
        } else {
            return null;
        }
    }

    @Override
    public ExternalUser authenticate(@Nonnull Credentials credentials) throws ExternalIdentityException, LoginException {
        if (credentials instanceof SimpleCredentials) {
            String userId = ((SimpleCredentials) credentials).getUserID();
            return getUser(userId);
        } else {
            throw new LoginException("unsupported credentials");
        }
    }

    @Override
    public ExternalGroup getGroup(@Nonnull final String name) throws ExternalIdentityException {
        if (groupIds.contains(name)) {
            return new ExternalGroup() {
                @Nonnull
                @Override
                public Iterable<ExternalIdentityRef> getDeclaredMembers() throws ExternalIdentityException {
                    Set<ExternalIdentityRef> members = new HashSet<ExternalIdentityRef>();
                    for (Map.Entry<String, Set<String>> entry : userGroupMap.entrySet()) {
                        if (entry.getValue().contains(name)) {
                            members.add(new ExternalIdentityRef(entry.getKey(), getName()));
                        }
                    }
                    return members;
                }

                @Nonnull
                @Override
                public ExternalIdentityRef getExternalId() {
                    return new ExternalIdentityRef(name, getName());
                }

                @Nonnull
                @Override
                public String getId() {
                    return name;
                }

                @Nonnull
                @Override
                public String getPrincipalName() {
                    return "p_" + getExternalId().getString();
                }

                @Override
                public String getIntermediatePath() {
                    return null;
                }

                @Nonnull
                @Override
                public Iterable<ExternalIdentityRef> getDeclaredGroups() throws ExternalIdentityException {
                    return ImmutableSet.of();
                }

                @Nonnull
                @Override
                public Map<String, ?> getProperties() {
                    return ImmutableMap.of();
                }
            };
        } else {
            return null;
        }
    }

    @Nonnull
    @Override
    public Iterator<ExternalUser> listUsers() throws ExternalIdentityException {
        throw new UnsupportedOperationException("listUsers");
    }

    @Nonnull
    @Override
    public Iterator<ExternalGroup> listGroups() throws ExternalIdentityException {
        throw new UnsupportedOperationException("listGroups");
    }
}