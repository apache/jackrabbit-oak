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

import org.apache.jackrabbit.guava.common.collect.Iterables;
import org.apache.jackrabbit.oak.spi.security.ConfigurationParameters;
import org.apache.jackrabbit.oak.spi.security.authentication.external.ExternalGroup;
import org.apache.jackrabbit.oak.spi.security.authentication.external.ExternalIdentity;
import org.apache.jackrabbit.oak.spi.security.authentication.external.ExternalIdentityProvider;
import org.apache.jackrabbit.oak.spi.security.authentication.external.ExternalIdentityRef;
import org.apache.jackrabbit.oak.spi.security.authentication.external.ExternalUser;
import org.apache.jackrabbit.util.Text;
import org.jetbrains.annotations.NotNull;
import org.osgi.service.component.annotations.Activate;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.ConfigurationPolicy;
import org.osgi.service.component.annotations.Modified;
import org.osgi.service.metatype.annotations.AttributeDefinition;
import org.osgi.service.metatype.annotations.Designate;
import org.osgi.service.metatype.annotations.ObjectClassDefinition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jcr.Credentials;
import javax.jcr.SimpleCredentials;
import javax.security.auth.login.LoginException;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

@Component(service = ExternalIdentityProvider.class, immediate = true, configurationPolicy = ConfigurationPolicy.REQUIRE)
@Designate(ocd = CustomExternalIdentityProvider.Configuration.class)
public class CustomExternalIdentityProvider implements ExternalIdentityProvider {

    @ObjectClassDefinition(name = "Apache Jackrabbit Oak CustomExternalIdentityProvider (Oak Exercises)")
    @interface Configuration {

        @AttributeDefinition(
                name = "External Identities",
                description = "Define external identities in the format: userid [, groupids], where groupids = groupid [, groupids]",
                cardinality = Integer.MAX_VALUE
        )
        String externalidentities() default "testUser,a,b,c";
    }

    private static final Logger log = LoggerFactory.getLogger(CustomExternalIdentityProvider.class);

    private Map<String, Set<String>> userGroupMap = new HashMap<>();
    private Set<String> groupIds = new HashSet<>();

    public CustomExternalIdentityProvider() {};


    //----------------------------------------------------< SCR integration >---
    @SuppressWarnings("UnusedDeclaration")
    @Activate
    public void activate(Map<String, Object> properties) {
        ConfigurationParameters config = ConfigurationParameters.of(properties);
        for (String entry : config.getConfigValue("externalidentities", Collections.<String>emptySet())) {
            String[] strs = Text.explode(entry, ',', false);
            String uid = strs[0].trim();
            Set<String> declaredGroups = new HashSet<>();
            if (strs.length > 1) {
                for (int i = 1; i < strs.length; i++) {
                    groupIds.add(strs[i]);
                    declaredGroups.add(strs[i]);

                }
            }
            userGroupMap.put(uid, declaredGroups);
        }
        log.info("activated IDP: {}", getName());
    }

    @SuppressWarnings("UnusedDeclaration")
    @Modified
    public void modified(Map<String, Object> properties) {
        activate(properties);
        log.info("modified IDP: {}", getName());
    }

    @NotNull
    @Override
    public String getName() {
        return "CustomExternalIdentityProvider";
    }

    @Override
    public ExternalIdentity getIdentity(@NotNull ExternalIdentityRef ref) {
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
    public ExternalUser getUser(@NotNull final String userId) {
        if (userGroupMap.containsKey(userId)) {
            return new ExternalUser() {

                @NotNull
                @Override
                public ExternalIdentityRef getExternalId() {
                    return new ExternalIdentityRef(userId, getName());
                }

                @NotNull
                @Override
                public String getId() {
                    return userId;
                }

                @NotNull
                @Override
                public String getPrincipalName() {
                    return "p_" + getExternalId().getString();
                }

                @Override
                public String getIntermediatePath() {
                    return null;
                }

                @NotNull
                @Override
                public Iterable<ExternalIdentityRef> getDeclaredGroups() {
                    Set<String> groupIds = userGroupMap.get(userId);
                    if (groupIds == null || groupIds.isEmpty()) {
                        return Set.of();
                    } else {
                        return Iterables.transform(groupIds,
                                input -> new ExternalIdentityRef(input, getName()));
                    }
                }

                @NotNull
                @Override
                public Map<String, ?> getProperties() {
                    return Map.of();
                }
            };
        } else {
            return null;
        }
    }

    @Override
    public ExternalUser authenticate(@NotNull Credentials credentials) throws LoginException {
        if (credentials instanceof SimpleCredentials) {
            String userId = ((SimpleCredentials) credentials).getUserID();
            return getUser(userId);
        } else {
            throw new LoginException("unsupported credentials");
        }
    }

    @Override
    public ExternalGroup getGroup(@NotNull final String name) {
        if (groupIds.contains(name)) {
            return new ExternalGroup() {
                @NotNull
                @Override
                public Iterable<ExternalIdentityRef> getDeclaredMembers() {
                    Set<ExternalIdentityRef> members = new HashSet<>();
                    for (Map.Entry<String, Set<String>> entry : userGroupMap.entrySet()) {
                        if (entry.getValue().contains(name)) {
                            members.add(new ExternalIdentityRef(entry.getKey(), getName()));
                        }
                    }
                    return members;
                }

                @NotNull
                @Override
                public ExternalIdentityRef getExternalId() {
                    return new ExternalIdentityRef(name, getName());
                }

                @NotNull
                @Override
                public String getId() {
                    return name;
                }

                @NotNull
                @Override
                public String getPrincipalName() {
                    return "p_" + getExternalId().getString();
                }

                @Override
                public String getIntermediatePath() {
                    return null;
                }

                @NotNull
                @Override
                public Iterable<ExternalIdentityRef> getDeclaredGroups() {
                    return Set.of();
                }

                @NotNull
                @Override
                public Map<String, ?> getProperties() {
                    return Map.of();
                }
            };
        } else {
            return null;
        }
    }

    @NotNull
    @Override
    public Iterator<ExternalUser> listUsers() {
        throw new UnsupportedOperationException("listUsers");
    }

    @NotNull
    @Override
    public Iterator<ExternalGroup> listGroups() {
        throw new UnsupportedOperationException("listGroups");
    }
}
