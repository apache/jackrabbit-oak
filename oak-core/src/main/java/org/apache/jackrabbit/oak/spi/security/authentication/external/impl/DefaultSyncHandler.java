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

import java.security.Principal;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import javax.jcr.PropertyType;
import javax.jcr.RepositoryException;
import javax.jcr.Value;
import javax.jcr.ValueFactory;
import javax.jcr.ValueFormatException;

import org.apache.felix.scr.annotations.Activate;
import org.apache.felix.scr.annotations.Component;
import org.apache.felix.scr.annotations.ConfigurationPolicy;
import org.apache.felix.scr.annotations.Service;
import org.apache.jackrabbit.api.security.user.Authorizable;
import org.apache.jackrabbit.api.security.user.Group;
import org.apache.jackrabbit.api.security.user.User;
import org.apache.jackrabbit.api.security.user.UserManager;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.plugins.value.ValueFactoryImpl;
import org.apache.jackrabbit.oak.spi.security.ConfigurationParameters;
import org.apache.jackrabbit.oak.spi.security.authentication.external.ExternalGroup;
import org.apache.jackrabbit.oak.spi.security.authentication.external.ExternalIdentity;
import org.apache.jackrabbit.oak.spi.security.authentication.external.ExternalIdentityProvider;
import org.apache.jackrabbit.oak.spi.security.authentication.external.ExternalUser;
import org.apache.jackrabbit.oak.spi.security.authentication.external.SyncContext;
import org.apache.jackrabbit.oak.spi.security.authentication.external.SyncException;
import org.apache.jackrabbit.oak.spi.security.authentication.external.SyncHandler;
import org.apache.jackrabbit.oak.spi.security.principal.PrincipalImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@code DefaultSyncHandler} implements an sync handler that synchronizes users and groups from an external identity
 * provider with the repository users.
 * <p/>
 * Please refer to {@link DefaultSyncConfig} for configuration options.
 */
@Component(
        // note that the metatype information is generated from DefaultSyncConfig
        policy = ConfigurationPolicy.REQUIRE
)
@Service
public class DefaultSyncHandler implements SyncHandler {

    /**
     * logger instance
     */
    private static final Logger log = LoggerFactory.getLogger(DefaultSyncHandler.class);

    /**
     * internal configuration
     */
    private DefaultSyncConfig config;

    /**
     * Default constructor for OSGi
     */
    public DefaultSyncHandler() {
    }

    /**
     * Constructor for non-OSGi cases.
     *
     * @param config the configuration
     */
    public DefaultSyncHandler(DefaultSyncConfig config) {
        this.config = config;
    }

    @Activate
    private void activate(Map<String, Object> properties) {
        ConfigurationParameters cfg = ConfigurationParameters.of(properties);
        config = DefaultSyncConfig.of(cfg);
    }

    @Nonnull
    @Override
    public String getName() {
        return config.getName();
    }

    @Nonnull
    @Override
    public SyncContext createContext(@Nonnull ExternalIdentityProvider idp, @Nonnull UserManager userManager, @Nonnull Root root) throws SyncException {
        return new ContextImpl(idp, userManager, root);
    }

    private class ContextImpl implements SyncContext {

        private final ExternalIdentityProvider idp;

        private final UserManager userManager;

        private final Root root;

        private final ValueFactory valueFactory;

        private ContextImpl(ExternalIdentityProvider idp, UserManager userManager, Root root) {
            this.idp = idp;
            this.userManager = userManager;
            this.root = root;
            valueFactory = new ValueFactoryImpl(root, NamePathMapper.DEFAULT);
        }

        @Override
        public void close() {
            // nothing to do
        }

        @Override
        public boolean sync(@Nonnull ExternalIdentity identity) throws SyncException {
            try {
                if (identity instanceof ExternalUser) {
                    User user = getUser(identity);
                    if (user == null) {
                        createUser((ExternalUser) identity);
                    } else {
                        updateUser((ExternalUser) identity, user);
                    }
                    return true;
                } else if (identity instanceof ExternalGroup) {
                    // todo
                    return false;

                } else {
                    throw new IllegalArgumentException("identity must be user or group but was: " + identity);
                }
            } catch (RepositoryException e) {
                throw new SyncException(e);
            }
        }

        @CheckForNull
        private User getUser(@Nonnull ExternalIdentity externalUser) throws RepositoryException {
            Authorizable authorizable = userManager.getAuthorizable(externalUser.getId());
            if (authorizable == null) {
                authorizable = userManager.getAuthorizable(externalUser.getPrincipalName());
            }
            if (authorizable == null) {
                return null;
            } else if (authorizable instanceof User) {
                return (User) authorizable;
            } else {
                // TODO: deal with colliding authorizable that is group.
                log.warn("unexpected authorizable: {}", authorizable);
                return null;
            }
        }

        @CheckForNull
        private User createUser(ExternalUser externalUser) throws RepositoryException, SyncException {
            String password = externalUser.getPassword(); // todo: make configurable
            Principal principal = new PrincipalImpl(externalUser.getPrincipalName());
            User user = userManager.createUser(
                    externalUser.getId(),
                    password,
                    principal,
                    concatPaths(config.user().getPathPrefix(), externalUser.getIntermediatePath())
            );
            syncAuthorizable(externalUser, user);
            return user;
        }

        @CheckForNull
        private Group createGroup(ExternalGroup externalGroup) throws RepositoryException, SyncException {
            Principal principal = new PrincipalImpl(externalGroup.getPrincipalName());
            Group group = userManager.createGroup(
                    externalGroup.getId(),
                    principal,
                    concatPaths(config.user().getPathPrefix(), externalGroup.getIntermediatePath()));
            syncAuthorizable(externalGroup, group);
            return group;
        }

        private void updateUser(ExternalUser externalUser, User user) throws RepositoryException, SyncException {
            syncAuthorizable(externalUser, user);
        }

        private void syncAuthorizable(ExternalIdentity externalUser, Authorizable authorizable) throws RepositoryException, SyncException {
            for (ExternalGroup externalGroup : externalUser.getGroups()) {
                String groupId = externalGroup.getId();
                Group group;
                Authorizable a = userManager.getAuthorizable(groupId);
                if (a == null) {
                    group = createGroup(externalGroup);
                } else {
                    group = (a.isGroup()) ? (Group) a : null;
                }

                if (group != null) {
                    group.addMember(authorizable);
                } else {
                    log.debug("No such group " + groupId + "; Ignoring group membership.");
                }
            }

            Map<String, ?> properties = externalUser.getProperties();
            for (String key : properties.keySet()) {
                Object prop = properties.get(key);
                if (prop instanceof Collection) {
                    Value[] values = createValues((Collection) prop);
                    if (values != null) {
                        authorizable.setProperty(key, values);
                    }
                } else {
                    Value value = createValue(prop);
                    if (value != null) {
                        authorizable.setProperty(key, value);
                    }
                }
            }
        }

        @CheckForNull
        private Value createValue(Object propValue) throws ValueFormatException {
            int type = getType(propValue);
            if (type == PropertyType.UNDEFINED) {
                return null;
            } else {
                return valueFactory.createValue(propValue.toString(), type);
            }
        }

        @CheckForNull
        private Value[] createValues(Collection<?> propValues) throws ValueFormatException {
            List<Value> values = new ArrayList<Value>();
            for (Object obj : propValues) {
                Value v = createValue(obj);
                if (v != null) {
                    values.add(v);
                }
            }
            return values.toArray(new Value[values.size()]);
        }

        private int getType(Object propValue) {
            // TODO: add proper type detection
            if (propValue == null) {
                return PropertyType.UNDEFINED;
            } else {
                return PropertyType.STRING;
            }
        }

    }

    /**
     * Robust relative path concatenation.
     * @param paths relative paths
     * @return the concatenated path
     */
    private static String concatPaths(String ... paths) {
        StringBuilder result = new StringBuilder();
        for (String path: paths) {
            if (path != null && !path.isEmpty()) {
                int i0 = 0;
                int i1 = path.length();
                while (i0 < i1 && path.charAt(i0) == '/') {
                    i0++;
                }
                while (i1 > i0 && path.charAt(i1-1) == '/') {
                    i1--;
                }
                if (i1 > i0) {
                    if (result.length() > 0) {
                        result.append('/');
                    }
                    result.append(path.substring(i0, i1));
                }
            }
        }
        return result.length() == 0 ? null : result.toString();
    }
}