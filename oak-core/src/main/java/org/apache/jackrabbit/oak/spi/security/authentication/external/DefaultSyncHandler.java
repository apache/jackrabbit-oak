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
package org.apache.jackrabbit.oak.spi.security.authentication.external;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import javax.annotation.CheckForNull;
import javax.jcr.PropertyType;
import javax.jcr.RepositoryException;
import javax.jcr.Value;
import javax.jcr.ValueFactory;
import javax.jcr.ValueFormatException;

import org.apache.jackrabbit.api.security.user.Authorizable;
import org.apache.jackrabbit.api.security.user.Group;
import org.apache.jackrabbit.api.security.user.User;
import org.apache.jackrabbit.api.security.user.UserManager;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.plugins.value.ValueFactoryImpl;
import org.apache.jackrabbit.oak.spi.security.ConfigurationParameters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * DefaultSyncHandler... TODO
 */
public class DefaultSyncHandler implements SyncHandler {

    /**
     * logger instance
     */
    private static final Logger log = LoggerFactory.getLogger(DefaultSyncHandler.class);

    private UserManager userManager;
    private Root root;
    private SyncMode mode;
    private ConfigurationParameters options;

    private ValueFactory valueFactory;

    private boolean initialized;

    @Override
    public boolean initialize(UserManager userManager, Root root, SyncMode mode,
                              ConfigurationParameters options) throws SyncException {
        if (userManager == null || root == null) {
            throw new SyncException("Error while initializing sync handler.");
        }
        this.userManager = userManager;
        this.root = root;
        this.mode = mode;
        this.options = (options == null) ? ConfigurationParameters.EMPTY : options;

        valueFactory = new ValueFactoryImpl(root.getBlobFactory(), NamePathMapper.DEFAULT);
        initialized = true;
        return true;
    }

    @Override
    public boolean sync(ExternalUser externalUser) throws SyncException {
        checkInitialized();
        try {
            User user = getUser(externalUser);
            if (user == null) {
                createUser(externalUser);
            } else {
                updateUser(externalUser, user);
            }
            return true;
        } catch (RepositoryException e) {
            throw new SyncException(e);
        }
    }

    //--------------------------------------------------------------------------
    private void checkInitialized() {
        if (!initialized) {
            throw new IllegalStateException("not initialized");
        }
    }

    @CheckForNull
    private User getUser(ExternalUser externalUser) throws RepositoryException {
        // TODO: deal with colliding authorizable that is group.

        Authorizable authorizable = userManager.getAuthorizable(externalUser.getId());
        if (authorizable == null) {
            authorizable = userManager.getAuthorizable(externalUser.getPrincipal());
        }

        return (authorizable == null) ? null : (User) authorizable;
    }

    @CheckForNull
    private User createUser(ExternalUser externalUser) throws RepositoryException, SyncException {
        if (mode.contains(SyncMode.MODE_CREATE_USER)) {
            User user = userManager.createUser(externalUser.getId(), externalUser.getPassword(), externalUser.getPrincipal(), null);
            syncAuthorizable(externalUser, user);
            return user;
        } else {
            return null;
        }
    }

    @CheckForNull
    private Group createGroup(ExternalGroup externalGroup) throws RepositoryException, SyncException {
        if (mode.contains(SyncMode.MODE_CREATE_GROUPS)) {
            Group group = userManager.createGroup(externalGroup.getId(), externalGroup.getPrincipal(), null);
            syncAuthorizable(externalGroup, group);
            return group;
        } else {
            return null;
        }
    }

    private void updateUser(ExternalUser externalUser, User user) throws RepositoryException, SyncException {
        if (mode.contains(SyncMode.MODE_UPDATE)) {
            syncAuthorizable(externalUser, user);
        }
    }

    private void syncAuthorizable(ExternalUser externalUser, Authorizable authorizable) throws RepositoryException, SyncException {
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

    private static int getType(Object propValue) {
        // TODO: add proper type detection
        if (propValue == null) {
            return PropertyType.UNDEFINED;
        } else {
            return PropertyType.STRING;
        }
    }
}