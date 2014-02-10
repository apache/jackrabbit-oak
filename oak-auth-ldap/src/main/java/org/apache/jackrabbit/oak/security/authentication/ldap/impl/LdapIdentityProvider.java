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

import java.io.IOException;
import java.util.Map;

import javax.annotation.Nonnull;
import javax.jcr.Credentials;
import javax.security.auth.login.LoginException;

import org.apache.directory.api.ldap.model.cursor.CursorException;
import org.apache.directory.api.ldap.model.cursor.SearchCursor;
import org.apache.directory.api.ldap.model.entry.Attribute;
import org.apache.directory.api.ldap.model.entry.Entry;
import org.apache.directory.api.ldap.model.exception.LdapException;
import org.apache.directory.api.ldap.model.exception.LdapInvalidAttributeValueException;
import org.apache.directory.api.ldap.model.message.Response;
import org.apache.directory.api.ldap.model.message.SearchRequest;
import org.apache.directory.api.ldap.model.message.SearchRequestImpl;
import org.apache.directory.api.ldap.model.message.SearchResultEntry;
import org.apache.directory.api.ldap.model.message.SearchScope;
import org.apache.directory.api.ldap.model.name.Dn;
import org.apache.directory.ldap.client.api.LdapConnection;
import org.apache.directory.ldap.client.api.LdapNetworkConnection;
import org.apache.felix.scr.annotations.Activate;
import org.apache.felix.scr.annotations.Component;
import org.apache.felix.scr.annotations.ConfigurationPolicy;
import org.apache.felix.scr.annotations.Service;
import org.apache.jackrabbit.oak.spi.security.ConfigurationParameters;
import org.apache.jackrabbit.oak.spi.security.authentication.external.ExternalGroup;
import org.apache.jackrabbit.oak.spi.security.authentication.external.ExternalIdentity;
import org.apache.jackrabbit.oak.spi.security.authentication.external.ExternalIdentityException;
import org.apache.jackrabbit.oak.spi.security.authentication.external.ExternalIdentityProvider;
import org.apache.jackrabbit.oak.spi.security.authentication.external.ExternalIdentityRef;
import org.apache.jackrabbit.oak.spi.security.authentication.external.ExternalUser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@code LdapIdentityProvider} implements an external identity provider that reads users and groups from an ldap
 * source.
 *
 * Please refer to {@link LdapProviderConfig} for configuration options.
 */
@Component(
        // note that the metatype information is generated from LdapProviderConfig
        policy = ConfigurationPolicy.REQUIRE
)
@Service
public class LdapIdentityProvider implements ExternalIdentityProvider {

    /**
     * default logger
     */
    private static final Logger log = LoggerFactory.getLogger(LdapIdentityProvider.class);

    /**
     * internal configuration
     */
    private LdapProviderConfig config;

    /**
     * Default constructor for OSGi
     */
    public LdapIdentityProvider() {
    }

    /**
     * Constructor for non-OSGi cases.
     * @param config the configuration
     */
    public LdapIdentityProvider(LdapProviderConfig config) {
        this.config = config;
    }

    @Activate
    private void activate(Map<String, Object> properties) {
        ConfigurationParameters cfg = ConfigurationParameters.of(properties);
        config = LdapProviderConfig.of(cfg);
    }

    @Nonnull
    @Override
    public String getName() {
        return config.getName();
    }

    @Override
    public ExternalIdentity getIdentity(@Nonnull ExternalIdentityRef ref) throws ExternalIdentityException {
        if (!isMyRef(ref)) {
            return null;
        }

        LdapConnection connection = connect();
        try {
            Entry entry = connection.lookup(ref.getId(), "*");
            if (entry.hasObjectClass(config.getUserConfig().getObjectClasses())) {
                return createUser(entry, null);
            } else if (entry.hasObjectClass(config.getGroupConfig().getObjectClasses())) {
                return createGroup(entry, null);
            } else {
                log.warn("referenced identity is neither user or group: {}", ref.getString());
                return null;
            }
        } catch (LdapException e) {
            log.error("Error during ldap lookup", e);
            throw new ExternalIdentityException("Error during ldap lookup.", e);
        } finally {
            disconnect(connection);
        }
    }

    @Override
    public ExternalUser getUser(@Nonnull String userId) throws ExternalIdentityException {
        LdapConnection connection = connect();
        try {
            Entry entry = getEntry(connection, config.getUserConfig(), userId);
            if (entry != null) {
                return createUser(entry, userId);
            } else {
                return null;
            }
        } catch (LdapException e) {
            log.error("Error during ldap lookup", e);
            throw new ExternalIdentityException("Error during ldap lookup.", e);
        } catch (CursorException e) {
            log.error("Error during ldap lookup", e);
            throw new ExternalIdentityException("Error during ldap lookup.", e);
        } finally {
            disconnect(connection);
        }
    }

    @Override
    public ExternalGroup getGroup(@Nonnull String name) throws ExternalIdentityException {
        LdapConnection connection = connect();
        try {
            Entry entry = getEntry(connection, config.getGroupConfig(), name);
            if (entry != null) {
                return createGroup(entry, name);
            } else {
                return null;
            }
        } catch (LdapException e) {
            log.error("Error during ldap lookup", e);
            throw new ExternalIdentityException("Error during ldap lookup.", e);
        } catch (CursorException e) {
            log.error("Error during ldap lookup", e);
            throw new ExternalIdentityException("Error during ldap lookup.", e);
        } finally {
            disconnect(connection);
        }
    }

    private Entry getEntry(LdapConnection connection, LdapProviderConfig.Identity idConfig, String id)
            throws CursorException, LdapException {
        String searchFilter = idConfig.getSearchFilter(id);

        // Create the SearchRequest object
        SearchRequest req = new SearchRequestImpl();
        req.setScope(SearchScope.SUBTREE);
        req.addAttributes("*");
        req.setTimeLimit(config.getSearchTimeout());
        req.setBase(new Dn(idConfig.getBaseDN()));
        req.setFilter(searchFilter);

        log.debug("Searching entries below {} with {}", idConfig.getBaseDN(), searchFilter);

        // Process the request
        SearchCursor searchCursor = connection.search(req);
        while (searchCursor.next()) {
            Response response = searchCursor.get();

            // process the SearchResultEntry
            if (response instanceof SearchResultEntry) {
                Entry resultEntry = ((SearchResultEntry) response).getEntry();
                if (searchCursor.next()) {
                    log.warn("search for {} returned more than one entry. discarding additional ones.", searchFilter);
                }
                return resultEntry;
            }
        }
        return null;
    }

    private ExternalUser createUser(Entry e, String id)
            throws LdapInvalidAttributeValueException {
        ExternalIdentityRef ref = new ExternalIdentityRef(e.getDn().getName(), this.getName());
        if (id == null) {
            id = e.get(config.getUserConfig().getIdAttribute()).getString();
        }
        LdapUser user = new LdapUser(this, ref, id, null);
        Map<String, Object> props = user.getProperties();
        for (Attribute attr: e.getAttributes()) {
            if (attr.isHumanReadable()) {
                props.put(attr.getId(), attr.getString());
            }
        }
        return user;
    }

    private ExternalGroup createGroup(Entry e, String name)
            throws LdapInvalidAttributeValueException {
        ExternalIdentityRef ref = new ExternalIdentityRef(e.getDn().getName(), this.getName());
        if (name == null) {
            name = e.get(config.getGroupConfig().getIdAttribute()).getString();
        }
        LdapGroup group = new LdapGroup(this, ref, name);
        Map<String, Object> props = group.getProperties();
        for (Attribute attr: e.getAttributes()) {
            if (attr.isHumanReadable()) {
                props.put(attr.getId(), attr.getString());
            }
        }
        return group;

    }

    private LdapConnection connect() throws ExternalIdentityException {
        try {
            LdapConnection connection = new LdapNetworkConnection(config.getHostname(), config.getPort(), config.useSSL());
            if (config.getBindDN().length() > 0) {
                connection.bind(config.getBindDN(), config.getBindPassword());
            } else {
                connection.bind();
            }
            return connection;
        } catch (LdapException e) {
            log.error("Error while connecting to the ldap server.", e);
            throw new ExternalIdentityException("Error while connecting and binding to the ldap server", e);
        }
    }

    private void disconnect(LdapConnection connection) throws ExternalIdentityException {
        try {
            connection.unBind();
        } catch (LdapException e) {
            log.error("Error while unbinding from the ldap server.", e);
        }
        try {
            connection.close();
        } catch (IOException e) {
            log.error("Error while disconnecting from the ldap server.", e);
        }
    }

    @Override
    public ExternalUser authenticate(@Nonnull Credentials credentials) throws ExternalIdentityException, LoginException {
        return null;
    }

    private boolean isMyRef(ExternalIdentityRef ref) {
        final String refProviderName = ref.getProviderName();
        return refProviderName == null || refProviderName.length() == 0 || getName().equals(refProviderName);
    }

}