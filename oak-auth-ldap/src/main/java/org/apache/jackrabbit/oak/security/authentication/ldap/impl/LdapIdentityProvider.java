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

import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.jcr.Credentials;
import javax.jcr.SimpleCredentials;
import javax.security.auth.login.LoginException;

import org.apache.commons.pool.impl.GenericObjectPool;
import org.apache.directory.api.ldap.model.constants.SchemaConstants;
import org.apache.directory.api.ldap.model.cursor.CursorException;
import org.apache.directory.api.ldap.model.cursor.SearchCursor;
import org.apache.directory.api.ldap.model.entry.Attribute;
import org.apache.directory.api.ldap.model.entry.Entry;
import org.apache.directory.api.ldap.model.entry.Value;
import org.apache.directory.api.ldap.model.exception.LdapAuthenticationException;
import org.apache.directory.api.ldap.model.exception.LdapException;
import org.apache.directory.api.ldap.model.exception.LdapInvalidAttributeValueException;
import org.apache.directory.api.ldap.model.message.Response;
import org.apache.directory.api.ldap.model.message.SearchRequest;
import org.apache.directory.api.ldap.model.message.SearchRequestImpl;
import org.apache.directory.api.ldap.model.message.SearchResultEntry;
import org.apache.directory.api.ldap.model.message.SearchScope;
import org.apache.directory.api.ldap.model.name.Dn;
import org.apache.directory.api.ldap.model.name.Rdn;
import org.apache.directory.ldap.client.api.LdapConnection;
import org.apache.directory.ldap.client.api.LdapConnectionConfig;
import org.apache.directory.ldap.client.api.LdapConnectionPool;
import org.apache.directory.ldap.client.api.NoVerificationTrustManager;
import org.apache.directory.ldap.client.api.PoolableLdapConnectionFactory;
import org.apache.felix.scr.annotations.Activate;
import org.apache.felix.scr.annotations.Component;
import org.apache.felix.scr.annotations.ConfigurationPolicy;
import org.apache.felix.scr.annotations.Deactivate;
import org.apache.felix.scr.annotations.Service;
import org.apache.jackrabbit.commons.iterator.AbstractLazyIterator;
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
     * the connection pool with connections authenticated with the bind DN
     */
    private LdapConnectionPool adminPool;

    /**
     * admin connection factory
     */
    private PoolableLdapConnectionFactory adminConnectionFactory;

    /**
     * the connection pool with unbound connections
     */
    private UnboundLdapConnectionPool userPool;

    /**
     * user connection factory
     */
    private PoolableUnboundConnectionFactory userConnectionFactory;


    /**
     * Default constructor for OSGi
     */
    @SuppressWarnings("UnusedDeclaration")
    public LdapIdentityProvider() {
    }

    /**
     * Constructor for non-OSGi cases.
     * @param config the configuration
     */
    public LdapIdentityProvider(@Nonnull LdapProviderConfig config) {
        this.config = config;
        init();
    }

    //----------------------------------------------------< SCR integration >---
    @SuppressWarnings("UnusedDeclaration")
    @Activate
    private void activate(Map<String, Object> properties) {
        ConfigurationParameters cfg = ConfigurationParameters.of(properties);
        config = LdapProviderConfig.of(cfg);
        init();
    }

    @SuppressWarnings("UnusedDeclaration")
    @Deactivate
    private void deactivate() {
        close();
    }

    /**
     * Closes this provider and releases the internal pool. This should be called by Non-OSGi users of this provider.
     */
    public void close() {
        if (adminPool != null) {
            try {
                adminPool.close();
            } catch (Exception e) {
                log.warn("Error while closing LDAP connection pool", e);
            }
            adminPool = null;
        }
        if (userPool != null) {
            try {
                userPool.close();
            } catch (Exception e) {
                log.warn("Error while closing LDAP connection pool", e);
            }
            userPool = null;
        }
    }


    //-------------------------------------------< ExternalIdentityProvider >---
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
            if (entry == null) {
                return null;
            } else if (entry.hasObjectClass(config.getUserConfig().getObjectClasses())) {
                return createUser(entry, null);
            } else if (entry.hasObjectClass(config.getGroupConfig().getObjectClasses())) {
                return createGroup(entry, null);
            } else {
                log.warn("referenced identity is neither user or group: {}", ref.getString());
                return null;
            }
        } catch (LdapException e) {
            throw lookupFailedException(e, null);
        } finally {
            disconnect(connection);
        }
    }

    @Override
    public ExternalUser getUser(@Nonnull String userId) throws ExternalIdentityException {
        DebugTimer timer = new DebugTimer();
        LdapConnection connection = connect();
        timer.mark("connect");
        try {
            Entry entry = getEntry(connection, config.getUserConfig(), userId);
            timer.mark("lookup");
            if (log.isDebugEnabled()) {
                log.debug("getUser({}) {}", userId, timer.getString());
            }
            if (entry != null) {
                return createUser(entry, userId);
            } else {
                return null;
            }
        } catch (LdapException e) {
            throw lookupFailedException(e, timer);
        } catch (CursorException e) {
            throw lookupFailedException(e, timer);
        } finally {
            disconnect(connection);
        }
    }

    @Override
    public ExternalGroup getGroup(@Nonnull String name) throws ExternalIdentityException {
        DebugTimer timer = new DebugTimer();
        LdapConnection connection = connect();
        timer.mark("connect");
        try {
            Entry entry = getEntry(connection, config.getGroupConfig(), name);
            timer.mark("lookup");
            if (log.isDebugEnabled()) {
                log.debug("getGroup({}) {}", name, timer.getString());
            }
            if (entry != null) {
                return createGroup(entry, name);
            } else {
                return null;
            }
        } catch (LdapException e) {
            throw lookupFailedException(e, timer);
        } catch (CursorException e) {
            throw lookupFailedException(e, timer);
        } finally {
            disconnect(connection);
        }
    }

    @Nonnull
    @Override
    public Iterator<ExternalUser> listUsers() throws ExternalIdentityException {
        DebugTimer timer = new DebugTimer();
        LdapConnection connection = connect();
        timer.mark("connect");
        try {
            final List<Entry> entries = getEntries(connection, config.getUserConfig());
            timer.mark("lookup");
            if (log.isDebugEnabled()) {
                log.debug("listUsers() {}", timer.getString());
            }
            return new AbstractLazyIterator<ExternalUser>() {

                private final Iterator<Entry> iter = entries.iterator();

                @Override
                protected ExternalUser getNext() {
                    while (iter.hasNext()) {
                        try {
                            return createUser(iter.next(), null);
                        } catch (LdapInvalidAttributeValueException e) {
                            log.warn("Error while creating external user object", e);
                        }
                    }
                    return null;
                }
            };
        } catch (LdapException e) {
            throw lookupFailedException(e, timer);
        } catch (CursorException e) {
            throw lookupFailedException(e, timer);
        } finally {
            disconnect(connection);
        }
    }

    @Nonnull
    @Override
    public Iterator<ExternalGroup> listGroups() throws ExternalIdentityException {
        DebugTimer timer = new DebugTimer();
        LdapConnection connection = connect();
        timer.mark("connect");
        try {
            final List<Entry> entries = getEntries(connection, config.getGroupConfig());
            timer.mark("lookup");
            if (log.isDebugEnabled()) {
                log.debug("listGroups() {}", timer.getString());
            }
            return new AbstractLazyIterator<ExternalGroup>() {

                private final Iterator<Entry> iter = entries.iterator();

                @Override
                protected ExternalGroup getNext() {
                    while (iter.hasNext()) {
                        try {
                            return createGroup(iter.next(), null);
                        } catch (LdapInvalidAttributeValueException e) {
                            log.warn("Error while creating external user object", e);
                        }
                    }
                    return null;
                }
            };
        } catch (LdapException e) {
            throw lookupFailedException(e, timer);
        } catch (CursorException e) {
            throw lookupFailedException(e, timer);
        } finally {
            disconnect(connection);
        }
    }

    @Override
    public ExternalUser authenticate(@Nonnull Credentials credentials) throws ExternalIdentityException, LoginException {
        if (!(credentials instanceof SimpleCredentials)) {
            log.debug("LDAP IDP can only authenticate SimpleCredentials.");
            return null;
        }
        final SimpleCredentials creds = (SimpleCredentials) credentials;
        final ExternalUser user = getUser(creds.getUserID());
        if (user != null) {
            // OAK-2078: check for non-empty passwords to avoid anonymous bind on weakly configured servers
            // see http://tools.ietf.org/html/rfc4513#section-5.1.1 for details.
            if (creds.getPassword().length == 0) {
                throw new LoginException("Refusing to authenticate against LDAP server: Empty passwords not allowed.");
            }

            // authenticate
            LdapConnection connection = null;
            try {
                DebugTimer timer = new DebugTimer();
                if (userPool == null) {
                    connection = userConnectionFactory.makeObject();
                } else {
                    connection = userPool.getConnection();
                }
                timer.mark("connect");
                connection.bind(user.getExternalId().getId(), new String(creds.getPassword()));
                timer.mark("bind");
                if (log.isDebugEnabled()) {
                    log.debug("authenticate({}) {}", user.getId(), timer.getString());
                }
            } catch (LdapAuthenticationException e) {
                throw new LoginException("Unable to authenticate against LDAP server: " + e.getMessage());
            } catch (Exception e) {
                throw new ExternalIdentityException("Error while binding user credentials", e);
            } finally {
                if (connection != null) {
                    try {
                        if (userPool == null) {
                            userConnectionFactory.destroyObject(connection);
                        } else {
                            userPool.releaseConnection(connection);
                        }
                    } catch (Exception e) {
                        // ignore
                    }
                }
            }
        }
        return user;
    }

    //-----------------------------------------------------------< internal >---
    /**
     * Collects the declared (direct) groups of an identity
     * @param ref reference to the identity
     * @return map of identities where the key is the DN of the LDAP entity
     */
    Map<String, ExternalIdentityRef> getDeclaredGroupRefs(ExternalIdentityRef ref) throws ExternalIdentityException {
        if (!isMyRef(ref)) {
            return Collections.emptyMap();
        }
        String searchFilter = config.getMemberOfSearchFilter(ref.getId());

        LdapConnection connection = null;
        SearchCursor searchCursor = null;
        try {
            // Create the SearchRequest object
            SearchRequest req = new SearchRequestImpl();
            req.setScope(SearchScope.SUBTREE);
            req.addAttributes(SchemaConstants.NO_ATTRIBUTE);
            req.setTimeLimit((int) config.getSearchTimeout());
            req.setBase(new Dn(config.getGroupConfig().getBaseDN()));
            req.setFilter(searchFilter);

            Map<String, ExternalIdentityRef> groups = new HashMap<String, ExternalIdentityRef>();
            DebugTimer timer = new DebugTimer();
            connection = connect();
            timer.mark("connect");

            searchCursor = connection.search(req);
            timer.mark("search");
            while (searchCursor.next()) {
                Response response = searchCursor.get();
                if (response instanceof SearchResultEntry) {
                    Entry resultEntry = ((SearchResultEntry) response).getEntry();
                    ExternalIdentityRef groupRef = new ExternalIdentityRef(resultEntry.getDn().toString(), this.getName());
                    groups.put(groupRef.getId(), groupRef);
                }
            }
            timer.mark("iterate");
            if (log.isDebugEnabled()) {
                log.debug("search below {} with {} found {} entries. {}",
                        config.getGroupConfig().getBaseDN(), searchFilter, groups.size(), timer.getString());
            }
            return groups;
        } catch (Exception e) {
            log.error("Error during ldap membership search." ,e);
            throw new ExternalIdentityException("Error during ldap membership search.", e);
        } finally {
            if (searchCursor != null) {
                searchCursor.close();
            }
            disconnect(connection);
        }
    }

    /**
     * Collects the declared (direct) members of a group
     * @param ref the reference to the group
     * @return map of identity refers
     * @throws ExternalIdentityException if an error occurs
     */
    Map<String, ExternalIdentityRef> getDeclaredMemberRefs(ExternalIdentityRef ref) throws ExternalIdentityException {
        if (!isMyRef(ref)) {
            return Collections.emptyMap();
        }
        LdapConnection connection = null;
        try {
            Map<String, ExternalIdentityRef> members = new HashMap<String, ExternalIdentityRef>();
            DebugTimer timer = new DebugTimer();
            connection = connect();
            timer.mark("connect");
            Entry entry = connection.lookup(ref.getId());
            timer.mark("lookup");
            Attribute attr = entry.get(config.getGroupMemberAttribute());
            for (Value value: attr) {
                ExternalIdentityRef memberRef = new ExternalIdentityRef(value.getString(), this.getName());
                members.put(memberRef.getId(), memberRef);
            }
            timer.mark("iterate");
            if (log.isDebugEnabled()) {
                log.debug("members lookup of {} found {} members. {}", ref.getId(), members.size(), timer.getString());
            }
            return members;
        } catch (Exception e) {
            String msg = "Error during ldap group members lookup.";
            log.error(msg ,e);
            throw new ExternalIdentityException(msg, e);
        } finally {
            disconnect(connection);
        }
    }

    //------------------------------------------------------------< private >---
    /**
     * Initializes the ldap identity provider.
     */
    private void init() {
        if (adminConnectionFactory != null) {
            throw new IllegalStateException("Provider already initialized.");
        }

        // setup admin connection pool
        LdapConnectionConfig cc = createConnectionConfig();
        String bindDN = config.getBindDN();
        if (bindDN != null && !bindDN.isEmpty()) {
            cc.setName(bindDN);
            cc.setCredentials(config.getBindPassword());
        }
        adminConnectionFactory = new PoolableLdapConnectionFactory(cc);

        if (config.getAdminPoolConfig().getMaxActive() != 0) {
            adminPool = new LdapConnectionPool(adminConnectionFactory);
            adminPool.setTestOnBorrow(true);
            adminPool.setMaxActive(config.getAdminPoolConfig().getMaxActive());
            adminPool.setWhenExhaustedAction(GenericObjectPool.WHEN_EXHAUSTED_BLOCK);
        }

        // setup unbound connection pool. let's create a new version of the config
        cc = createConnectionConfig();

        userConnectionFactory = new PoolableUnboundConnectionFactory(cc);
        if (config.getUserPoolConfig().getMaxActive() != 0) {
            userPool = new UnboundLdapConnectionPool(userConnectionFactory);
            userPool.setTestOnBorrow(true);
            userPool.setMaxActive(config.getUserPoolConfig().getMaxActive());
            userPool.setWhenExhaustedAction(GenericObjectPool.WHEN_EXHAUSTED_BLOCK);
        }

        log.info("LdapIdentityProvider initialized: {}", config);
    }

    /**
     * Creates a new connection config based on the config.
     * @return the connection config.
     */
    @Nonnull
    private LdapConnectionConfig createConnectionConfig() {
        LdapConnectionConfig cc = new LdapConnectionConfig();
        cc.setLdapHost(config.getHostname());
        cc.setLdapPort(config.getPort());
        cc.setUseSsl(config.useSSL());
        cc.setUseTls(config.useTLS());

        // todo: implement better trustmanager/keystore management (via sling/felix)
        if (config.noCertCheck()) {
            cc.setTrustManagers(new NoVerificationTrustManager());
        }
        return cc;
    }

    @CheckForNull
    private Entry getEntry(@Nonnull LdapConnection connection, @Nonnull LdapProviderConfig.Identity idConfig, @Nonnull String id)
            throws CursorException, LdapException {
        String searchFilter = idConfig.getSearchFilter(id);

        // Create the SearchRequest object
        SearchRequest req = new SearchRequestImpl();
        req.setScope(SearchScope.SUBTREE);
        req.addAttributes(SchemaConstants.ALL_USER_ATTRIBUTES);
        req.setTimeLimit((int) config.getSearchTimeout());
        req.setBase(new Dn(idConfig.getBaseDN()));
        req.setFilter(searchFilter);

        // Process the request
        SearchCursor searchCursor = null;
        try {
            searchCursor = connection.search(req);
            while (searchCursor.next()) {
                Response response = searchCursor.get();

                // process the SearchResultEntry
                if (response instanceof SearchResultEntry) {
                    Entry resultEntry = ((SearchResultEntry) response).getEntry();
                    if (searchCursor.next()) {
                        log.warn("search for {} returned more than one entry. discarding additional ones.", searchFilter);
                    }
                    if (log.isDebugEnabled()) {
                        log.debug("search below {} with {} found {}", idConfig.getBaseDN(), searchFilter, resultEntry.getDn());
                    }
                    return resultEntry;
                }
            }
        } finally {
            if (searchCursor != null) {
                searchCursor.close();
            }
        }
        if (log.isDebugEnabled()) {
            log.debug("search below {} with {} found 0 entries.", idConfig.getBaseDN(), searchFilter);
        }
        return null;
    }

    /**
     * currently fetch all entries so that we can close the connection afterwards. maybe switch to an iterator approach
     * later.
     */
    @Nonnull
    private List<Entry> getEntries(@Nonnull LdapConnection connection, @Nonnull LdapProviderConfig.Identity idConfig)
            throws CursorException, LdapException {
        StringBuilder filter = new StringBuilder();
        int num = 0;
        for (String objectClass: idConfig.getObjectClasses()) {
            num++;
            filter.append("(objectclass=")
                    .append(LdapProviderConfig.encodeFilterValue(objectClass))
                    .append(')');
        }
        String extraFilter = idConfig.getExtraFilter();
        if (extraFilter != null && !extraFilter.isEmpty()) {
            num++;
            filter.append(extraFilter);
        }
        String searchFilter = num > 1
                ? "(&" + filter + ')'
                : filter.toString();

        // Create the SearchRequest object
        SearchRequest req = new SearchRequestImpl();
        req.setScope(SearchScope.SUBTREE);
        req.addAttributes(SchemaConstants.ALL_USER_ATTRIBUTES);
        req.setTimeLimit((int) config.getSearchTimeout());
        req.setBase(new Dn(idConfig.getBaseDN()));
        req.setFilter(searchFilter);

        // Process the request
        List<Entry> result = new LinkedList<Entry>();
        SearchCursor searchCursor = null;
        try {
            searchCursor = connection.search(req);
            while (searchCursor.next()) {
                Response response = searchCursor.get();

                // process the SearchResultEntry
                if (response instanceof SearchResultEntry) {
                    Entry resultEntry = ((SearchResultEntry) response).getEntry();
                    result.add(resultEntry);
                    if (log.isDebugEnabled()) {
                        log.debug("search below {} with {} found {}", idConfig.getBaseDN(), searchFilter, resultEntry.getDn());
                    }
                }
            }
        } finally {
            if (searchCursor != null) {
                searchCursor.close();
            }
        }
        if (log.isDebugEnabled()) {
            log.debug("search below {} with {} found {} entries.", idConfig.getBaseDN(), searchFilter, result.size());
        }
        return result;
    }

    @Nonnull
    private ExternalUser createUser(@Nonnull Entry entry, @CheckForNull String id)
            throws LdapInvalidAttributeValueException {
        ExternalIdentityRef ref = new ExternalIdentityRef(entry.getDn().getName(), this.getName());
        if (id == null) {
            id = entry.get(config.getUserConfig().getIdAttribute()).getString();
        }
        String path = config.getUserConfig().makeDnPath()
                ? createDNPath(entry.getDn())
                : null;
        LdapUser user = new LdapUser(this, ref, id, path);
        Map<String, Object> props = user.getProperties();
        for (Attribute attr: entry.getAttributes()) {
            if (attr.isHumanReadable()) {
                props.put(attr.getId(), attr.getString());
            }
        }
        return user;
    }

    @Nonnull
    private ExternalGroup createGroup(@Nonnull Entry entry, @CheckForNull String name)
            throws LdapInvalidAttributeValueException {
        ExternalIdentityRef ref = new ExternalIdentityRef(entry.getDn().getName(), this.getName());
        if (name == null) {
            name = entry.get(config.getGroupConfig().getIdAttribute()).getString();
        }
        String path = config.getGroupConfig().makeDnPath()
                ? createDNPath(entry.getDn())
                : null;
        LdapGroup group = new LdapGroup(this, ref, name, path);
        Map<String, Object> props = group.getProperties();
        for (Attribute attr: entry.getAttributes()) {
            if (attr.isHumanReadable()) {
                props.put(attr.getId(), attr.getString());
            }
        }
        return group;

    }

    @Nonnull
    private LdapConnection connect() throws ExternalIdentityException {
        try {
            if (adminPool == null) {
                return adminConnectionFactory.makeObject();
            } else {
                return adminPool.getConnection();
            }
        } catch (Exception e) {
            String msg = "Error while connecting to the ldap server.";
            log.error(msg, e);
            throw new ExternalIdentityException(msg, e);
        }
    }

    private void disconnect(@Nullable LdapConnection connection) {
        try {
            if (connection != null) {
                if (adminPool == null) {
                    adminConnectionFactory.destroyObject(connection);
                } else {
                    adminPool.releaseConnection(connection);
                }
            }
        } catch (Exception e) {
            log.warn("Error while disconnecting from the ldap server.", e);
        }
    }

    private boolean isMyRef(@Nonnull ExternalIdentityRef ref) {
        final String refProviderName = ref.getProviderName();
        return refProviderName == null || refProviderName.isEmpty() || getName().equals(refProviderName);
    }

    /**
     * Makes the intermediate path of an DN by splitting along the RDNs
     * @param dn the dn of the identity
     * @return the intermediate path or {@code null} if disabled by config
     */
    private static String createDNPath(Dn dn) {
        StringBuilder path = new StringBuilder();
        for (Rdn rnd: dn.getRdns()) {
            if (path.length() > 0) {
                path.append('/');
            }
            path.append(Text.escapeIllegalJcrChars(rnd.toString()));
        }
        return path.toString();
    }

    private static ExternalIdentityException lookupFailedException(@Nonnull Exception e, @CheckForNull DebugTimer timer) {
        String msg = "Error during ldap lookup. ";
        log.error(msg + ((timer != null) ? timer.getString() : ""), e);
        return new ExternalIdentityException(msg, e);
    }
}