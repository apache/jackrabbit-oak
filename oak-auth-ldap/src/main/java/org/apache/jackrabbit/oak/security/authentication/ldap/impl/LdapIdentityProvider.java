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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

import javax.jcr.Credentials;
import javax.jcr.SimpleCredentials;
import javax.security.auth.login.LoginException;

import org.apache.commons.pool2.impl.DefaultPooledObject;
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
import org.apache.directory.api.ldap.model.message.ResultCodeEnum;
import org.apache.directory.api.ldap.model.message.SearchRequest;
import org.apache.directory.api.ldap.model.message.SearchRequestImpl;
import org.apache.directory.api.ldap.model.message.SearchResultDone;
import org.apache.directory.api.ldap.model.message.SearchResultEntry;
import org.apache.directory.api.ldap.model.message.SearchScope;
import org.apache.directory.api.ldap.model.message.controls.PagedResults;
import org.apache.directory.api.ldap.model.message.controls.PagedResultsImpl;
import org.apache.directory.api.ldap.model.name.Dn;
import org.apache.directory.api.ldap.model.name.Rdn;
import org.apache.directory.ldap.client.api.AbstractPoolableLdapConnectionFactory;
import org.apache.directory.ldap.client.api.DefaultLdapConnectionValidator;
import org.apache.directory.ldap.client.api.LdapConnection;
import org.apache.directory.ldap.client.api.LdapConnectionConfig;
import org.apache.directory.ldap.client.api.LdapConnectionPool;
import org.apache.directory.ldap.client.api.LookupLdapConnectionValidator;
import org.apache.directory.ldap.client.api.NoVerificationTrustManager;
import org.apache.directory.ldap.client.api.ValidatingPoolableLdapConnectionFactory;
import org.apache.felix.scr.annotations.Activate;
import org.apache.felix.scr.annotations.Component;
import org.apache.felix.scr.annotations.ConfigurationPolicy;
import org.apache.felix.scr.annotations.Deactivate;
import org.apache.felix.scr.annotations.Service;
import org.apache.jackrabbit.commons.iterator.AbstractLazyIterator;
import org.apache.jackrabbit.oak.commons.DebugTimer;
import org.apache.jackrabbit.oak.spi.security.ConfigurationParameters;
import org.apache.jackrabbit.oak.spi.security.authentication.external.ExternalGroup;
import org.apache.jackrabbit.oak.spi.security.authentication.external.ExternalIdentity;
import org.apache.jackrabbit.oak.spi.security.authentication.external.ExternalIdentityException;
import org.apache.jackrabbit.oak.spi.security.authentication.external.ExternalIdentityProvider;
import org.apache.jackrabbit.oak.spi.security.authentication.external.ExternalIdentityRef;
import org.apache.jackrabbit.oak.spi.security.authentication.external.ExternalUser;
import org.apache.jackrabbit.oak.spi.security.authentication.external.PrincipalNameResolver;
import org.apache.jackrabbit.util.Text;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
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
public class LdapIdentityProvider implements ExternalIdentityProvider, PrincipalNameResolver {

    /**
     * default logger
     */
    private static final Logger log = LoggerFactory.getLogger(LdapIdentityProvider.class);
    
    private static final String MARKER_CONNECT = "connect";
    private static final String MARKER_LOOKUP = "lookup";

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
    private AbstractPoolableLdapConnectionFactory adminConnectionFactory;

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
    public LdapIdentityProvider(@NotNull LdapProviderConfig config) {
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

    //----------------------------------------------< PrincipalNameResolver >---
    @NotNull
    @Override
    public String fromExternalIdentityRef(@NotNull ExternalIdentityRef externalIdentityRef) throws ExternalIdentityException {
        if (!isMyRef(externalIdentityRef)) {
            throw new ExternalIdentityException("Foreign IDP " + externalIdentityRef.getString());
        }
        return externalIdentityRef.getId();
    }

    //-------------------------------------------< ExternalIdentityProvider >---
    @NotNull
    @Override
    public String getName() {
        return config.getName();
    }

    @Override
    public ExternalIdentity getIdentity(@NotNull ExternalIdentityRef ref) throws ExternalIdentityException {
        if (!isMyRef(ref)) {
            return null;
        }

        LdapConnection connection = connect();
        try {
            Entry entry;
            String id = ref.getId();
            boolean useUidForExtId = config.getUseUidForExtId();
            String userIdAttr = config.getUserConfig().getIdAttribute();
            String groupIdAttr = config.getGroupConfig().getIdAttribute();
            String[] ca = config.getCustomAttributes();
            if (useUidForExtId) {
                entry = getEntry(connection, config.getUserConfig(), id, config.getCustomAttributes());
                if (entry == null) {
                    entry = getEntry(connection, config.getGroupConfig(), id, config.getCustomAttributes());
                }
            } else {
                if (ca.length == 0) {
                    entry = connection.lookup(id, SchemaConstants.ALL_USER_ATTRIBUTES);
                }
                else {
                    List<String> attributes = new ArrayList<>(Arrays.asList(ca));
                    attributes.add("objectClass");
                    attributes.add(userIdAttr);
                    attributes.add(groupIdAttr);
                    String[] attributeArray = new String[attributes.size()];
                    attributes.toArray(attributeArray);
                    entry = connection.lookup(id, attributeArray);
                }
            }
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
        } catch (LdapException | CursorException e) {
            throw lookupFailedException(e, null);
        } finally {
            disconnect(connection);
        }
    }

    @Override
    public ExternalUser getUser(@NotNull String userId) throws ExternalIdentityException {
        DebugTimer timer = new DebugTimer();
        LdapConnection connection = connect();
        timer.mark(MARKER_CONNECT);
        try {
            Entry entry = getEntry(connection, config.getUserConfig(), userId, config.getCustomAttributes());
            timer.mark(MARKER_LOOKUP);
            log.debug("getUser({}) {}", userId, timer);

            if (entry != null) {
                return createUser(entry, userId);
            } else {
                return null;
            }
        } catch (LdapException | CursorException e) {
            throw lookupFailedException(e, timer);
        } finally {
            disconnect(connection);
        }
    }

    @Override
    public ExternalGroup getGroup(@NotNull String name) throws ExternalIdentityException {
        DebugTimer timer = new DebugTimer();
        LdapConnection connection = connect();
        timer.mark(MARKER_CONNECT);
        try {
            Entry entry = getEntry(connection, config.getGroupConfig(), name, config.getCustomAttributes());
            timer.mark(MARKER_LOOKUP);
            log.debug("getGroup({}) {}", name, timer);

            if (entry != null) {
                return createGroup(entry, name);
            } else {
                return null;
            }
        } catch (LdapException | CursorException e) {
            throw lookupFailedException(e, timer);
        } finally {
            disconnect(connection);
        }
    }

    @NotNull
    @Override
    public Iterator<ExternalUser> listUsers() throws ExternalIdentityException {
        try {
            final Iterator<Entry> iter = getEntryIterator(config.getUserConfig());
            return new AbstractLazyIterator<ExternalUser>() {

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
        } catch (LdapException | CursorException e) {
            throw lookupFailedException(e, null);
        }
    }

    @NotNull
    @Override
    public Iterator<ExternalGroup> listGroups() throws ExternalIdentityException {
        try {
            final Iterator<Entry> iter = getEntryIterator(config.getGroupConfig());
            return new AbstractLazyIterator<ExternalGroup>() {

                @Override
                protected ExternalGroup getNext() {
                    while (iter.hasNext()) {
                        try {
                            return createGroup(iter.next(), null);
                        } catch (LdapInvalidAttributeValueException e) {
                            log.warn("Error while creating external group object", e);
                        }
                    }
                    return null;
                }
            };
        } catch (LdapException | CursorException e) {
            throw lookupFailedException(e, null);
        }
    }

    @Override
    public ExternalUser authenticate(@NotNull Credentials credentials) throws ExternalIdentityException, LoginException {
        if (!(credentials instanceof SimpleCredentials)) {
            log.debug("LDAP IDP can only authenticate SimpleCredentials.");
            return null;
        }
        final SimpleCredentials creds = (SimpleCredentials) credentials;
        final LdapUser user = (LdapUser)getUser(creds.getUserID());
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
                connection = createUserConnection();
                timer.mark(MARKER_CONNECT);
                connection.bind(user.getEntry().getDn(), new String(creds.getPassword()));
                timer.mark("bind");
                log.debug("authenticate({}) {}", user.getId(), timer);
            } catch (LdapAuthenticationException e) {
                throw new LoginException("Unable to authenticate against LDAP server: " + e.getMessage());
            } catch (Exception e) {
                throw error(e, "Error while binding user credentials");
            } finally {
                disconnectUserConnection(connection);
            }
        }
        return user;
    }

    @NotNull
    private LdapConnection createUserConnection() throws Exception {
        if (userPool == null) {
            return userConnectionFactory.create();
        } else {
            return userPool.getConnection();
        }
    }

    private void disconnectUserConnection(@Nullable LdapConnection connection) {
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

    //-----------------------------------------------------------< internal >---

    /**
     * Collects the declared (direct) groups of an identity
     * @param ref reference to the identity
     * @return map of identities where the key is the DN of the LDAP entity
     */
    Map<String, ExternalIdentityRef> getDeclaredGroupRefs(ExternalIdentityRef ref, String dn) throws ExternalIdentityException {
        if (!isMyRef(ref)) {
            return Collections.emptyMap();
        }
        String searchFilter = config.getMemberOfSearchFilter(dn);

        LdapConnection connection = null;
        SearchCursor searchCursor = null;
        try {
            // Create the SearchRequest object
            SearchRequest req = new SearchRequestImpl();
            req.setScope(SearchScope.SUBTREE);
            String idAttribute = config.getGroupConfig().getIdAttribute();
            req.addAttributes(idAttribute == null? SchemaConstants.NO_ATTRIBUTE : idAttribute);
            req.setTimeLimit((int) config.getSearchTimeout());
            req.setBase(new Dn(config.getGroupConfig().getBaseDN()));
            req.setFilter(searchFilter);

            log.debug("getDeclaredGroupRefs: using SearchRequest {}.", req);

            Map<String, ExternalIdentityRef> groups = new HashMap<>();
            DebugTimer timer = new DebugTimer();
            connection = connect();
            timer.mark(MARKER_CONNECT);

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
            log.debug("getDeclaredGroupRefs: search below {} with {} found {} entries. {}",
                        config.getGroupConfig().getBaseDN(), searchFilter, groups.size(), timer);

            return groups;
        } catch (Exception e) {
            throw error(e, "Error during ldap membership search.");
        } finally {
            closeSearchCursor(searchCursor);
            disconnect(connection);
        }
    }

    /**
     * Collects the declared (direct) members of a group
     * @param ref the reference to the group
     * @return map of identity refers
     * @throws ExternalIdentityException if an error occurs
     */
    Map<String, ExternalIdentityRef> getDeclaredMemberRefs(ExternalIdentityRef ref, String dn) throws ExternalIdentityException {
        if (!isMyRef(ref)) {
            return Collections.emptyMap();
        }
        LdapConnection connection = null;
        try {
            Map<String, ExternalIdentityRef> members = new HashMap<>();
            DebugTimer timer = new DebugTimer();
            connection = connect();
            timer.mark(MARKER_CONNECT);
            Entry entry = connection.lookup(dn);
            timer.mark(MARKER_LOOKUP);
            Attribute attr = entry.get(config.getGroupMemberAttribute());
            if (attr == null) {
                log.warn("LDAP group does not have configured attribute: {}", config.getGroupMemberAttribute());
            } else {
                for (Value value: attr) {
                    ExternalIdentityRef memberRef = new ExternalIdentityRef(value.getString(), this.getName());
                    members.put(memberRef.getId(), memberRef);
                }
            }
            timer.mark("iterate");
            log.debug("members lookup of {} found {} members. {}", ref.getId(), members.size(), timer);

            return members;
        } catch (Exception e) {
            throw error(e, "Error during ldap group members lookup.");
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
        adminConnectionFactory = new ValidatingPoolableLdapConnectionFactory(cc);
        if (config.getAdminPoolConfig().lookupOnValidate()) {
            adminConnectionFactory.setValidator(new LookupLdapConnectionValidator());
        } else {
            adminConnectionFactory.setValidator(new DefaultLdapConnectionValidator());
        }
        if (config.getAdminPoolConfig().getMaxActive() != 0) {
            adminPool = new LdapConnectionPool(adminConnectionFactory);
            adminPool.setTestOnBorrow(true);
            adminPool.setMaxTotal(config.getAdminPoolConfig().getMaxActive());
            adminPool.setBlockWhenExhausted(true);
            adminPool.setMinEvictableIdleTimeMillis(config.getAdminPoolConfig().getMinEvictableIdleTimeMillis());
            adminPool.setTimeBetweenEvictionRunsMillis(config.getAdminPoolConfig().getTimeBetweenEvictionRunsMillis());
            adminPool.setNumTestsPerEvictionRun(config.getAdminPoolConfig().getNumTestsPerEvictionRun());
        }

        // setup unbound connection pool. let's create a new version of the config
        cc = createConnectionConfig();

        userConnectionFactory = new PoolableUnboundConnectionFactory(cc);
        if (config.getUserPoolConfig().lookupOnValidate()) {
            userConnectionFactory.setValidator(new UnboundLookupConnectionValidator());
        } else {
            userConnectionFactory.setValidator(new UnboundConnectionValidator());
        }
        if (config.getUserPoolConfig().getMaxActive() != 0) {
            userPool = new UnboundLdapConnectionPool(userConnectionFactory);
            userPool.setTestOnBorrow(true);
            userPool.setMaxTotal(config.getUserPoolConfig().getMaxActive());
            userPool.setBlockWhenExhausted(true);
            userPool.setMinEvictableIdleTimeMillis(config.getUserPoolConfig().getMinEvictableIdleTimeMillis());
            userPool.setTimeBetweenEvictionRunsMillis(config.getUserPoolConfig().getTimeBetweenEvictionRunsMillis());
            userPool.setNumTestsPerEvictionRun(config.getUserPoolConfig().getNumTestsPerEvictionRun());
        }

        log.info("LdapIdentityProvider initialized: {}", config);
    }

    /**
     * Creates a new connection config based on the config.
     * @return the connection config.
     */
    @NotNull
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

        String[] enabledProtocols = config.enabledProtocols();
        if (enabledProtocols != null && enabledProtocols.length > 0) {
            cc.setEnabledProtocols(enabledProtocols);
        }

        return cc;
    }

    @Nullable
    private Entry getEntry(@NotNull LdapConnection connection, @NotNull LdapProviderConfig.Identity idConfig, @NotNull String id, @NotNull String[] customAttributes)
            throws CursorException, LdapException {
        String searchFilter = idConfig.getSearchFilter(id);

        // Create the SearchRequest object
        SearchRequest req = createSearchRequest(customAttributes, (int) config.getSearchTimeout(), idConfig.getBaseDN(), searchFilter);
        log.debug("getEntry: using SearchRequest {}.", req);

        // Process the request
        SearchCursor searchCursor = null;
        Entry resultEntry = null;
        try {
            searchCursor = connection.search(req);
            while (searchCursor.next()) {
                if (resultEntry != null) {
                    log.warn("search for {} returned more than one entry. discarding additional ones.", searchFilter);
                } else {
                    // process the SearchResultEntry
                    Response response = searchCursor.get();
                    if (response instanceof SearchResultEntry) {
                        resultEntry = ((SearchResultEntry) response).getEntry();
                    }
                }
            }
        } finally {
            closeSearchCursor(searchCursor);
        }
        if (resultEntry == null) {
            log.debug("getEntry: search below {} with {} found 0 entries.", idConfig.getBaseDN(), searchFilter);
        } else {
            log.debug("getEntry: search below {} with {} found {}", idConfig.getBaseDN(), searchFilter, resultEntry.getDn());
        }
        return resultEntry;
    }


    @NotNull
    private SearchResultIterator getEntryIterator(@NotNull LdapProviderConfig.Identity idConfig) throws LdapException, CursorException, ExternalIdentityException {
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

        return new SearchResultIterator(searchFilter, idConfig);
    }
    
    @NotNull
    private static SearchRequest createSearchRequest(@NotNull String[] attributes, long timeout, @NotNull String baseDN, @NotNull String searchFilter) throws LdapException {
        SearchRequest req = new SearchRequestImpl();
        req.setScope(SearchScope.SUBTREE);
        if (attributes.length == 0) {
            req.addAttributes(SchemaConstants.ALL_USER_ATTRIBUTES);
        } else {
            req.addAttributes(attributes);
        }
        req.setTimeLimit((int) timeout);
        req.setBase(new Dn(baseDN));
        req.setFilter(searchFilter);
        return req;
    }

    private final class SearchResultIterator implements Iterator<Entry> {

        private final String searchFilter;
        private final LdapProviderConfig.Identity idConfig;

        private byte[] cookie;
        private List<Entry> page = Collections.emptyList();
        private boolean searchComplete;
        private int pos = -1;

        public SearchResultIterator(
                @NotNull String searchFilter,
                @NotNull LdapProviderConfig.Identity idConfig) throws LdapException, CursorException, ExternalIdentityException {
            this.searchFilter = searchFilter;
            this.idConfig = idConfig;
            findNextEntry();
        }

        //-------------------------------------------------------< Iterator >---

        @Override
        public boolean hasNext() {
            return pos >= 0;
        }

        @Override
        public Entry next() {
            if (hasNext()) {
                try {
                    Entry entry = page.get(pos);
                    findNextEntry();
                    return entry;
                } catch (LdapException | CursorException | ExternalIdentityException e) {
                    log.error("Error while performing LDAP search", e);
                }
            }
            throw new NoSuchElementException();
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }

        //-------------------------------------------------------< internal >---

        @NotNull
        private SearchRequest createSearchRequest(byte[] cookie, @NotNull String[] userAttributes) throws LdapException {
            SearchRequest req = LdapIdentityProvider.createSearchRequest(userAttributes, config.getSearchTimeout(), idConfig.getBaseDN(), searchFilter);

            // do paged searches (OAK-2874)
            PagedResultsImpl pagedResults = new PagedResultsImpl();
            pagedResults.setSize(1000);
            pagedResults.setCookie(cookie);
            req.addControl(pagedResults);

            return req;
        }

        private boolean loadNextPage() throws ExternalIdentityException, LdapException, CursorException {
            if (searchComplete) {
                return false;
            }
            SearchCursor searchCursor = null;
            DebugTimer timer = new DebugTimer();
            LdapConnection connection = connect();
            timer.mark(MARKER_CONNECT);
            page = new ArrayList<>();
            try {
                SearchRequest req = createSearchRequest(cookie, config.getCustomAttributes());
                log.debug("loadNextPage: using SearchRequest {}.", req);

                searchCursor = connection.search(req);
                while (searchCursor.next()) {
                    Response response = searchCursor.get();

                    if (response instanceof SearchResultEntry) {
                        Entry resultEntry = ((SearchResultEntry) response).getEntry();
                        page.add(resultEntry);
                        log.debug("loadNextPage: search below {} with {} found {}", idConfig.getBaseDN(), searchFilter, resultEntry.getDn());
                    }
                }

                boolean done = searchCursor.isDone();
                cookie = null;
                if (done) {
                    SearchResultDone searchResultDone = searchCursor.getSearchResultDone();
                    if (searchResultDone != null && searchResultDone.getLdapResult().getResultCode() != ResultCodeEnum.UNWILLING_TO_PERFORM) {
                        PagedResults ctrl = (PagedResults) searchResultDone.getControl(PagedResults.OID);
                        if (ctrl != null) {
                            cookie = ctrl.getCookie();
                        }
                    }
                }
                searchComplete = cookie == null || cookie.length == 0;
                timer.mark(MARKER_LOOKUP);

                return !page.isEmpty();
            } finally {
                closeSearchCursor(searchCursor);
                disconnect(connection);
            }
        }

        private void findNextEntry() throws LdapException, CursorException, ExternalIdentityException {
            if (pos == -1 && !loadNextPage()) {
                return;
            }
            if (pos + 1 == page.size()) {
                pos = -1;
                page = Collections.emptyList();
                if (!loadNextPage()) {
                    return;
                }
            }
            pos++;
        }
    }

    @NotNull
    private ExternalUser createUser(@NotNull Entry entry, @Nullable String id)
            throws LdapInvalidAttributeValueException {
        return (ExternalUser) createIdentity(entry, id, false);
    }

    @NotNull
    private ExternalGroup createGroup(@NotNull Entry entry, @Nullable String id)
            throws LdapInvalidAttributeValueException {
        return (ExternalGroup) createIdentity(entry, id, true);
    }

    @NotNull
    private ExternalIdentity createIdentity(@NotNull Entry entry, @Nullable String id, boolean isGroup)
            throws LdapInvalidAttributeValueException {
        LdapProviderConfig.Identity cfg = isGroup ? config.getGroupConfig() : config.getUserConfig();
        if (id == null) {
            String idAttribute = cfg.getIdAttribute();
            Attribute attr = entry.get(idAttribute);
            if (attr == null) {
                throw new LdapInvalidAttributeValueException(ResultCodeEnum.CONSTRAINT_VIOLATION,
                        "no value found for attribute '" + idAttribute + "' for entry " + entry);
            }
            id = attr.getString();
        }
        String extId = config.getUseUidForExtId() ? id : entry.getDn().getName();
        ExternalIdentityRef ref = new ExternalIdentityRef(extId, this.getName());
        String path = cfg.makeDnPath()
                ? createDNPath(entry.getDn())
                : null;
        LdapIdentity identity = isGroup ? new LdapGroup(this, ref, id, path, entry)
                                        : new LdapUser(this, ref, id, path, entry);
        Map<String, Object> props = identity.getProperties();
        applyAttributes(props, entry);
        return identity;
    }

    private static void applyAttributes(Map<String, Object> props, Entry entry) throws LdapInvalidAttributeValueException {
        for (Attribute attr: entry.getAttributes()) {
            if (attr.isHumanReadable()) {
                final Object propValue;
                // for multivalue properties, store as collection
                if (attr.size() > 1) {
                    List<String> values = new ArrayList<>();
                    for (Value value : attr) {
                        values.add(value.getString());
                    }
                    propValue = values;
                } else {
                    propValue = attr.getString();
                }
                props.put(attr.getId(), propValue);
            }
        }
    }

    @NotNull
    private LdapConnection connect() throws ExternalIdentityException {
        try {
            if (adminPool == null) {
                return adminConnectionFactory.makeObject().getObject();
            } else {
                return adminPool.getConnection();
            }
        } catch (Exception e) {
            throw error(e, "Error while connecting to the ldap server.");
        }
    }

    private void disconnect(@Nullable LdapConnection connection) {
        try {
            if (connection != null) {
                if (adminPool == null) {
                    adminConnectionFactory.destroyObject(new DefaultPooledObject<>(connection));
                } else {
                    adminPool.releaseConnection(connection);
                }
            }
        } catch (Exception e) {
            log.warn("Error while disconnecting from the ldap server.", e);
        }
    }

    private boolean isMyRef(@NotNull ExternalIdentityRef ref) {
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

    private static ExternalIdentityException lookupFailedException(@NotNull Exception e, @Nullable DebugTimer timer) {
        String msg = "Error during ldap lookup. {}";
        log.error(msg, ((timer != null) ? timer.getString() : ""), e);
        return new ExternalIdentityException(msg, e);
    }

    private static ExternalIdentityException error(@NotNull Exception e, @NotNull String msg) {
        log.error(msg, e);
        return new ExternalIdentityException(msg, e);
    }
    
    private static void closeSearchCursor(@Nullable SearchCursor searchCursor) {
        if (searchCursor != null) {
            try {
                searchCursor.close();
            } catch (IOException e) {
                log.warn("Failed to close search cursor.", e);
            }
        }
    }
}
