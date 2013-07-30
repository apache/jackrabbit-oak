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
package org.apache.jackrabbit.oak.jcr;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.Map;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.jcr.Credentials;
import javax.jcr.Repository;
import javax.jcr.RepositoryException;
import javax.jcr.Session;
import javax.jcr.SimpleCredentials;
import javax.jcr.Value;
import javax.security.auth.login.LoginException;

import org.apache.jackrabbit.api.security.authentication.token.TokenCredentials;
import org.apache.jackrabbit.commons.SimpleValueFactory;
import org.apache.jackrabbit.oak.api.ContentRepository;
import org.apache.jackrabbit.oak.api.ContentSession;
import org.apache.jackrabbit.oak.jcr.delegate.SessionDelegate;
import org.apache.jackrabbit.oak.spi.security.SecurityProvider;
import org.apache.jackrabbit.oak.spi.whiteboard.Whiteboard;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * TODO document
 */
public class RepositoryImpl implements Repository {

    /**
     * logger instance
     */
    private static final Logger log = LoggerFactory.getLogger(RepositoryImpl.class);

    /**
     * Name of the session attribute value determining the session refresh
     * behaviour.
     *
     * @see SessionDelegate#SessionDelegate(ContentSession, long)
     */
    public static final String REFRESH_INTERVAL = "refresh-interval";

    /**
     * Name of the session attribute value determining works space name.
     */
    public static final String WORKSPACE_NAME = "workspace-name";

    /**
     * Default value for {@link #REFRESH_INTERVAL}.
     */
    private static final long DEFAULT_REFRESH_INTERVAL = TimeUnit.SECONDS.toMillis(1);

    private final Descriptors descriptors = new Descriptors(new SimpleValueFactory());
    private final ContentRepository contentRepository;
    private final Whiteboard whiteboard;
    private final SecurityProvider securityProvider;

    public RepositoryImpl(@Nonnull ContentRepository contentRepository,
                          @Nonnull Whiteboard whiteboard,
                          @Nonnull SecurityProvider securityProvider) {
        this.contentRepository = checkNotNull(contentRepository);
        this.whiteboard = checkNotNull(whiteboard);
        this.securityProvider = checkNotNull(securityProvider);
    }

    //---------------------------------------------------------< Repository >---
    /**
     * @see javax.jcr.Repository#getDescriptorKeys()
     */
    @Override
    public String[] getDescriptorKeys() {
        return descriptors.getKeys();
    }

    /**
     * @see Repository#isStandardDescriptor(String)
     */
    @Override
    public boolean isStandardDescriptor(String key) {
        return descriptors.isStandardDescriptor(key);
    }

    /**
     * @see javax.jcr.Repository#getDescriptor(String)
     */
    @Override
    public String getDescriptor(String key) {
        try {
            Value v = getDescriptorValue(key);
            return v == null
                    ? null
                    : v.getString();
        } catch (RepositoryException e) {
            log.debug("Error converting value for descriptor with key {} to string", key);
            return null;
        }
    }

    /**
     * @see javax.jcr.Repository#getDescriptorValue(String)
     */
    @Override
    public Value getDescriptorValue(String key) {
        return descriptors.getValue(key);
    }

    /**
     * @see javax.jcr.Repository#getDescriptorValues(String)
     */
    @Override
    public Value[] getDescriptorValues(String key) {
        return descriptors.getValues(key);
    }

    /**
     * @see javax.jcr.Repository#isSingleValueDescriptor(String)
     */
    @Override
    public boolean isSingleValueDescriptor(String key) {
        return descriptors.isSingleValueDescriptor(key);
    }

    // TODO make this method available through JackrabbitRepository. See OAK-803, JCR-3634
    public Session login(Credentials credentials, Map<String, Object> attributes)
            throws RepositoryException {
        return login(credentials,
                getString(attributes, WORKSPACE_NAME),
                getLong(attributes, REFRESH_INTERVAL, DEFAULT_REFRESH_INTERVAL));
    }

    /**
     * @see javax.jcr.Repository#login(javax.jcr.Credentials, String)
     */
    @Override
    public Session login(@Nullable Credentials credentials, @Nullable String workspaceName)
            throws RepositoryException {
        return login(credentials, workspaceName, getRefreshInterval(credentials));
    }

    /**
     * Calls {@link Repository#login(Credentials, String)} with
     * {@code null} arguments.
     *
     * @return logged in session
     * @throws RepositoryException if an error occurs
     */
    @Override
    public Session login() throws RepositoryException {
        return login(null, (String) null);
    }

    /**
     * Calls {@link Repository#login(Credentials, String)} with
     * the given credentials and a {@code null} workspace name.
     *
     * @param credentials login credentials
     * @return logged in session
     * @throws RepositoryException if an error occurs
     */
    @Override
    public Session login(Credentials credentials) throws RepositoryException {
        return login(credentials, (String) null);
    }

    /**
     * Calls {@link Repository#login(Credentials, String)} with
     * {@code null} credentials and the given workspace name.
     *
     * @param workspace workspace name
     * @return logged in session
     * @throws RepositoryException if an error occurs
     */
    @Override
    public Session login(String workspace) throws RepositoryException {
        return login(null, workspace);
    }

    //------------------------------------------------------------< internal >---

    SecurityProvider getSecurityProvider() {
        return securityProvider;
    }

    ContentRepository getContentRepository() {
        return contentRepository;
    }

    //------------------------------------------------------------< private >---

    private Session login(
            @Nullable Credentials credentials, @Nullable String workspaceName,
            long refreshInterval) throws RepositoryException {
        try {
            ContentSession contentSession = contentRepository.login(credentials, workspaceName);
            SessionContext context = new SessionContext(this, whiteboard,
                    new SessionDelegate(contentSession, refreshInterval));
            return context.getSession();
        } catch (LoginException e) {
            throw new javax.jcr.LoginException(e.getMessage(), e);
        }
    }

    private static long getRefreshInterval(Credentials credentials) {
        if (credentials instanceof SimpleCredentials) {
            Object value = ((SimpleCredentials) credentials).getAttribute(REFRESH_INTERVAL);
            return toLong(value, DEFAULT_REFRESH_INTERVAL);
        } else if (credentials instanceof TokenCredentials) {
            String value = ((TokenCredentials) credentials).getAttribute(REFRESH_INTERVAL);
            if (value != null) {
                return toLong(value);
            }
        }
        return DEFAULT_REFRESH_INTERVAL;
    }

    private static long getLong(Map<String, Object> attributes, String name, long defaultValue) {
        return toLong(attributes.get(name), defaultValue);
    }

    private static String getString(Map<String, Object> attributes, String name) {
        Object value = attributes.get(name);
        if (value instanceof String) {
            return (String) value;
        } else {
            return null;
        }
    }

    private static long toLong(Object value, long defaultValue) {
        if (value instanceof Long) {
            return (Long) value;
        } else if (value instanceof Integer) {
            return (Integer) value;
        } else if (value instanceof String) {
            return toLong((String) value);
        } else {
            return defaultValue;
        }
    }

    private static long toLong(String longValue) {
        try {
            return Long.parseLong(longValue);
        } catch (NumberFormatException e) {
            log.warn("Invalid value '" + longValue + "' for " + REFRESH_INTERVAL +
                    ". Expected long. Defaulting to '" + DEFAULT_REFRESH_INTERVAL +
                    "' seconds .", e);
            return DEFAULT_REFRESH_INTERVAL;
        }
    }

}