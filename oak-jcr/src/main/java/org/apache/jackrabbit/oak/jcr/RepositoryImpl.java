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

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import javax.jcr.Credentials;
import javax.jcr.Repository;
import javax.jcr.RepositoryException;
import javax.jcr.Session;
import javax.jcr.Value;
import javax.security.auth.login.LoginException;

import org.apache.jackrabbit.commons.SimpleValueFactory;
import org.apache.jackrabbit.mk.api.MicroKernel;
import org.apache.jackrabbit.mk.core.MicroKernelImpl;
import org.apache.jackrabbit.oak.api.ContentRepository;
import org.apache.jackrabbit.oak.api.ContentSession;
import org.apache.jackrabbit.oak.core.ContentRepositoryImpl;
import org.apache.jackrabbit.oak.plugins.name.NameValidatorProvider;
import org.apache.jackrabbit.oak.plugins.name.NamespaceValidatorProvider;
import org.apache.jackrabbit.oak.plugins.type.InitialContent;
import org.apache.jackrabbit.oak.plugins.type.TypeValidatorProvider;
import org.apache.jackrabbit.oak.plugins.unique.UniqueIndexHook;
import org.apache.jackrabbit.oak.plugins.commit.ConflictValidatorProvider;
import org.apache.jackrabbit.oak.security.authorization.AccessControlValidatorProvider;
import org.apache.jackrabbit.oak.security.authorization.PermissionValidatorProvider;
import org.apache.jackrabbit.oak.security.privilege.PrivilegeValidatorProvider;
import org.apache.jackrabbit.oak.security.user.UserValidatorProvider;
import org.apache.jackrabbit.oak.spi.commit.CompositeHook;
import org.apache.jackrabbit.oak.spi.commit.CompositeValidatorProvider;
import org.apache.jackrabbit.oak.spi.commit.ValidatingHook;
import org.apache.jackrabbit.oak.spi.commit.ValidatorProvider;
import org.apache.jackrabbit.oak.spi.security.user.UserConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@code RepositoryImpl}...
 */
public class RepositoryImpl implements Repository {

    /**
     * logger instance
     */
    private static final Logger log = LoggerFactory.getLogger(RepositoryImpl.class);

    private static final ValidatorProvider DEFAULT_VALIDATOR =
            new CompositeValidatorProvider(
                    new NameValidatorProvider(),
                    new NamespaceValidatorProvider(),
                    new TypeValidatorProvider(),
                    new ConflictValidatorProvider(),
                    new PermissionValidatorProvider(),
                    new AccessControlValidatorProvider(),
                    // FIXME: retrieve from user context
                    new UserValidatorProvider(new UserConfig("admin")),
                    new PrivilegeValidatorProvider());

    private static final CompositeHook DEFAULT_COMMIT_HOOK =
            new CompositeHook(
                    new ValidatingHook(DEFAULT_VALIDATOR),
                    new UniqueIndexHook());

    private final Descriptors descriptors = new Descriptors(new SimpleValueFactory());
    private final ContentRepository contentRepository;

    private final ScheduledExecutorService executor;

    public RepositoryImpl(
            ContentRepository contentRepository,
            ScheduledExecutorService executor) {
        this.contentRepository = contentRepository;
        this.executor = executor;
    }

    public RepositoryImpl(
            MicroKernel kernel, ScheduledExecutorService executor) {
        this(new ContentRepositoryImpl(setupInitialContent(kernel), null,
                DEFAULT_COMMIT_HOOK), executor);
    }

    /**
     * Utility constructor that creates a new in-memory repository for use
     * mostly in test cases. The executor service is initialized with an
     * empty thread pool, so things like observation won't work by default.
     * Use the other constructor with a properly managed executor service
     * if such features are needed.
     */
    public RepositoryImpl() {
        this(new MicroKernelImpl(), Executors.newScheduledThreadPool(0));
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
        }
        catch (RepositoryException e) {
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

    /**
     * @see javax.jcr.Repository#login(javax.jcr.Credentials, String)
     */
    @Override
    public Session login(Credentials credentials, String workspaceName) throws RepositoryException {
        // TODO: needs complete refactoring
        try {
            ContentSession contentSession = contentRepository.login(credentials, workspaceName);
            return new SessionDelegate(this, executor, contentSession, false).getSession();
        } catch (LoginException e) {
            throw new javax.jcr.LoginException(e.getMessage(), e);
        }
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
        return login(null, null);
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
        return login(credentials, null);
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

    private static MicroKernel setupInitialContent(MicroKernel mk) {
        new InitialContent().available(mk);
        return mk;
    }
}