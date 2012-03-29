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

import org.apache.jackrabbit.mk.core.MicroKernelImpl;
import org.apache.jackrabbit.oak.api.Connection;
import org.apache.jackrabbit.oak.api.RepositoryService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jcr.Credentials;
import javax.jcr.Repository;
import javax.jcr.RepositoryException;
import javax.jcr.Session;
import javax.jcr.Value;
import javax.security.auth.login.LoginException;

/**
 * {@code RepositoryImpl}...
 */
public class RepositoryImpl implements Repository {

    /**
     * logger instance
     */
    private static final Logger log = LoggerFactory.getLogger(RepositoryImpl.class);

    private final GlobalContext context;

    /**
     * Utility constructor that creates a JCR binding for an initially empty,
     * newly constructed Oak repository.
     */
    public RepositoryImpl() {
        this(new GlobalContext(new MicroKernelImpl()));
    }

    public RepositoryImpl(GlobalContext context) {
        this.context = context;
    }

    //---------------------------------------------------------< Repository >---
    @Override
    public String[] getDescriptorKeys() {
        // TODO
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isStandardDescriptor(String key) {
        // TODO
        throw new UnsupportedOperationException();
    }

    @Override
    public String getDescriptor(String key) {
        // TODO
        throw new UnsupportedOperationException();
    }

    @Override
    public Value getDescriptorValue(String key) {
        // TODO
        throw new UnsupportedOperationException();
    }

    @Override
    public Value[] getDescriptorValues(String key) {
        // TODO
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isSingleValueDescriptor(String key) {
        // TODO
        throw new UnsupportedOperationException();
    }

    /**
     * @see javax.jcr.Repository#login(javax.jcr.Credentials, String)
     */
    @Override
    public Session login(Credentials credentials, String workspaceName) throws RepositoryException {
        // TODO: needs complete refactoring

        RepositoryService service = context.getInstance(RepositoryService.class);
        try {
            Connection connection = service.login(credentials, workspaceName);
            return new SessionImpl(context, connection);
        } catch (LoginException e) {
            throw new javax.jcr.LoginException(e.getMessage());
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
}