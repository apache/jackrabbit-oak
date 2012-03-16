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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jcr.Credentials;
import javax.jcr.Repository;
import javax.jcr.RepositoryException;
import javax.jcr.Session;
import javax.jcr.Value;
import javax.jcr.ValueFactory;

/**
 * RepositoryAdaptor
 */
class RepositoryAdaptor implements Repository {

    /**
     * logger instance
     */
    private static final Logger log = LoggerFactory.getLogger(RepositoryAdaptor.class);

    private final Repository repository;
    private final Descriptors descriptors;

    RepositoryAdaptor(Repository repository, ValueFactory valueFactory) {
        this.repository = repository;
        // TODO: define if descriptors are being retrieved from oak-api.
        // TODO: in this case the 'sessionInfo' needs to be passed to the constructor as well.
        this.descriptors = new Descriptors(valueFactory);
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
     * @see javax.jcr.Repository#login(Credentials, String)
     */
    @Override
    public Session login(Credentials credentials, String workspaceName) throws RepositoryException {
        return repository.login(credentials, workspaceName);
    }

    /**
     * @see javax.jcr.Repository#login(Credentials)
     */
    @Override
    public Session login(Credentials credentials) throws RepositoryException {
        return repository.login(credentials);
    }

    /**
     * @see javax.jcr.Repository#login(String)
     */
    @Override
    public Session login(String workspaceName) throws RepositoryException {
        return repository.login(workspaceName);

    }

    /**
     * @see javax.jcr.Repository#login()
     */
    @Override
    public Session login() throws RepositoryException {
        return repository.login();
    }

    //-------------------------------------------------------------< Object >---
    @Override
    public int hashCode() {
        return repository.hashCode();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        } else if (o instanceof RepositoryAdaptor) {
            return repository.equals(((RepositoryAdaptor) o).repository);
        }
        return false;
    }
}