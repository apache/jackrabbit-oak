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

import org.apache.jackrabbit.commons.AbstractRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jcr.Credentials;
import javax.jcr.LoginException;
import javax.jcr.NoSuchWorkspaceException;
import javax.jcr.RepositoryException;
import javax.jcr.Session;
import javax.jcr.Value;
import java.util.Map;
import java.util.Set;

/**
 * {@code RepositoryImpl}...
 */
public class RepositoryImpl extends AbstractRepository {

    /**
     * logger instance
     */
    private static final Logger log = LoggerFactory.getLogger(RepositoryImpl.class);

    private final GlobalContext context;

    private Map<String, Value[]> descriptors;

    public RepositoryImpl(GlobalContext context) {
        this.context = context;
    }

    //---------------------------------------------------------< Repository >---
    /**
     * @see javax.jcr.Repository#getDescriptorKeys()
     */
    @Override
    public String[] getDescriptorKeys() {
        Set<String> keys = getDescriptors().keySet();
        return keys.toArray(new String[keys.size()]);
    }

    /**
     * @see javax.jcr.Repository#getDescriptor(String)
     */
    @Override
    public String getDescriptor(String key) {
        Value v = getDescriptorValue(key);
        try {
            return (v == null) ? null : v.getString();
        } catch (RepositoryException e) {
            log.error("corrupt descriptor value: " + key, e);
            return null;
        }
    }

    /**
     * @see javax.jcr.Repository#getDescriptorValue(String)
     */
    @Override
    public Value getDescriptorValue(String key) {
        Value[] vs = getDescriptorValues(key);
        return (vs == null || vs.length != 1) ? null : vs[0];
    }

    /**
     * @see javax.jcr.Repository#getDescriptorValues(String)
     */
    @Override
    public Value[] getDescriptorValues(String key) {
        Map<String, Value[]> descriptors = getDescriptors();
        if (descriptors.containsKey(key)) {
            return descriptors.get(key);
        } else {
            return null;

        }
    }

    /**
     * @see javax.jcr.Repository#isSingleValueDescriptor(String)
     */
    @Override
    public boolean isSingleValueDescriptor(String key) {
        Value[] vs = getDescriptors().get(key);
        return (vs != null && vs.length == 1);
    }

    /**
     * @see javax.jcr.Repository#login(javax.jcr.Credentials, String)
     */
    @Override
    public Session login(Credentials credentials, String workspaceName) throws LoginException, NoSuchWorkspaceException, RepositoryException {
        SessionFactory sessionFactory = context.getInstance(SessionFactory.class);
        return sessionFactory.createSession(context, credentials, workspaceName);
    }

    //------------------------------------------------------------< private >---
    /**
     * Returns the descriptor map.
     *
     * @return the descriptor map.
     */
    private Map<String, Value[]> getDescriptors() {
        // TODO
        return null;
    }
}