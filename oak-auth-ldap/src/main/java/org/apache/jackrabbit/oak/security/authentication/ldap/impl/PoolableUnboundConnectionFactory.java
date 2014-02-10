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


import org.apache.commons.pool.PoolableObjectFactory;
import org.apache.directory.ldap.client.api.LdapConnection;
import org.apache.directory.ldap.client.api.LdapConnectionConfig;
import org.apache.directory.ldap.client.api.LdapNetworkConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * A factory for creating unbound LdapConnection objects managed by LdapConnectionPool.
 */
public class PoolableUnboundConnectionFactory implements PoolableObjectFactory<LdapConnection> {

    /**
     * the logger
     */
    private static final Logger log = LoggerFactory.getLogger(PoolableUnboundConnectionFactory.class);

    /**
     * configuration object for the connection
     */
    private LdapConnectionConfig config;

    /**
     * Creates a new instance of PoolableUnboundConnectionFactory
     *
     * @param config the configuration for creating LdapConnections
     */
    public PoolableUnboundConnectionFactory(LdapConnectionConfig config) {
        this.config = config;
    }


    /**
     * {@inheritDoc}
     */
    public void activateObject(LdapConnection connection) throws Exception {
        log.debug("Activating {}", connection);
    }


    /**
     * {@inheritDoc}
     */
    public void destroyObject(LdapConnection connection) throws Exception {
        log.debug("Destroying {}", connection);
        connection.close();
    }


    /**
     * {@inheritDoc}
     */
    public LdapConnection makeObject() throws Exception {
        log.debug("Creating a LDAP connection");
        LdapNetworkConnection connection = new LdapNetworkConnection(config);
        connection.connect();
        return connection;
    }


    /**
     * {@inheritDoc}
     */
    public void passivateObject(LdapConnection connection) throws Exception {
        log.debug("Passivating {}", connection);
    }


    /**
     * {@inheritDoc}
     */
    public boolean validateObject(LdapConnection connection) {
        log.debug("Validating {}", connection);
        return connection.isConnected();
    }
}
