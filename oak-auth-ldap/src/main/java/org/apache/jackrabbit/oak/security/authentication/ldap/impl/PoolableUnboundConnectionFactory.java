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

import org.apache.commons.pool.PoolableObjectFactory;
import org.apache.directory.api.ldap.model.exception.LdapException;
import org.apache.directory.ldap.client.api.LdapConnection;
import org.apache.directory.ldap.client.api.LdapConnectionConfig;
import org.apache.directory.ldap.client.api.LdapConnectionValidator;
import org.apache.directory.ldap.client.api.LdapNetworkConnection;
import org.apache.directory.ldap.client.api.LookupLdapConnectionValidator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * A factory for creating unbound LdapConnection objects managed by LdapConnectionPool.
 */
public class PoolableUnboundConnectionFactory implements PoolableObjectFactory<LdapConnection> {

    /**
     * default logger
     */
    private static final Logger log = LoggerFactory.getLogger(PoolableUnboundConnectionFactory.class);

    /**
     * configuration object for the connection
     */
    private LdapConnectionConfig config;

    /**
     * internal validator
     */
    private LdapConnectionValidator validator = new LookupLdapConnectionValidator();

    /**
     * Creates a new instance of PoolableUnboundConnectionFactory
     *
     * @param config the configuration for creating LdapConnections
     */
    public PoolableUnboundConnectionFactory(LdapConnectionConfig config) {
        this.config = config;
    }

    /**
     * gets the connection validator
     * @return the connection validator
     */
    public LdapConnectionValidator getValidator() {
        return validator;
    }

    /**
     * Sets the connection validator that is used when the connection is taken out of the pool
     * @param validator the validator
     */
    public void setValidator(LdapConnectionValidator validator) {
        this.validator = validator;
    }

    /**
     * {@inheritDoc}
     */
    public void activateObject(LdapConnection connection) {
        log.debug("activate connection: {}", connection);
    }


    /**
     * {@inheritDoc}
     */
    public void destroyObject(LdapConnection connection) throws IOException {
        log.debug("destroy connection: {}", connection);
        connection.close();
    }


    /**
     * {@inheritDoc}
     */
    public LdapConnection makeObject() throws LdapException {
        LdapNetworkConnection connection = config.isUseTls()
                ? new TlsGuardingConnection(config)
                : new LdapNetworkConnection(config);
        connection.connect();
        log.debug("creating new connection: {}", connection);
        return connection;
    }


    /**
     * {@inheritDoc}
     */
    public void passivateObject(LdapConnection connection) {
        log.debug("passivate connection: {}", connection);
    }


    /**
     * {@inheritDoc}
     */
    public boolean validateObject(LdapConnection connection) {
        boolean valid = validator == null || validator.validate(connection);
        log.debug("validating connection {}: {}", connection, valid);
        return valid;
    }

    /**
     * internal helper class that guards the original ldap connection from starting TLS if already started..
     * this is to ensure that pooled connections can be 'bind()' several times.
     *
     * @see org.apache.directory.ldap.client.api.LdapNetworkConnection#bindAsync(org.apache.directory.api.ldap.model.message.BindRequest)
     */
    private static final class TlsGuardingConnection extends LdapNetworkConnection {

        private boolean tlsStarted;

        private TlsGuardingConnection(LdapConnectionConfig config) {
            super(config);
        }

        @Override
        public void startTls() throws LdapException {
            if (tlsStarted) {
                return;
            }
            super.startTls();
            tlsStarted = true;
        }
    }
}
