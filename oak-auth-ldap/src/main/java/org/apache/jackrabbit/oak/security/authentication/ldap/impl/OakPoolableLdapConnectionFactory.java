/*
 *   Licensed to the Apache Software Foundation (ASF) under one
 *   or more contributor license agreements.  See the NOTICE file
 *   distributed with this work for additional information
 *   regarding copyright ownership.  The ASF licenses this file
 *   to you under the Apache License, Version 2.0 (the
 *   "License"); you may not use this file except in compliance
 *   with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing,
 *   software distributed under the License is distributed on an
 *   "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *   KIND, either express or implied.  See the License for the
 *   specific language governing permissions and limitations
 *   under the License.
 *
 */

package org.apache.jackrabbit.oak.security.authentication.ldap.impl;


import org.apache.directory.api.ldap.model.constants.SchemaConstants;
import org.apache.directory.api.ldap.model.exception.LdapException;
import org.apache.directory.api.ldap.model.name.Dn;
import org.apache.directory.ldap.client.api.LdapConnection;
import org.apache.directory.ldap.client.api.LdapConnectionConfig;
import org.apache.directory.ldap.client.api.PoolableLdapConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * A factory for creating LdapConnection objects managed by LdapConnectionPool.
 *
 * @author <a href="mailto:dev@directory.apache.org">Apache Directory Project</a>
 */
public class OakPoolableLdapConnectionFactory extends PoolableLdapConnectionFactory {

    /**
     * the logger
     */
    private static final Logger log = LoggerFactory.getLogger(OakPoolableLdapConnectionFactory.class);

    /**
     * flag controlling the validation behavior
     */
    private boolean lookupOnValidate;

    public OakPoolableLdapConnectionFactory(LdapConnectionConfig config) {
        super(config);
    }

    /**
     * Checks if a lookup is performed during {@link #validateObject(LdapConnection)}.
     * @return {@code true} if a lookup is performed.
     */
    public boolean getLookupOnValidate() {
        return lookupOnValidate;
    }

    /**
     * @see #getLookupOnValidate()
     */
    public void setLookupOnValidate(boolean lookupOnValidate) {
        this.lookupOnValidate = lookupOnValidate;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean validateObject(LdapConnection connection) {
        boolean valid = false;
        if (connection.isConnected()) {
            if (lookupOnValidate) {
                try {
                    valid = connection.lookup(Dn.ROOT_DSE, SchemaConstants.NO_ATTRIBUTE) != null;
                } catch (LdapException le) {
                    log.debug("error during connection validation: {}", le.toString());
                }
            }
        }
        log.debug("validating connection {}: {}", connection, valid);
        return valid;
    }
}
