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

import org.apache.directory.api.ldap.model.constants.SchemaConstants;
import org.apache.directory.api.ldap.model.exception.LdapException;
import org.apache.directory.api.ldap.model.name.Dn;
import org.apache.directory.ldap.client.api.LdapConnection;
import org.apache.directory.ldap.client.api.LdapConnectionValidator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@code UnboundConnectionValidator}...
 */
public class UnboundLookupConnectionValidator implements LdapConnectionValidator {

    /**
     * default logger
     */
    private static final Logger log = LoggerFactory.getLogger(UnboundLookupConnectionValidator.class);

    /**
     * Returns true if <code>connection</code> is connected, authenticated, and
     * a lookup on the rootDSE returns a non-null response.
     *
     * @param connection The connection to validate
     * @return True, if the connection is still valid
     */
    public boolean validate(LdapConnection connection) {
        try {
            return connection.isConnected()
                    && (connection.lookup(Dn.ROOT_DSE, SchemaConstants.NO_ATTRIBUTE) != null);
        } catch (LdapException e) {
            log.info("validating failed: {}", e);
            return false;
        }
    }
}