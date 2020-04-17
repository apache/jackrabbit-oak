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

import org.apache.commons.pool.impl.GenericObjectPool;
import org.apache.directory.ldap.client.api.LdapConnection;

/**
 * A pool implementation for LdapConnection objects.
 * <p>
 * This class is just a wrapper around the commons GenericObjectPool, and has
 * a more meaningful name to represent the pool type.
 */
public class UnboundLdapConnectionPool extends GenericObjectPool<LdapConnection> {
    /**
     * Instantiates a new LDAP connection pool.
     *
     * @param factory the LDAP connection factory
     */
    public UnboundLdapConnectionPool(PoolableUnboundConnectionFactory factory) {
        super(factory);
    }


    /**
     * Gives a Unbound LdapConnection fetched from the pool.
     *
     * @return an LdapConnection object from pool
     * @throws Exception if an error occurs while obtaining a connection from the factory
     */
    public LdapConnection getConnection() throws Exception {
        return super.borrowObject();
    }


    /**
     * Places the given LdapConnection back in the pool.
     *
     * @param connection the LdapConnection to be released
     * @throws Exception if an error occurs while releasing the connection
     */
    public void releaseConnection(LdapConnection connection) throws Exception {
        super.returnObject(connection);
    }
}
