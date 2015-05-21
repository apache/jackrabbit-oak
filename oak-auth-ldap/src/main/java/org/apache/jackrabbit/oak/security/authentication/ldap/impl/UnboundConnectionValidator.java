/**
 * **********************************************************************
 * <p/>
 * ADOBE CONFIDENTIAL
 * ___________________
 * <p/>
 * Copyright ${today.year} Adobe Systems Incorporated
 * All Rights Reserved.
 * <p/>
 * NOTICE:  All information contained herein is, and remains
 * the property of Adobe Systems Incorporated and its suppliers,
 * if any.  The intellectual and technical concepts contained
 * herein are proprietary to Adobe Systems Incorporated and its
 * suppliers and are protected by trade secret or copyright law.
 * Dissemination of this information or reproduction of this material
 * is strictly forbidden unless prior written permission is obtained
 * from Adobe Systems Incorporated.
 * ************************************************************************
 */
package org.apache.jackrabbit.oak.security.authentication.ldap.impl;

import org.apache.directory.ldap.client.api.LdapConnection;
import org.apache.directory.ldap.client.api.LdapConnectionValidator;

/**
 * {@code UnboundConnectionValidator}...
 */
public class UnboundConnectionValidator implements LdapConnectionValidator {

    /**
     * Returns true if <code>connection</code> is connected
     *
     * @param connection The connection to validate
     * @return True, if the connection is still valid
     */
    @Override
    public boolean validate( LdapConnection connection ) {
        return connection.isConnected();
    }
}