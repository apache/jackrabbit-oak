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