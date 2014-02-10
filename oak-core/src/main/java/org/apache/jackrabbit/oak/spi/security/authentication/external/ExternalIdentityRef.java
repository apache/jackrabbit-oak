/*************************************************************************
 *
 * ADOBE CONFIDENTIAL
 * ___________________
 *
 *  Copyright ${today.year} Adobe Systems Incorporated
 *  All Rights Reserved.
 *
 * NOTICE:  All information contained herein is, and remains
 * the property of Adobe Systems Incorporated and its suppliers,
 * if any.  The intellectual and technical concepts contained
 * herein are proprietary to Adobe Systems Incorporated and its
 * suppliers and are protected by trade secret or copyright law.
 * Dissemination of this information or reproduction of this material
 * is strictly forbidden unless prior written permission is obtained
 * from Adobe Systems Incorporated.
 **************************************************************************/
package org.apache.jackrabbit.oak.spi.security.authentication.external;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;

import org.apache.jackrabbit.util.Text;

/**
 * {@code ExternalIdentityRef} defines a reference to an external identity.
 */
public class ExternalIdentityRef {

    private final String id;

    private final String providerName;

    private final String string;

    /**
     * Creates a new external identity ref with the given id and provider name
     * @param id the id of the identity.
     * @param providerName the name of the identity provider
     */
    public ExternalIdentityRef(@Nonnull String id, @CheckForNull String providerName) {
        this.id = id;
        this.providerName = providerName;

        StringBuilder b = new StringBuilder();
        b.append(Text.escape(id));
        if (providerName != null && providerName.length() > 0) {
            b.append('@').append(Text.escape(providerName));
        }
        string =  b.toString();
    }

    /**
     * Returns the name of the identity provider.
     * @return the name of the identity provider.
     */
    @CheckForNull
    public String getProviderName() {
        return providerName;
    }

    /**
     * Returns the id of the external identity. for example the DN of an LDAP user.
     * @return the id
     */
    @Nonnull
    public String getId() {
        return id;
    }

    /**
     * Returns a string representation of this external identity reference
     * @return a string representation.
     */
    @Nonnull
    public String getString() {
        return string;
    }

    /**
     * Creates an external identity reference from a string representation.
     * @param str the string
     * @return the reference
     */
    public static ExternalIdentityRef fromString(@Nonnull String str) {
        int idx = str.indexOf('@');
        if (idx < 0) {
            return new ExternalIdentityRef(Text.unescape(str), null);
        } else {
            return new ExternalIdentityRef(
                    Text.unescape(str.substring(0, idx)),
                    Text.unescape(str.substring(idx+1))
            );
        }
    }
}