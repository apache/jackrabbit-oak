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

import java.security.Principal;
import java.util.Map;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;

/**
 * {@code ExternalIdentity} defines an identity provided by an external system.
 */
public interface ExternalIdentity {

    /**
     * Returns the id of this identity as used in the external system.
     * @return the external id.
     */
    @Nonnull
    ExternalIdentityRef getExternalId();

    /**
     * Returns the local id of this identity as it would be used in this repository. This usually corresponds to
     * {@link org.apache.jackrabbit.api.security.user.Authorizable#getID()}
     *
     * @return the internal id.
     */
    @Nonnull
    String getId();

    /**
     * Returns the desired intermediate relative path of the authorizable to be created. For example, one could map
     * an external hierarchy into the local users and groups hierarchy.
     *
     * @return the intermediate path or {@code null} or empty.
     */
    @CheckForNull
    String getIntermediatePath();

    /**
     * Returns an iterable of the declared groups of this external identity.
     * @return the declared groups
     */
    @Nonnull
    Iterable<? extends ExternalGroup> getGroups();

    /**
     * Returns a map of properties of this external identity.
     * @return the properties
     */
    @Nonnull
    Map<String, ?> getProperties();

    // todo: really?
    Principal getPrincipal();

}