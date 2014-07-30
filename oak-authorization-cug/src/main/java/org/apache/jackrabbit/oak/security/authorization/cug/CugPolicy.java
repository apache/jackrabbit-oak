package org.apache.jackrabbit.oak.security.authorization.cug; /*************************************************************************
 *
 * ADOBE CONFIDENTIAL
 * ___________________
 *
 *  Copyright 2011 Adobe Systems Incorporated
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

import java.security.Principal;
import java.util.Set;
import javax.annotation.Nonnull;
import javax.jcr.security.AccessControlException;

import org.apache.jackrabbit.api.security.JackrabbitAccessControlPolicy;

/**
 * Denies read access for all principals except for the specified principals.
 */
public interface CugPolicy extends JackrabbitAccessControlPolicy {

    @Nonnull
    Set<Principal> getPrincipals();

    boolean addPrincipals(@Nonnull Principal... principals) throws AccessControlException;

    boolean removePrincipals(@Nonnull Principal... principals);

}
