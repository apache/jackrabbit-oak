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
package org.apache.jackrabbit.oak.security.authorization.permission;

import java.util.Collection;
import java.util.Iterator;

import javax.annotation.Nonnull;

import org.apache.jackrabbit.oak.api.Tree;

/**
 * <code>PermissionEntryProvider</code> provides permission entries for a given set of principals.
 * It may internally hold a cache to improve performance and usually operates on the permission store.
 */
public interface PermissionEntryProvider {

    Iterator<PermissionEntry> getEntryIterator(@Nonnull EntryPredicate predicate);

    Collection<PermissionEntry> getEntries(@Nonnull Tree accessControlledTree);

    Collection<PermissionEntry> getEntries(@Nonnull String accessControlledPath);

    void flush();
}