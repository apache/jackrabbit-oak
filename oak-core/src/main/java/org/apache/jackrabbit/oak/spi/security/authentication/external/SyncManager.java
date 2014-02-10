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

/**
 * The external identity synchronization management.
 *
 * The default manager is registered as OSGi service and can also be retrieved via
 * {@link org.apache.jackrabbit.oak.spi.security.SecurityProvider#getConfiguration(Class)}
 */
public interface SyncManager {

    /**
     * Returns the sync handler with the given name.
     * @param name the name of the sync handler or {@code null}
     * @return the sync handler
     */
    @CheckForNull
    SyncHandler getSyncHandler(@Nonnull String name);
}