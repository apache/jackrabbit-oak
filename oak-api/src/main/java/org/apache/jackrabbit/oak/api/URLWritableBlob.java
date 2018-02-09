/**************************************************************************
 *
 * ADOBE CONFIDENTIAL
 * __________________
 *
 *  Copyright 2018 Adobe Systems Incorporated
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
 *************************************************************************/

package org.apache.jackrabbit.oak.api;

import java.net.URL;
import javax.annotation.Nullable;

public interface URLWritableBlob extends Blob {

    @Nullable
    URL getWriteURL();

    /**
     * Called when the tree was committed and the blob is referenced at least once in a property.
     * Implementations must generated the write URL inside this method and start any expiry
     * time at this point.
     */
    void commit();
}
