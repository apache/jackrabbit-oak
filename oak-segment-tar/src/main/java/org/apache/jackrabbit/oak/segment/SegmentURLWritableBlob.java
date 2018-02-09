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

package org.apache.jackrabbit.oak.segment;

import javax.annotation.Nonnull;

import org.apache.jackrabbit.oak.api.URLWritableBlob;
import org.apache.jackrabbit.oak.spi.blob.URLWritableBlobStore;

public class SegmentURLWritableBlob extends SegmentBlob implements URLWritableBlob {

    private final URLWritableBlobStore urlWritableBlobStore;

    SegmentURLWritableBlob(@Nonnull URLWritableBlobStore urlWritableBlobStore,
                           @Nonnull RecordId id) {
        super(urlWritableBlobStore, id);
        this.urlWritableBlobStore = urlWritableBlobStore;
    }

    @Override
    public String getPutURL() {
        return urlWritableBlobStore.getPutURL(getBlobId());
    }
}
