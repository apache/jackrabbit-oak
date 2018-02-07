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

import org.apache.jackrabbit.oak.api.ExternalBlob;
import org.apache.jackrabbit.oak.spi.blob.ExternalBlobStore;

public class SegmentExternalBlob extends SegmentBlob implements ExternalBlob {

    private final ExternalBlobStore extBlobStore;

    SegmentExternalBlob(@Nonnull ExternalBlobStore extBlobStore,
                        @Nonnull RecordId id) {
        super(extBlobStore, id);
        this.extBlobStore = extBlobStore;
    }

    @Override
    public String getPutURL() {
        return extBlobStore.getPutURL(getBlobId());
    }
}
