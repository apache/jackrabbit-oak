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

package org.apache.jackrabbit.oak.api.binary;

import javax.jcr.RepositoryException;

public interface URLWritableBinaryValueFactory {

    /**
     * Creates a new URLWritableBinary for uploading to the binary content through a URL instead of
     * an InputStream passed through the JCR API, if supported.
     *
     * A typical use case is if the repository is backed by a binary cloud storage such as S3, where
     * the binary can be uploaded to S3 directly.
     *
     * Note that the write URL of the binary can only be retrieved after it has been set as a binary
     * Property on a Node and after the session has been successfully persisted.
     *
     * If the underlying data store does not support this feature, {@code null}
     * is returned and the binary has to be passed in directly using InputStream as in JCR 2.0.
     *
     * @return a new URLWritableBinary or {@code null} if external binaries are not supported
     */
    URLWritableBinary createURLWritableBinary() throws RepositoryException;
}
