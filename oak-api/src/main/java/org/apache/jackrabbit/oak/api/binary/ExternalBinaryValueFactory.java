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

import javax.jcr.Binary;
import javax.jcr.RepositoryException;

public interface ExternalBinaryValueFactory {

    /**
     * Creates a new external binary as a placeholder.
     *
     * This allows upload of the binary stream directly to cloud storage such as S3 after the
     * session has been persisted.
     *
     * If the underlying data store does not support this, {@code null}
     * is returned and the binary has to be passed in directly using InputStream as in JCR 2.0.
     *
     * @return a new external Binary placeholder or {@code null} if external binaries are not supported
     */
    Binary createNewExternalBinary() throws RepositoryException;
}
