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

/**
 * {@code ExternalIdentityException} is used to notify about errors when dealing with external identities.
 */
public class ExternalIdentityException extends Exception {

    public ExternalIdentityException() {
    }

    public ExternalIdentityException(String message) {
        super(message);
    }

    public ExternalIdentityException(String message, Throwable cause) {
        super(message, cause);
    }

    public ExternalIdentityException(Throwable cause) {
        super(cause);
    }
}