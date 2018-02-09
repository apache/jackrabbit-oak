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

package org.apache.jackrabbit.oak.plugins.value.jcr;

import java.net.URL;
import javax.jcr.AccessDeniedException;
import javax.jcr.RepositoryException;
import javax.jcr.security.AccessControlManager;
import javax.jcr.security.Privilege;

import org.apache.jackrabbit.oak.api.URLWritableBlob;
import org.apache.jackrabbit.oak.api.binary.URLWritableBinary;

public class URLWritableBinaryImpl extends BinaryImpl implements URLWritableBinary {

    private final URLWritableBlob urlWritableBlob;

    URLWritableBinaryImpl(ValueImpl value, URLWritableBlob urlWritableBlob) {
        super(value);
        this.urlWritableBlob = urlWritableBlob;
    }

    @Override
    public URL getWriteURL() throws RepositoryException {
        return urlWritableBlob.getWriteURL();
    }

}
