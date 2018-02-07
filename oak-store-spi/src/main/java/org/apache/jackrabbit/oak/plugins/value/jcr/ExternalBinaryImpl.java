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

import javax.jcr.AccessDeniedException;
import javax.jcr.RepositoryException;
import javax.jcr.security.AccessControlManager;
import javax.jcr.security.Privilege;

import org.apache.jackrabbit.oak.api.ExternalBlob;
import org.apache.jackrabbit.oak.api.binary.ExternalBinary;

public class ExternalBinaryImpl extends BinaryImpl implements ExternalBinary {

    private final ExternalBlob externalBlob;
    private String nodePath;
    private AccessControlManager accessControlManager;

    ExternalBinaryImpl(ValueImpl value, ExternalBlob externalBlob) {
        super(value);
        this.externalBlob = externalBlob;
    }

    @Override
    public String getPutURL() throws AccessDeniedException, RepositoryException {
        // check if user can write to binary
        if (accessControlManager != null) {
            boolean canWrite = accessControlManager.hasPrivileges(nodePath, new Privilege[] {
                accessControlManager.privilegeFromName(Privilege.JCR_MODIFY_PROPERTIES)
            });
            if (!canWrite) {
                throw new AccessDeniedException("Cannot write binary");
            }
        }
        return externalBlob.getPutURL();
    }

    public void setJCRContext(String nodePath, AccessControlManager accessControlManager) {
        this.nodePath = nodePath;
        this.accessControlManager = accessControlManager;
    }
}
