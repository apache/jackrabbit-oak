/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.oak.security.authorization.permission;

import org.apache.jackrabbit.oak.spi.security.authorization.permission.Permissions;
import org.apache.jackrabbit.oak.spi.security.authorization.restriction.RestrictionPattern;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeBits;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants;

final class ReadStatus {

    private static final int THIS = 1;
    private static final int PROPERTIES = 2;
    private static final int CHILD_NODES = 4;
    private static final int THIS_PROPERTIES = THIS | PROPERTIES;
    private static final int ALL = THIS | PROPERTIES | CHILD_NODES;

    static final ReadStatus ALLOW_THIS = new ReadStatus(THIS, true);
    static final ReadStatus ALLOW_THIS_PROPERTIES = new ReadStatus(THIS_PROPERTIES, true);
    static final ReadStatus ALLOW_ALL = new ReadStatus(ALL, true);
    static final ReadStatus DENY_THIS = new ReadStatus(THIS, false);
    static final ReadStatus DENY_THIS_PROPERTIES = new ReadStatus(THIS_PROPERTIES, false);
    static final ReadStatus DENY_ALL = new ReadStatus(ALL, false);

    private static final PrivilegeBits READ_BITS = PrivilegeBits.BUILT_IN.get(PrivilegeConstants.JCR_READ);
    private static final PrivilegeBits READ_PROPERTIES_BITS = PrivilegeBits.BUILT_IN.get(PrivilegeConstants.REP_READ_PROPERTIES);

    private final int status;
    private final boolean isAllow;

    private ReadStatus(int status, boolean isAllow) {
        this.status = status;
        this.isAllow = isAllow;
    }

    static ReadStatus create(PermissionEntry pe, long permission, boolean skipped) {
        /*
        best effort: read status is only calculated if
        - no permission entries have been filtered out (e.g. an entry that
          only applies to certain properties and thus not to the target tree itself)
        - the target does not define access control content
        - the matching entry doesn't contain any restrictions
        */
        if (skipped || permission == Permissions.READ_ACCESS_CONTROL || pe.restriction != RestrictionPattern.EMPTY) {
            return (pe.isAllow) ? ALLOW_THIS : DENY_THIS;
        } else {
            if (pe.privilegeBits.includes(READ_BITS)) {
                return (pe.isAllow) ? ALLOW_ALL : DENY_ALL;
            } else if (pe.privilegeBits.includes(READ_PROPERTIES_BITS)) {
                return (pe.isAllow) ? ALLOW_THIS_PROPERTIES : DENY_THIS_PROPERTIES;
            } else {
                return (pe.isAllow) ? ALLOW_THIS : DENY_THIS;
            }
        }
    }

    boolean allowsThis() {
        return isAllow && ((status & THIS) == THIS);
    }

    boolean allowsProperties() {
        return isAllow && ((status & PROPERTIES) == PROPERTIES);
    }

    boolean allowsAll() {
        // NOTE: calculation of allows-all requires knowledge of permissions defined in the subtree
        return false;
    }
}