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
package org.apache.jackrabbit.oak.spi.security.authorization.permission;

import javax.annotation.CheckForNull;
import javax.annotation.Nullable;

import org.apache.jackrabbit.oak.security.privilege.PrivilegeBits;

/**
 * ReadStatus... TODO
 */
public class ReadStatus {

    private static final int STATUS_THIS = 1;
    private static final int STATUS_CHILDREN = 2;
    private static final int STATUS_NODES = 3;
    private static final int STATUS_PROPERTIES = 4;
    private static final int STATUS_THIS_PROPERTIES = 5;
    private static final int STATUS_CHILDITEMS = 6;
    private static final int STATUS_ALL_REGULAR = 7;
    private static final int STATUS_ACCESS_CONTROL = 8;
    private static final int STATUS_ALL = 15;

    public static final ReadStatus ALLOW_THIS = new ReadStatus(STATUS_THIS, true);
    public static final ReadStatus ALLOW_CHILDREN = new ReadStatus(STATUS_CHILDREN, true);
    public static final ReadStatus ALLOW_NODES = new ReadStatus(STATUS_NODES, true);
    public static final ReadStatus ALLOW_PROPERTIES = new ReadStatus(STATUS_PROPERTIES, true);
    public static final ReadStatus ALLOW_THIS_PROPERTIES = new ReadStatus(STATUS_THIS_PROPERTIES, true);
    public static final ReadStatus ALLOW_CHILDITEMS = new ReadStatus(STATUS_CHILDITEMS, true);
    public static final ReadStatus ALLOW_ALL_REGULAR = new ReadStatus(STATUS_ALL_REGULAR, true);
    public static final ReadStatus ALLOW_ACCESS_CONTROL = new ReadStatus(STATUS_ACCESS_CONTROL, true);
    public static final ReadStatus ALLOW_ALL = new ReadStatus(STATUS_ALL, true);

    public static final ReadStatus DENY_THIS = new ReadStatus(STATUS_THIS, false);
    public static final ReadStatus DENY_CHILDREN = new ReadStatus(STATUS_CHILDREN, false);
    public static final ReadStatus DENY_NODES = new ReadStatus(STATUS_NODES, false);
    public static final ReadStatus DENY_PROPERTIES = new ReadStatus(STATUS_PROPERTIES, false);
    public static final ReadStatus DENY_THIS_PROPERTIES = new ReadStatus(STATUS_THIS_PROPERTIES, false);
    public static final ReadStatus DENY_CHILDITEMS = new ReadStatus(STATUS_CHILDITEMS, false);
    public static final ReadStatus DENY_ALL_REGULAR = new ReadStatus(STATUS_ALL_REGULAR, false);
    public static final ReadStatus DENY_ACCESS_CONTROL = new ReadStatus(STATUS_ACCESS_CONTROL, false);
    public static final ReadStatus DENY_ALL = new ReadStatus(STATUS_ALL, false);

    private final int status;
    private final boolean isAllow;

    private ReadStatus(int status, boolean isAllow) {
        this.status = status;
        this.isAllow = isAllow;
    }

    @CheckForNull
    public static ReadStatus getInstance(PrivilegeBits pb, boolean isAllow) {
        if (pb.includesRead(Permissions.READ)) {
            return (isAllow) ? ReadStatus.ALLOW_ALL : ReadStatus.DENY_ALL;
        } else if (pb.includesRead(Permissions.READ_NODE)) {
            return (isAllow) ? ReadStatus.ALLOW_NODES : ReadStatus.DENY_NODES;
        } else if (pb.includesRead(Permissions.READ_PROPERTY)) {
            return (isAllow) ? ReadStatus.ALLOW_PROPERTIES : ReadStatus.DENY_PROPERTIES;
        } else {
            return null;
        }
    }

    @CheckForNull
    public static ReadStatus getChildStatus(@Nullable ReadStatus parentStatus,
                                            boolean hasAcChildren) {
        if (parentStatus == null) {
            return null;
        }
        // TODO
        switch (parentStatus.status) {
            case STATUS_THIS:
                return null; // recalculate for child items
            case STATUS_CHILDREN:
            case STATUS_NODES:
                if (hasAcChildren) {
                    return null;
                } else {
                    return (parentStatus.isAllow) ? ALLOW_NODES : DENY_NODES;
                }
            case STATUS_PROPERTIES:
            case STATUS_THIS_PROPERTIES:
                // TODO
                return null; // recalculate for properties of child node
            case STATUS_CHILDITEMS:
            case STATUS_ALL_REGULAR:
                if (hasAcChildren) {
                    return null;
                } else {
                    return (parentStatus.isAllow) ? ALLOW_ALL_REGULAR : DENY_ALL_REGULAR;
                }
            case STATUS_ACCESS_CONTROL:
                // TODO
                return null; // recalculate
            case STATUS_ALL:
                return (parentStatus.isAllow) ? ALLOW_ALL : DENY_ALL;
            default: throw new IllegalArgumentException("invalid status");
        }
    }

    public boolean includes(ReadStatus status) {
        if (this == status) {
            return true;
        } else {
            return isAllow == status.isAllow && Permissions.includes(this.status, status.status);
        }
    }

    public boolean isAllow() {
        return isAllow;
    }

    public boolean isAll() {
        return status == STATUS_ALL;
    }

    public boolean appliesToThis() {
        return status == STATUS_THIS;
    }

    public int getStatus() {
        return status;
    }

    //-------------------------------------------------------------< Object >---
    @Override
    public String toString() {
        return "ReadStatus : " + (isAllow ? "allow " : "deny ") + status;
    }
}
