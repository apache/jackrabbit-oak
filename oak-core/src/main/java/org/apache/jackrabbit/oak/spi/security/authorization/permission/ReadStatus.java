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

    public static final ReadStatus ALLOW_THIS = new ReadStatus(1, true);
    public static final ReadStatus ALLOW_CHILDREN = new ReadStatus(2, true);
    public static final ReadStatus ALLOW_NODES = new ReadStatus(3, true);
    public static final ReadStatus ALLOW_PROPERTIES = new ReadStatus(4, true);
    public static final ReadStatus ALLOW_THIS_PROPERTIES = new ReadStatus(5, true);
    public static final ReadStatus ALLOW_CHILDITEMS = new ReadStatus(6, true);
    public static final ReadStatus ALLOW_ALL = new ReadStatus(7, true);

    public static final ReadStatus DENY_THIS = new ReadStatus(1, false);
    public static final ReadStatus DENY_CHILDREN = new ReadStatus(2, false);
    public static final ReadStatus DENY_NODES = new ReadStatus(3, false);
    public static final ReadStatus DENY_PROPERTIES = new ReadStatus(4, false);
    public static final ReadStatus DENY_THIS_PROPERTIES = new ReadStatus(5, false);
    public static final ReadStatus DENY_CHILDITEMS = new ReadStatus(6, false);
    public static final ReadStatus DENY_ALL = new ReadStatus(7, false);

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
    public static ReadStatus getChildStatus(@Nullable ReadStatus parentStatus) {
        if (parentStatus == null) {
            return null;
        }
        // TODO
        switch (parentStatus.status) {
            case 1: return null; // recalculate for child items
            case 2:
            case 3: return (parentStatus.isAllow) ? ALLOW_THIS : DENY_THIS;
            case 4:
            case 5: return null; // recalculate for properties of child node
            case 6:
            case 7: return (parentStatus.isAllow) ? ALLOW_ALL : DENY_ALL;
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
        return status == 7;
    }

    public boolean appliesToThis() {
        return status == 1;
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
