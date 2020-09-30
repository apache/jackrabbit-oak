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
package org.apache.jackrabbit.oak.spi.security.principal;

import java.security.Principal;
import java.util.Collections;
import java.util.Enumeration;

import org.apache.jackrabbit.api.security.principal.GroupPrincipal;
import org.jetbrains.annotations.NotNull;

/**
 * Helper class to deal with the migration between the 2 types of groups
 *
 */
public final class GroupPrincipals {

    private GroupPrincipals() {
    }

    /**
     * Checks if the provided principal is a group.
     *
     * @param principal
     *            to be checked.
     *
     * @return true if the principal is of type group.
     */
    public static boolean isGroup(@NotNull Principal principal) {
        return principal instanceof GroupPrincipal;
    }

    /**
     * Returns an enumeration of the members in the group.
     * @param principal the principal whose membership is listed.
     * @return an enumeration of the group members.
     */
    @NotNull
    public static Enumeration<? extends Principal> members(@NotNull Principal principal) {
        if (principal instanceof GroupPrincipal) {
            return ((GroupPrincipal) principal).members();
        }
        return Collections.emptyEnumeration();
    }

    /**
     * Returns true if the passed principal is a member of the group.
     * @param principal the principal whose members are being checked.
     * @param member the principal whose membership is to be checked.
     * @return true if the principal is a member of this group, false otherwise.
     */
    public static boolean isMember(@NotNull Principal principal, @NotNull Principal member) {
        if (principal instanceof GroupPrincipal) {
            return ((GroupPrincipal) principal).isMember(member);
        }
        return false;
    }
}
