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
package org.apache.jackrabbit.api.security.principal;

import java.security.Principal;
import java.util.Enumeration;

import org.jetbrains.annotations.NotNull;
import org.osgi.annotation.versioning.ProviderType;

/**
 * This interface is used to represent a group of principals. It is meant to
 * replace the deprecated {@code java.security.acl.Group}.
 */
@ProviderType
public interface GroupPrincipal extends Principal {

    /**
     * Returns true if the passed principal is a member of the group.
     * This method does a recursive search, so if a principal belongs to a
     * group which is a member of this group, true is returned.
     *
     * @param member the principal whose membership is to be checked.
     * @return true if the principal is a member of this group,
     * false otherwise.
     */
    boolean isMember(@NotNull Principal member);

    /**
     * Returns an enumeration of the members in the group. This includes both
     * declared members and all principals that are indirect group members. The
     * returned objects can be instances of either Principal or GroupPrincipal
     * (which is a subclass of Principal).
     *
     * @return an enumeration of the group members.
     */
    @NotNull
    Enumeration<? extends Principal> members();

}
