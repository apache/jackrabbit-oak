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
import java.security.acl.Group;
import java.util.Set;

/**
 * PrincipalProvider... TODO
 */
public interface PrincipalProvider {

    /**
     * Returns the principal with the specified name or {@code null} if the
     * principal does not exist.
     *
     * @param principalName the name of the principal to retrieve
     * @return return the requested principal or {@code null}
     */
    Principal getPrincipal(String principalName);

    /**
     * Returns an iterator over all group principals for which the given
     * principal is either direct or indirect member of. Thus for any principal
     * returned in the iterator {@link java.security.acl.Group#isMember(Principal)}
     * must return {@code true}.
     * <p/>
     * Example:<br>
     * If Principal is member of Group A, and Group A is member of
     * Group B, this method will return Group A and Group B.
     *
     * @param principal the principal to return it's membership from.
     * @return an iterator returning all groups the given principal is member of.
     * @see java.security.acl.Group#isMember(java.security.Principal)
     */
    Set<Group> getGroupMembership(Principal principal);
}