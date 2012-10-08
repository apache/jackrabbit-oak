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
package org.apache.jackrabbit.oak.spi.security.user;

import java.util.Iterator;

import javax.annotation.Nonnull;

import org.apache.jackrabbit.oak.api.Tree;

/**
 * MembershipProvider... TODO
 */
public interface MembershipProvider {

    @Nonnull
    Iterator<String> getMembership(String authorizableId, boolean includeInherited);

    @Nonnull
    Iterator<String> getMembership(Tree authorizableTree, boolean includeInherited);

    @Nonnull
    Iterator<String> getMembers(String groupId, AuthorizableType authorizableType, boolean includeInherited);

    @Nonnull
    Iterator<String> getMembers(Tree groupTree, AuthorizableType authorizableType, boolean includeInherited);

    boolean isMember(Tree groupTree, Tree authorizableTree, boolean includeInherited);

    boolean addMember(Tree groupTree, Tree newMemberTree);

    boolean removeMember(Tree groupTree, Tree memberTree);
}
