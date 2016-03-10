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
package org.apache.jackrabbit.oak.benchmark;

import javax.annotation.Nonnull;

import org.apache.jackrabbit.api.security.user.Group;
import org.apache.jackrabbit.api.security.user.User;

/**
 * Same as {@link MemberBaseTest} but only reading the declared membership.
 */
public class MemberMemberOf extends MemberBaseTest {

    public MemberMemberOf(int numberOfGroups, boolean nested, int numberOfMembers) {
        super(numberOfGroups, nested, numberOfMembers);
    }

    @Override
    protected void testMembership(@Nonnull Group g, @Nonnull User member) throws Exception {
        member.memberOf();
    }
}