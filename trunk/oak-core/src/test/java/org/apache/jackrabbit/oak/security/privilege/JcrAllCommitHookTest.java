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
package org.apache.jackrabbit.oak.security.privilege;

import java.util.Set;
import javax.jcr.security.Privilege;

import com.google.common.collect.Sets;
import org.apache.jackrabbit.api.security.authorization.PrivilegeManager;
import org.apache.jackrabbit.oak.AbstractSecurityTest;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants;
import org.junit.Test;

import static org.junit.Assert.assertTrue;

public class JcrAllCommitHookTest extends AbstractSecurityTest {

    private PrivilegeManager privilegeManager;
    private Privilege newPrivilege;

    @Override
    public void before() throws Exception {
        super.before();

        privilegeManager = getPrivilegeManager(root);
        newPrivilege = privilegeManager.registerPrivilege("abstractPrivilege", true, null);
    }

    @Test
    public void testJcrAll() throws Exception {
        Privilege all = privilegeManager.getPrivilege(PrivilegeConstants.JCR_ALL);
        Set<Privilege> aggregates = Sets.newHashSet(all.getDeclaredAggregatePrivileges());

        assertTrue(aggregates.contains(newPrivilege));
    }
}