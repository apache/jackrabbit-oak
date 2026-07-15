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
package org.apache.jackrabbit.oak.spi.security.authorization.principalbased.impl;

import org.apache.jackrabbit.api.security.principal.ItemBasedPrincipal;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.junit.Before;
import org.junit.Test;

import java.security.Principal;

import static org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants.JCR_READ;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class MoveSystemUserTest extends AbstractPrincipalBasedTest {

    private Principal principal;

    @Before
    public void before() throws Exception {
        super.before();
        principal = getTestSystemUser().getPrincipal();
        setupPrincipalBasedAccessControl(principal, "/content", JCR_READ);
        root.commit();
        PrincipalPolicyImpl policy = getPrincipalPolicyImpl(principal, getAccessControlManager(root));
        assertEquals(1, policy.size());
    }

    @Test
    public void testMoveUser() throws Exception {
        String srcJcrPath = getTestSystemUser().getPath();
        String name = PathUtils.getName(srcJcrPath);
        String destJcrPath = PathUtils.concat(PathUtils.getParentPath(srcJcrPath), name + "-moved");
        assertTrue(root.move(getNamePathMapper().getOakPath(srcJcrPath), getNamePathMapper().getOakPath(destJcrPath)));
        root.commit();

        assertNull(getUserManager(root).getAuthorizableByPath(srcJcrPath));

        Principal p = getPrincipalManager(root).getPrincipal(principal.getName());
        assertNotNull(p);
        assertTrue(p instanceof ItemBasedPrincipal);
        assertEquals(destJcrPath, ((ItemBasedPrincipal) p).getPath());
        PrincipalPolicyImpl policy = getPrincipalPolicyImpl(p, getAccessControlManager(root));
        assertEquals(1, policy.size());
    }
}