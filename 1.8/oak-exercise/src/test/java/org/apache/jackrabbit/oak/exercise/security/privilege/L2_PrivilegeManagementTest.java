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
package org.apache.jackrabbit.oak.exercise.security.privilege;

import org.apache.jackrabbit.api.JackrabbitSession;
import org.apache.jackrabbit.api.JackrabbitWorkspace;
import org.apache.jackrabbit.api.security.authorization.PrivilegeManager;
import org.apache.jackrabbit.test.AbstractJCRTest;
import org.apache.jackrabbit.test.NotExecutableException;

import javax.jcr.RepositoryException;
import javax.jcr.security.AccessControlManager;
import javax.jcr.security.Privilege;

/**
 * <pre>
 * Module: Privilege Management
 * =============================================================================
 *
 * Title: Privilege Management (Basics)
 * -----------------------------------------------------------------------------
 *
 * Goal:
 * Learn about the retrieving privileges in {@link javax.jcr.security.AccessControlManager)
 * and the API defined by the {@link org.apache.jackrabbit.api.security.authorization.PrivilegeManager}
 * API extension.
 *
 * Exercises:
 *
 * - {@link #testGetPrivilege()}
 *   Use this exercise to become familiar with the different ways to retrieve
 *   a privilege.
 *
 * - {@link #testGetSupportedAndRegisteredPrivileges()}
 *   Learn about the difference between supported and registered privileges.
 *   Based on the API contract compare the results of the 2 methods in the current
 *   implementation and discuss different ways of implementation.
 *
 *   Question: How could you compare the 'supported' with the 'registered' privileges?
 *   Question: What can you say about the difference?
 *   Question: What can you say about the default implementation of {@link AccessControlManager#getSupportedPrivileges(String)}
 *
 *
 * Related Exercises:
 * -----------------------------------------------------------------------------
 *
 * - {@link L3_BuiltInPrivilegesTest}
 * - {@link L4_CustomPrivilegeTest}
 *
 * </pre>
 *
 * @see javax.jcr.security.AccessControlManager
 * @see org.apache.jackrabbit.api.security.authorization.PrivilegeManager
 */
public class L2_PrivilegeManagementTest extends AbstractJCRTest {

    private AccessControlManager accessControlManager;
    private PrivilegeManager privilegeManager;

    @Override
    protected void setUp() throws Exception {
        super.setUp();

        accessControlManager = superuser.getAccessControlManager();

        if (!(superuser instanceof JackrabbitSession)) {
            throw new NotExecutableException("not a JackrabbitSession");
        }
        privilegeManager = ((JackrabbitWorkspace) superuser.getWorkspace()).getPrivilegeManager();
    }

    public void testGetPrivilege() throws RepositoryException {
        String privilegeName = Privilege.JCR_READ;

        Privilege readPriv = null; // EXERCISE: retrieve privilege from 'accessControlManager'
        Privilege readPriv2 = null; // EXERCISE: retrive the privilege from 'privilegeManager'

        assertTrue(readPriv.equals(readPriv2));
    }

    public void testGetSupportedAndRegisteredPrivileges() throws RepositoryException {
        Privilege[] supportedPrivilges = accessControlManager.getSupportedPrivileges(testRoot);
        Privilege[] registered = privilegeManager.getRegisteredPrivileges();

        // EXERCISE: compare the supported and the registered privileges
        // EXERCISE: read the API contract for the 2 methods
    }
}