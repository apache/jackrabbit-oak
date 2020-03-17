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
package org.apache.jackrabbit.oak.exercise.security.authorization.accesscontrol;

import java.security.Principal;
import java.util.Map;
import javax.jcr.PropertyType;
import javax.jcr.RepositoryException;
import javax.jcr.Value;
import javax.jcr.ValueFactory;
import javax.jcr.security.AccessControlEntry;
import javax.jcr.security.AccessControlManager;
import javax.jcr.security.Privilege;

import com.google.common.collect.ImmutableMap;
import org.apache.jackrabbit.api.JackrabbitSession;
import org.apache.jackrabbit.api.security.JackrabbitAccessControlEntry;
import org.apache.jackrabbit.api.security.JackrabbitAccessControlList;
import org.apache.jackrabbit.api.security.user.Authorizable;
import org.apache.jackrabbit.commons.jackrabbit.authorization.AccessControlUtils;
import org.apache.jackrabbit.oak.exercise.security.authorization.restriction.CustomRestrictionProvider;
import org.apache.jackrabbit.oak.exercise.ExerciseUtility;
import org.apache.jackrabbit.oak.spi.security.authorization.accesscontrol.AccessControlConstants;
import org.apache.jackrabbit.test.AbstractJCRTest;
import org.apache.jackrabbit.test.NotExecutableException;

/**
 * <pre>
 * Module: Authorization (Access Control Management)
 * =============================================================================
 *
 * Title: Restrictions and Restriction Management
 * -----------------------------------------------------------------------------
 *
 * Goal:
 * Become familiar with the concept of additional restrictions added to a given
 * access control entry and the API to read and write them.
 *
 * For simplicity this test make use of the default restrictions provided by Oak.
 * Be aware that these are not built-in constants as additional restrictions
 * are pluggable at runtime. See the advanced exercises.
 *
 * Exercises:
 *
 * - {@link #testApplicableRestrictions()}
 *   This test uses Jackrabbit API methods to obtain the applicable restrictions.
 *   Complete the test such that you also now the required type of the
 *   restrictions.
 *
 *   Question: Can you determine from the Jackrabbit API if the restriction is multivalued?
 *
 * - {@link #testAddEntryWithRestrictions()}
 *   Create an new ACE with a single valued restriction like e.g. the path globbing
 *   restriction.
 *
 * - {@link #testAddEntryWithMultiValuedRestriction()}
 *   Create an new ACE with multiple restrictions mixing both single and multi-
 *   valued restrictions.
 *
 * - {@link #testRetrieveRestrictionsFromACE()}
 *   This test creates an ACE with restrictions. Complete the test by verifying
 *   your expectations wrt restrictions present on the ACE.
 *
 *
 * Advanced Exercises:
 * -----------------------------------------------------------------------------
 *
 * While the restriction API provided by Jackrabbit API is rather limited the
 * Oak internal way to handle, store and read these restictions is a bit
 * more elaborate.
 *
 * Use the Oak code base and the documentation at
 * http://jackrabbit.apache.org/oak/docs/security/accesscontrol/restriction.html
 * to complete the following additional exercises.
 *
 * - Take a look at the interfaces and classes defined in
 *   {@code org.apache.jackrabbit.oak.spi.security.authorization.restriction}
 *
 * - Investigate how you could plug your custom restriction provider and try
 *   to implement it according to the instructions on the Oak.
 *   Use the stub at {@link CustomRestrictionProvider}
 *   to complete this exercise.
 *
 * - Make your custom restriction provider an OSGi service and deploy it in a
 *   OSGi-base repository setup like Sling (Granite|CQ). Use a low-level
 *   repository browser tool (or a test) to create ACEs making use of the custom
 *   restrictions you decided to implement.
 *
 * </pre>
 *
 * @see <a href="http://jackrabbit.apache.org/oak/docs/security/accesscontrol/restriction.html">Restriction Management Documentation</a>
 */
public class L7_RestrictionsTest extends AbstractJCRTest {


    private AccessControlManager acMgr;
    private JackrabbitAccessControlList acl;

    private Principal testPrincipal;
    private Privilege[] testPrivileges;

    @Override
    protected void setUp() throws Exception {
        super.setUp();

        acMgr = superuser.getAccessControlManager();

        testPrincipal = ExerciseUtility.createTestGroup(((JackrabbitSession) superuser).getUserManager()).getPrincipal();
        superuser.save();

        acl = AccessControlUtils.getAccessControlList(superuser, testRoot);
        if (acl == null) {
            throw new NotExecutableException();
        }

        testPrivileges = AccessControlUtils.privilegesFromNames(acMgr, Privilege.JCR_READ, Privilege.JCR_WRITE);
    }

    @Override
    protected void tearDown() throws Exception {
        try {
            Authorizable testGroup = ((JackrabbitSession) superuser).getUserManager().getAuthorizable(testPrincipal);
            if (testGroup != null) {
                testGroup.remove();
                superuser.save();
            }
        } finally {
            super.tearDown();
        }
    }

    public void testApplicableRestrictions() throws RepositoryException {
        String[] restrictionNames = acl.getRestrictionNames();

        for (String name : restrictionNames) {
            int type = acl.getRestrictionType(name);
            int expectedType = PropertyType.UNDEFINED;

            if (AccessControlConstants.REP_GLOB.equals(name)) {
                expectedType = PropertyType.UNDEFINED; // EXERCISE
            } else if (AccessControlConstants.REP_NT_NAMES.equals(name)) {
                expectedType = PropertyType.UNDEFINED; // EXERCISE
            } else if (AccessControlConstants.REP_PREFIXES.equals(name)) {
                expectedType = PropertyType.UNDEFINED; // EXERCISE
            }
            assertEquals(expectedType, type);
        }
    }

    public void testAddEntryWithRestrictions() throws RepositoryException {
        // EXERCISE : create the restriction map containing a globbing pattern.
        Map<String, Value> restrictions = null;

        assertTrue(acl.addEntry(testPrincipal, testPrivileges, false, restrictions));
    }

    public void testAddEntryWithMultiValuedRestriction() throws RepositoryException {
        // EXERCISE : create the restriction map containing a globbing pattern.
        Map<String, Value> restrictions = null;

        // EXERCISE : create a map with the multi-valued restrictions as well.
        Map<String, Value[]> mvRestrictions = null;

        assertTrue(acl.addEntry(testPrincipal, testPrivileges, false, restrictions, mvRestrictions));
    }

    public void testRetrieveRestrictionsFromACE() throws RepositoryException {
        ValueFactory vf = superuser.getValueFactory();

        acl.addEntry(testPrincipal, testPrivileges, false,
                ImmutableMap.of(AccessControlConstants.REP_GLOB, vf.createValue("/*")),
                ImmutableMap.of(AccessControlConstants.REP_PREFIXES, new Value[] {vf.createValue("jcr"), vf.createValue("rep")})
        );

        for (AccessControlEntry ace : acl.getAccessControlEntries()) {
            if (ace instanceof JackrabbitAccessControlEntry) {
                JackrabbitAccessControlEntry jace = (JackrabbitAccessControlEntry) ace;

                // EXERCISE retrieve the restriction names present on the ace and verify your expectations.
                // EXERCISE retrieve the restriction values for each restriction and verify your expectations.
            }
        }
    }
}