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

import java.security.Principal;
import java.util.Set;
import java.util.UUID;
import javax.annotation.Nullable;
import javax.jcr.security.Privilege;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import org.apache.jackrabbit.api.security.authorization.PrivilegeManager;
import org.apache.jackrabbit.oak.AbstractSecurityTest;
import org.apache.jackrabbit.oak.spi.security.principal.EveryonePrincipal;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * <pre>
 * Module: Privilege Management
 * =============================================================================
 *
 * Title: Custom Privileges
 * -----------------------------------------------------------------------------
 *
 * Goal:
 * The aim of this exercise is to make you familiar with the API to register
 * custom privileges and provide you with the basic understanding on how those
 * are being evaluated and enforced.
 *
 * Exercises:
 *
 * - {@link #testCustomPrivilege()}
 *   Use this test to verify the nature of the aggregated custom privilege
 *   registered in the setup.
 *
 * - {@link #testJcrAll()}
 *   Verify that our custom privileges properly listed in the aggregated privileges
 *   exposed by jcr:all.
 *
 *   Question: Having completed this test, what can you say about the nature of jcr:all?
 *   Question: What does that mean for the internal representation of jcr:all?
 *
 * - {@link #testHasPrivilege()}
 *   Fix the test case such that the given set of test princials is granted the
 *   custom privileges at the test node.
 *
 *   Question: Would it also work if you would grant jcr:all for any of the test principals?
 *
 *
 * Advanced Exercises:
 * -----------------------------------------------------------------------------
 *
 * As you can see in the permission evaluation implementation custom privileges
 * will NOT be enforced upon read/write to the repository as the nature of the
 * custom privilege is by definition known to the application responsible for
 * the registration and therefore must be evaluated/enforced on the application
 * level as well.
 *
 * - Verify this by taking another look at the Oak internal permission evaluation.
 *
 *   Question: Can you identify which parts in Oak would need to be extended and
 *             which interfaces you would need to implement and plug/replace if
 *             you wanted to enforce your custom privileges in the repository?
 *
 *
 * Related Exercises:
 * -----------------------------------------------------------------------------
 *
 * - {@link L6_JcrAllTest}
 *
 * </pre>
 *
 * @see org.apache.jackrabbit.api.security.authorization.PrivilegeManager#registerPrivilege(String, boolean, String[])
 */
public class L4_CustomPrivilegeTest extends AbstractSecurityTest {

    private PrivilegeManager privilegeManager;
    private Privilege customAbstractPriv;
    private Privilege customAggrPriv;

    @Override
    public void before() throws Exception {
        super.before();

        privilegeManager = getPrivilegeManager(root);
        customAbstractPriv = privilegeManager.registerPrivilege(
                "customAbstractPriv_" + UUID.randomUUID().toString(), true, new String[0]);
        customAggrPriv = privilegeManager.registerPrivilege(
                "customAbstractPriv_" + UUID.randomUUID().toString(), false,
                new String[] {customAbstractPriv.getName(), PrivilegeConstants.JCR_READ});
    }

    private static void assertEqualPrivileges(Set<String> expectedNames, Privilege[] result) {
        if (expectedNames.size() != result.length) {
            fail();
        }

        Iterable<String> resultNames = Iterables.transform(Sets.newHashSet(result), new Function<Privilege, String>() {
            @Nullable
            @Override
            public String apply(Privilege input) {
                return input.toString();
            }
        });

        Iterables.removeAll(resultNames, expectedNames);
        assertFalse(resultNames.iterator().hasNext());
    }

    @Test
    public void testCustomPrivilege() {
        Set<String> expected = null; //EXERCISE
        assertEqualPrivileges(expected, customAggrPriv.getDeclaredAggregatePrivileges());

        expected = null; // EXERCISE
        assertEqualPrivileges(expected, customAggrPriv.getAggregatePrivileges());

        Boolean expectedIsAbstract = null; // EXERCISE
        assertEquals(expectedIsAbstract.booleanValue(), customAggrPriv.isAbstract());
    }

    @Test
    public void testJcrAll() {
        // EXERCISE : verify that the custom privileges are contained in the jcr:all aggregation
    }

    @Test
    public void testHasPrivilege() throws Exception {
        try {
            // EXERCISE : fix the test case such that the test principals have the specified privileges granted at "/"

            Privilege[] testPrivileges = new Privilege[] {customAbstractPriv, customAggrPriv};
            Set<Principal> testPrincipals = ImmutableSet.of(EveryonePrincipal.getInstance(), getTestUser().getPrincipal());
            boolean hasPrivilege = getAccessControlManager(root).hasPrivileges("/", testPrincipals, testPrivileges);

            assertTrue(hasPrivilege);
        } finally {
            // EXERCISE: cleanup the changes.
        }
    }
}