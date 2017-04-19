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

import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;
import javax.jcr.RepositoryException;
import javax.jcr.security.AccessControlManager;
import javax.jcr.security.AccessControlPolicy;
import javax.jcr.security.Privilege;

import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import org.apache.jackrabbit.api.security.JackrabbitAccessControlList;
import org.apache.jackrabbit.api.security.authorization.PrivilegeManager;
import org.apache.jackrabbit.commons.jackrabbit.authorization.AccessControlUtils;
import org.apache.jackrabbit.oak.AbstractSecurityTest;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.ContentSession;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * <pre>
 * Module: Privilege Management
 * =============================================================================
 *
 * Title: The Built-in Privileges
 * -----------------------------------------------------------------------------
 *
 * Goal:
 * Understand what built-in privileges are provided by JCR specification and
 * the Oak repository and what type of operations and items they take
 * effect upon.
 *
 * Exercises:
 *
 * - Privilege Overview
 *   List all privileges defined by JSR 283 and Oak and mark if they are
 *   aggregated privileges (if yes: what's the aggregation). Try to understand
 *   the meaning of the individual privileges.
 *
 * - {@link #testAggregation()}
 *   For all built-in aggregated privileges defined the mapping of the name
 *   to the declared aggregated privileges such that the test passes.
 *
 *   Question: What can you say about the nature of {@link Privilege#JCR_READ}
 *   Question: Review again what JSR 283 states about {@link Privilege#JCR_ALL}
 *
 * - {@link #testMapItems()}
 *   This allows you to become familiar with the mapping of individual privileges
 *   to items. Use the Oak API or individual pluggins to change those items
 *   (instead of using the corresponding JCR API call).
 *   Use the JCR specification and the Oak security documentation to learn about
 *   the mapping between privileges and items|operations.
 *
 *   Question: Can you map the items to the corresponding JCR API calls? (see additional exercises below)
 *   Question: Can you extract the rules when a dedicated specific privilege is
 *   being used instead of regular JCR write privileges?
 *
 *
 * Additional Exercises:
 * -----------------------------------------------------------------------------
 *
 * - Create a new test-case similar to {@link #testMapItems()} extending from
 *   {@link org.apache.jackrabbit.test.AbstractJCRTest} and verify your findings
 *   by executing the corresponding JCR API calls.
 *
 * Related Exercises:
 * -----------------------------------------------------------------------------
 *
 * - {@link L4_CustomPrivilegeTest}
 *
 * </pre>
 *
 * @see javax.jcr.security.Privilege
 * @see org.apache.jackrabbit.api.security.authorization.PrivilegeManager
 * @see javax.jcr.security.AccessControlManager#privilegeFromName(String)
 * @see org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants
 */
public class L3_BuiltInPrivilegesTest extends AbstractSecurityTest {

    private ContentSession testSession;
    private Root testRoot;

    private AccessControlManager acMgr;

    @Override
    public void before() throws Exception {
        super.before();

        testSession = createTestSession();
        testRoot = testSession.getLatestRoot();

        acMgr = getAccessControlManager(root);
    }

    @Override
    public void after() throws Exception {
        try {
            if (testSession != null) {
                testSession.close();
            }
        } finally {
            super.after();
        }
    }

    @Test
    public void testAggregation() throws RepositoryException {
        PrivilegeManager privilegeManager = getPrivilegeManager(root);

        // EXERCISE: for all aggregated privileges define the mapping of the privilege name to declaredAggregates
        Map<String, Set<Privilege>> expectedResults = ImmutableMap.of(
                /* EXERCISE */
        );

        Iterable<Privilege> aggregated = Iterables.<Privilege>filter(
                ImmutableList.<Privilege>copyOf(privilegeManager.getRegisteredPrivileges()),
                new Predicate<Privilege>() {
                    @Override
                    public boolean apply(@Nullable Privilege input) {
                        return input != null && input.isAggregate();
                    }
                });

        for (Privilege aggrPrivilege : aggregated) {
            Set<Privilege> expected = expectedResults.get(aggrPrivilege.getName());
            assertEquals(expected, ImmutableSet.copyOf(aggrPrivilege.getDeclaredAggregatePrivileges()));
        }
    }

    @Test
    public void testMapItems() throws Exception {
        Privilege jcrAll = acMgr.privilegeFromName(PrivilegeConstants.JCR_ALL);
        for (Privilege privilege : jcrAll.getAggregatePrivileges()) {
            try {
                // EXERCISE : modify item(s) affected by the given privilege and verify that it fails

                setupAcl(privilege, acMgr);
                testRoot.refresh();

                // EXERCISE : modify the same item(s) again and verify that it succeeds

            } finally {
                clearAcl(acMgr);
                testRoot.refresh();
            }

        }
    }

    private void setupAcl(Privilege privilege, AccessControlManager acMgr) throws Exception {
        JackrabbitAccessControlList acl = AccessControlUtils.getAccessControlList(acMgr, "/");
        acl.addEntry(getTestUser().getPrincipal(), new Privilege[] {privilege}, true);
        acMgr.setPolicy("/", acl);
        root.commit();
    }

    private void clearAcl(AccessControlManager acMgr) throws RepositoryException, CommitFailedException {
        AccessControlPolicy[] policies = acMgr.getPolicies("/");
        for (AccessControlPolicy policy : policies) {
            acMgr.removePolicy("/", policy);
        }
        root.commit();
    }
}