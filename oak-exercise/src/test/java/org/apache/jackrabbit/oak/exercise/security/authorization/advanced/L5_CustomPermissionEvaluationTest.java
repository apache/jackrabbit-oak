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
package org.apache.jackrabbit.oak.exercise.security.authorization.advanced;

import java.security.Principal;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.jcr.GuestCredentials;
import javax.jcr.Session;
import javax.jcr.security.Privilege;

import org.apache.jackrabbit.guava.common.collect.ImmutableList;
import org.apache.jackrabbit.guava.common.collect.Iterables;
import org.apache.jackrabbit.api.JackrabbitSession;
import org.apache.jackrabbit.api.security.authorization.PrivilegeManager;
import org.apache.jackrabbit.oak.AbstractSecurityTest;
import org.apache.jackrabbit.oak.api.ContentSession;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.exercise.security.authorization.models.predefined.Editor;
import org.apache.jackrabbit.oak.exercise.security.authorization.models.predefined.PredefinedAuthorizationConfiguration;
import org.apache.jackrabbit.oak.exercise.security.authorization.models.predefined.Reader;
import org.apache.jackrabbit.oak.plugins.memory.PropertyStates;
import org.apache.jackrabbit.oak.plugins.tree.TreeUtil;
import org.apache.jackrabbit.oak.security.authentication.AuthenticationConfigurationImpl;
import org.apache.jackrabbit.oak.security.authentication.token.TokenConfigurationImpl;
import org.apache.jackrabbit.oak.security.internal.SecurityProviderBuilder;
import org.apache.jackrabbit.oak.security.principal.PrincipalConfigurationImpl;
import org.apache.jackrabbit.oak.security.privilege.PrivilegeConfigurationImpl;
import org.apache.jackrabbit.oak.security.user.UserConfigurationImpl;
import org.apache.jackrabbit.oak.spi.nodetype.NodeTypeConstants;
import org.apache.jackrabbit.oak.spi.security.ConfigurationParameters;
import org.apache.jackrabbit.oak.spi.security.SecurityProvider;
import org.apache.jackrabbit.oak.spi.security.authorization.AuthorizationConfiguration;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.PermissionProvider;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.Permissions;
import org.apache.jackrabbit.oak.spi.security.principal.EveryonePrincipal;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants;
import org.apache.jackrabbit.util.Text;
import org.jetbrains.annotations.NotNull;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * <pre>
 * Module: Advanced Authorization Topics
 * =============================================================================
 *
 * Title: Writing Custom Authorization : Permission Evaluation
 * -----------------------------------------------------------------------------
 *
 * Goal:
 * Write a custom {@link org.apache.jackrabbit.oak.spi.security.authorization.permission.PermissionProvider}
 * for a predefined requirement in order to become familiar with the details of
 * the Oak permission evaluation.
 *
 * Exercises:
 *
 * Complete the implementation of {@link org.apache.jackrabbit.oak.exercise.security.authorization.models.predefined.PredefinedPermissionProvider}
 * such that the tests pass.
 *
 * - {@link #testAdministrativeAccess}
 *   Complete the {@link org.apache.jackrabbit.oak.exercise.security.authorization.models.predefined.PredefinedPermissionProvider}
 *   such that at least the admin principal has full access everywhere.
 *
 *   Questions:
 *   - How can you identify the 'administrator' from a given set of principals?
 *   - Would it make sense to include other principals in that category? How would you identify them?
 *   - Take another look at the built-in authorization models (default and oak-authorization-cug):
 *     Can you describe what types of 'administrative' access they define? And how?
 *
 * - {@link #testGuestAccess()}
 *   The {@link org.apache.jackrabbit.oak.exercise.security.authorization.models.predefined.PredefinedPermissionProvider}
 *   assumes that the guest account doesn't have any permissions granted. Complete
 *   the permission provider implementation accordingly.
 *
 *   Question:
 *   Do you need to explicitly identify the guest account? If yes, how would you do that?
 *
 * - {@link #testWriteAccess()}
 *   This tests asserts that 'editors' have basic read/write permissions. Complete
 *   the permission provider implementation accordingly.
 *
 *   Questions:
 *   - The test hard-codes the 'editor' principal. Can you come up with a setup scenario
 *     where the Editor principal would be placed into the Subject upon login? What are the criteria?
 *   - Can you make sure a given test-user won't be able to map itself to the 'editor' principal?
 *
 * - {@link #testReadAccess()}
 *   This tests asserts that 'readers' exclusively have basic read access. Complete
 *   the permission provider implementation accordingly.
 *
 *
 * Advanced Exercises
 * -----------------------------------------------------------------------------
 *
 * 1. Aggregation
 *
 * Currently the {@link org.apache.jackrabbit.oak.exercise.security.authorization.models.predefined.PredefinedPermissionProvider}
 * doesn't implement {@link org.apache.jackrabbit.oak.spi.security.authorization.permission.AggregatedPermissionProvider} interface
 * and can therefore not be used in a setup that combines multiple authorization models.
 *
 * As an advanced exercise modify the {@link org.apache.jackrabbit.oak.exercise.security.authorization.models.predefined.PredefinedPermissionProvider}
 * to additionally implement {@link org.apache.jackrabbit.oak.spi.security.authorization.permission.AggregatedPermissionProvider}
 * and deploy the {@link PredefinedAuthorizationConfiguration} in a setup with
 * multiple authorization models.
 *
 * - Discuss the additional methods defined by {@link org.apache.jackrabbit.oak.spi.security.authorization.permission.AggregatedPermissionProvider}.
 * - Clarify which type of 'Authorization Composition' your implementation should be used.
 * - Observe the result of your combination and explain the results to effective permissions.
 *
 *
 * 2. Limit Access
 *
 * Currently the predefined {@code PermissionProvider} grants/denies the same permissions
 * on the whole content repository. As an advanced exercise discuss how you would
 * limit the permissions to certain parts of the content repository.
 *
 * For example: Imagine the first hierarchy level would define a trust-boundary
 * based on continent. So, every 'Editor' only has write access to the continent
 * he/she has been assigned to.
 *
 * Questions:
 *
 * - Should read-access be granted across continents?
 * - Do you need a distinction between repository-administrators and continent-administrators?
 * - How do you identify a given continent and map it to the access pattern of a given principal set?
 * - Can you come up with your own PrincipalConfiguration serving a custom Principal implementation that help you with that task?
 * - Are the items used for continent-identification properly protected to prevent unintended (or malicious) meddling?
 * - At which point would you additionally need access control management?
 *
 * </pre>
 */
public class L5_CustomPermissionEvaluationTest extends AbstractSecurityTest {

    private static final String[] ACTION_NAMES = new String[] {
            Session.ACTION_READ, Session.ACTION_ADD_NODE, Session.ACTION_SET_PROPERTY, Session.ACTION_REMOVE
    };

    private List<Tree> trees;
    private PropertyState prop;

    @Override
    protected SecurityProvider initSecurityProvider() {
        AuthorizationConfiguration ac = new PredefinedAuthorizationConfiguration();

        return SecurityProviderBuilder.newBuilder().with(
                new AuthenticationConfigurationImpl(), ConfigurationParameters.EMPTY,
                new PrivilegeConfigurationImpl(), ConfigurationParameters.EMPTY,
                new UserConfigurationImpl(), ConfigurationParameters.EMPTY,
                ac, ConfigurationParameters.EMPTY,
                new PrincipalConfigurationImpl(), ConfigurationParameters.EMPTY,
                new TokenConfigurationImpl(), ConfigurationParameters.EMPTY)
                .with(getSecurityConfigParameters())
                .withRootProvider(getRootProvider())
                .withTreeProvider(getTreeProvider())
                .build();
    }

    @Override
    public void before() throws Exception {
        super.before();

        prop = PropertyStates.createProperty("prop", "value");

        Tree testTree = TreeUtil.addChild(root.getTree("/"), "contentA", NodeTypeConstants.NT_OAK_UNSTRUCTURED);
        Tree aTree = TreeUtil.addChild(testTree, "a", NodeTypeConstants.NT_OAK_UNSTRUCTURED);
        aTree.setProperty(prop);

        Tree aaTree = TreeUtil.addChild(aTree, "a", NodeTypeConstants.NT_OAK_UNSTRUCTURED);
        aaTree.setProperty(prop);

        Tree bTree = TreeUtil.addChild(root.getTree("/"), "contentB", NodeTypeConstants.NT_OAK_UNSTRUCTURED);
        bTree.setProperty(prop);

        Tree bbTree = TreeUtil.addChild(bTree, "b", NodeTypeConstants.NT_OAK_UNSTRUCTURED);
        bbTree.setProperty(prop);

        Tree cTree = TreeUtil.addChild(root.getTree("/"), "contentC", NodeTypeConstants.NT_OAK_UNSTRUCTURED);
        cTree.setProperty(prop);

        Tree ccTree = TreeUtil.addChild(cTree, "c", NodeTypeConstants.NT_OAK_UNSTRUCTURED);
        ccTree.setProperty(prop);

        root.commit();

        trees = ImmutableList.<Tree>builder().add(root.getTree("/")).add(testTree).add(aTree).add(aaTree).add(bTree).add(bbTree).add(cTree).add(ccTree).build();

    }

    private PermissionProvider getPermissionProvider(@NotNull Set<Principal> principals) {
        return getConfig(AuthorizationConfiguration.class).getPermissionProvider(root, adminSession.getWorkspaceName(), principals);
    }

    private Iterable<String> getTreePaths() {
        return Iterables.transform(trees, Tree::getPath);
    }

    private Set<Principal> getGuestPrincipals() throws Exception {
        try (ContentSession guest = login(new GuestCredentials())) {
            return guest.getAuthInfo().getPrincipals();
        }
    }

    @Test
    public void testAdministrativeAccess() {
        for (String path : getTreePaths()) {
            Tree t = root.getTree(path);
            assertFalse(t.exists());
        }

        PermissionProvider pp = getPermissionProvider(adminSession.getAuthInfo().getPrincipals());
        for (Tree t : trees) {
            pp.getPrivileges(t).contains(PrivilegeConstants.JCR_ALL);
            assertTrue(pp.isGranted(t, null, Permissions.ALL));
            assertTrue(pp.isGranted(t, prop, Permissions.ALL));

            String treePath = t.getPath();
            String allActions = Text.implode(ACTION_NAMES, ",");
            assertTrue(pp.isGranted(treePath, allActions));
            assertTrue(pp.isGranted(PathUtils.concat(treePath, prop.getName()), allActions));
        }
    }

    @Test
    public void testGuestAccess() throws Exception {
        try (ContentSession guest = login(new GuestCredentials())) {
            Root r = guest.getLatestRoot();
            for (String path : getTreePaths()) {
                Tree t = r.getTree(path);
                assertFalse(t.exists());
            }

            PermissionProvider pp = getPermissionProvider(guest.getAuthInfo().getPrincipals());
            for (Tree t : trees) {
                pp.getPrivileges(t).isEmpty();
                for (long permission : Permissions.aggregates(Permissions.ALL)) {
                    assertFalse(pp.isGranted(t, null, permission));
                    assertFalse(pp.isGranted(t, prop, permission));
                }

                for (String action : ACTION_NAMES) {
                    String treePath = t.getPath();
                    assertFalse(pp.isGranted(treePath, action));
                    assertFalse(pp.isGranted(PathUtils.concat(treePath, prop.getName()), action));
                }
            }
        }
    }

    @Test
    public void testWriteAccess() throws Exception {
        List<Set<Principal>> editors = ImmutableList.<Set<Principal>>of(
                Set.of(new Editor("ida")),
                Set.of(EveryonePrincipal.getInstance(), new Editor("amanda")),
                Set.of(getTestUser().getPrincipal(),new Editor("susi")),
                Stream.concat(getGuestPrincipals().stream(), Stream.of(new Editor("naima"))).collect(Collectors.toUnmodifiableSet())
        );

        for (Set<Principal> principals : editors) {
            PermissionProvider pp = getPermissionProvider(principals);
            for (Tree t : trees) {
                assertTrue(pp.hasPrivileges(t, PrivilegeConstants.JCR_READ, PrivilegeConstants.JCR_WRITE));

                assertFalse(pp.hasPrivileges(t, PrivilegeConstants.JCR_WRITE, PrivilegeConstants.JCR_NODE_TYPE_MANAGEMENT));
                assertFalse(pp.hasPrivileges(t, PrivilegeConstants.JCR_READ_ACCESS_CONTROL, PrivilegeConstants.JCR_MODIFY_ACCESS_CONTROL, PrivilegeConstants.REP_USER_MANAGEMENT));
                assertFalse(pp.hasPrivileges(t, PrivilegeConstants.JCR_ALL));

                assertTrue(pp.isGranted(t, null, Permissions.WRITE | Permissions.READ));
                assertTrue(pp.isGranted(t, prop, Permissions.WRITE|Permissions.READ));

                assertFalse(pp.isGranted(t, null, Permissions.ALL));
                assertFalse(pp.isGranted(t, prop, Permissions.ALL));

                assertFalse(pp.isGranted(t, null, Permissions.READ_ACCESS_CONTROL|Permissions.MODIFY_ACCESS_CONTROL|Permissions.USER_MANAGEMENT));
                assertFalse(pp.isGranted(t, prop, Permissions.READ_ACCESS_CONTROL | Permissions.MODIFY_ACCESS_CONTROL | Permissions.USER_MANAGEMENT));


                for (String action : ACTION_NAMES) {
                    String treePath = t.getPath();
                    assertTrue(pp.isGranted(treePath, action));
                    assertTrue(pp.isGranted(PathUtils.concat(treePath, prop.getName()), action));
                }

                String deniedActions = Text.implode(new String[] {JackrabbitSession.ACTION_MODIFY_ACCESS_CONTROL, JackrabbitSession.ACTION_READ_ACCESS_CONTROL, JackrabbitSession.ACTION_USER_MANAGEMENT}, ",");
                assertFalse(pp.isGranted(t.getPath(), deniedActions));
            }
        }
    }

    @Test
    public void testReadAccess() throws Exception {
        List<Set<Principal>> readers = ImmutableList.<Set<Principal>>of(
                Set.of(new Reader("ida")),
                Set.of(EveryonePrincipal.getInstance(), new Reader("fairuz")),
                Set.of(getTestUser().getPrincipal(),new Editor("juni")),
                Stream.concat(getGuestPrincipals().stream(), Stream.of(new Editor("ale"))).collect(Collectors.toUnmodifiableSet())
        );

        PrivilegeManager privilegeManager = getPrivilegeManager(root);
        Privilege all = privilegeManager.getPrivilege(PrivilegeConstants.JCR_ALL);
        Set<String> readPrivNames = Set.of(PrivilegeConstants.JCR_READ, PrivilegeConstants.REP_READ_NODES, PrivilegeConstants.REP_READ_PROPERTIES);

        for (Set<Principal> principals : readers) {
            PermissionProvider pp = getPermissionProvider(principals);
            for (Tree t : trees) {
                assertTrue(pp.hasPrivileges(t, readPrivNames.toArray(new String[readPrivNames.size()])));

                for (Privilege p : all.getAggregatePrivileges()) {
                    String pName = p.getName();
                    if (readPrivNames.contains(pName)) {
                        assertTrue(pp.hasPrivileges(t, pName));
                    } else {
                        assertFalse(pp.hasPrivileges(t, pName));
                    }
                }
                assertFalse(pp.hasPrivileges(t, PrivilegeConstants.JCR_ALL, PrivilegeConstants.JCR_READ));

                assertTrue(pp.isGranted(t, null, Permissions.READ));
                assertTrue(pp.isGranted(t, null, Permissions.READ_NODE));
                assertTrue(pp.isGranted(t, prop, Permissions.READ_PROPERTY));

                assertFalse(pp.isGranted(t, null, Permissions.ALL));
                assertFalse(pp.isGranted(t, prop, Permissions.ALL));

                assertFalse(pp.isGranted(t, null, Permissions.WRITE|Permissions.VERSION_MANAGEMENT|Permissions.READ_ACCESS_CONTROL));
                assertFalse(pp.isGranted(t, prop, Permissions.SET_PROPERTY|Permissions.VERSION_MANAGEMENT|Permissions.READ_ACCESS_CONTROL));

                String treePath = t.getPath();
                assertTrue(pp.isGranted(treePath, Session.ACTION_READ));
                assertTrue(pp.isGranted(PathUtils.concat(treePath, prop.getName()), Session.ACTION_READ));

                String deniedActions = Text.implode(new String[] {Session.ACTION_ADD_NODE, Session.ACTION_SET_PROPERTY, Session.ACTION_REMOVE, JackrabbitSession.ACTION_READ_ACCESS_CONTROL}, ",");
                assertFalse(pp.isGranted(t.getPath(), deniedActions));
            }

            assertTrue(pp.isGranted("/path/to/nonexisting/item", Session.ACTION_READ));
            assertFalse(pp.isGranted("/path/to/nonexisting/item", Session.ACTION_SET_PROPERTY));
        }
    }

}
