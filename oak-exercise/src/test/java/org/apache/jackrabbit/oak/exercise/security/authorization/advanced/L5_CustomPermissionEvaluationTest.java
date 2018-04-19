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
import javax.annotation.Nonnull;
import javax.jcr.GuestCredentials;
import javax.jcr.Session;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import org.apache.jackrabbit.oak.AbstractSecurityTest;
import org.apache.jackrabbit.oak.api.ContentSession;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.exercise.security.authorization.models.predefined.PredefinedAuthorizationConfiguration;
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
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants;
import org.apache.jackrabbit.util.Text;
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
 *
 * Advanced Exercise
 * -----------------------------------------------------------------------------
 *
 * Currently the {@link org.apache.jackrabbit.oak.exercise.security.authorization.models.predefined.PredefinedPermissionProvider}
 * doesn't implement {@link org.apache.jackrabbit.oak.spi.security.authorization.permission.AggregatedPermissionProvider} interface
 * and can therefore not be used in a setup that combines multipe authorization models.
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

    private PermissionProvider getPermissionProvider(@Nonnull Set<Principal> principals) {
        return getConfig(AuthorizationConfiguration.class).getPermissionProvider(root, adminSession.getWorkspaceName(), principals);
    }

    private Iterable<String> getTreePaths() {
        return Iterables.transform(trees, Tree::getPath);
    }

    @Test
    public void testAdministratorHasFullAccessEverywhere() {
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
    public void testGuestHasNowherePermissions() throws Exception {
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

    // TODO: add more tests
}