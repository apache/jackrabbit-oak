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

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.security.Principal;
import java.util.Map;
import java.util.Set;
import javax.jcr.ImportUUIDBehavior;
import javax.jcr.Node;
import javax.jcr.Repository;
import javax.jcr.Session;
import javax.jcr.nodetype.NodeDefinition;
import javax.jcr.nodetype.PropertyDefinition;
import javax.jcr.security.AccessControlManager;
import javax.jcr.security.AccessControlPolicy;
import javax.jcr.security.AccessControlPolicyIterator;
import javax.jcr.security.NamedAccessControlPolicy;

import org.apache.jackrabbit.api.security.principal.PrincipalManager;
import org.apache.jackrabbit.oak.AbstractSecurityTest;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.ContentSession;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.exercise.security.authorization.models.simplifiedroles.ThreeRolesAuthorizationConfiguration;
import org.apache.jackrabbit.oak.exercise.security.authorization.models.simplifiedroles.ThreeRolesConstants;
import org.apache.jackrabbit.oak.exercise.security.principal.CustomPrincipalConfiguration;
import org.apache.jackrabbit.oak.jcr.Jcr;
import org.apache.jackrabbit.oak.jcr.repository.RepositoryImpl;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.plugins.nodetype.ReadOnlyNodeTypeManager;
import org.apache.jackrabbit.oak.plugins.tree.TreeUtil;
import org.apache.jackrabbit.oak.security.authorization.composite.CompositeAuthorizationConfiguration;
import org.apache.jackrabbit.oak.security.internal.SecurityProviderHelper;
import org.apache.jackrabbit.oak.spi.nodetype.NodeTypeConstants;
import org.apache.jackrabbit.oak.spi.security.ConfigurationParameters;
import org.apache.jackrabbit.oak.spi.security.SecurityProvider;
import org.apache.jackrabbit.oak.spi.security.authorization.AuthorizationConfiguration;
import org.apache.jackrabbit.oak.spi.security.authorization.accesscontrol.PolicyOwner;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.PermissionProvider;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.Permissions;
import org.apache.jackrabbit.oak.spi.security.principal.EveryonePrincipal;
import org.apache.jackrabbit.oak.spi.security.principal.PrincipalConfiguration;
import org.apache.jackrabbit.oak.spi.whiteboard.DefaultWhiteboard;
import org.apache.jackrabbit.oak.spi.xml.ProtectedNodeImporter;
import org.jetbrains.annotations.NotNull;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * <pre>
 * Module: Advanced Authorization Topics
 * =============================================================================
 *
 * Title: Writing Custom Authorization : Access Control Management
 * -----------------------------------------------------------------------------
 *
 * Goal:
 * Learn how to write your own access control management and how to properly
 * secure access control content.
 * The exercises is this lesson will make use of a authorization model stub that
 * already has the permission evaluation implemented. The entry point of that model
 * is {@link ThreeRolesAuthorizationConfiguration}.
 *
 * Exercises:
 *
 * - {@link #testGetPolicies()}
 *   Complete the implementation of {@link AccessControlManager#getPolicies(String)}
 *   such that the test passes.
 *   Adjust the number of expected policies and the type of policies according
 *   to your implementation.
 *
 *   Questions:
 *   - what type of policy do you want to expose?
 *   - does any of the existing types of access control policies fit your needs?
 *     existing types include
 *     > {@link NamedAccessControlPolicy},
 *     > {@link javax.jcr.security.AccessControlList},
 *     > {@link org.apache.jackrabbit.api.security.JackrabbitAccessControlList},
 *     > {@link org.apache.jackrabbit.api.security.authorization.PrincipalSetPolicy}
 *   - if you choose to define your own policy type: what does it look like?
 *   - should {@link AccessControlManager#getPolicies(String)} return one or many policies?
 *
 * - {@link #testGetEffectivePolicies()}
 *   Complete the implementation of {@link AccessControlManager#getEffectivePolicies(String)}
 *   such that the test passes.
 *   NOTE: computation of effective policies is specified to be a best-effort operation.
 *
 *   Questions:
 *   - what type of policy do you want to expose?
 *   - what are the effective policies for those nodes that don't have the custom
 *     policy set? or in other words: which nodes are affected by the policy set
 *     at /test/a in the test setup?
 *   - does the set of effective policies include any default policies that have
 *     not been explicit set?
 *   - what's the maximal number of effective policies your implementation may return?
 *
 * - {@link #testGetApplicablePolicies()}
 *   Complete the implementation of {@link AccessControlManager#testGetApplicablePolicies(String)}
 *   such that the test passes.
 *
 *   Questions:
 *   - what type of policies do you expose here?
 *   - are they they same as with {@link #testGetPolicies()} and/or {@link #testGetEffectivePolicies()}?
 *   - does /test/a still have applicable policies?
 *   - what about the path outside of the tree defined by 'supportedPath' configuration option?
 *   - as you learned in the previous section and can see in {@link org.apache.jackrabbit.oak.exercise.security.authorization.models.simplifiedroles.ThreeRolesPermissionProvider}
 *     this simplified authorization model doesn't respect nesting of policies in a
 *     given tree. what does that mean for the applicable policies?
 *   - what's the maximal number of applicable policies your implementation may return at a given path?
 *
 * - {@link #testSetPolicy()}
 *   Implement {@link AccessControlManager#setPolicy(String, AccessControlPolicy)} and
 *   {@link PolicyOwner#defines(String, AccessControlPolicy)} such that policies
 *   can be written to the repository in a composite authorization setup.
 *   The {@link PolicyOwner} is also required for the subsequent tests.
 *
 * - {@link #testSetModifiedPolicy()}
 *   Modify the custom access control setup at /test/a such that the principal
 *   associated with the test-user get moved from the editor to the owner set.
 *
 * - {@link #testRemovePolicy()}
 *   Implement {@link AccessControlManager#removePolicy(String, AccessControlPolicy)} such
 *   that the test passes.
 *
 * - {@link #testAccessControlContentIsProtected()}
 *   Your authorization setup should come with some validation of the access control
 *   content written to the repository.
 *   Write a {@link org.apache.jackrabbit.oak.spi.commit.ValidatorProvider} and
 *   plug it into the authorization configuration such that the test-case passes
 *   and discuss each of the assertions made.
 *
 *   Questions:
 *   - Can you identify possible shortcomings with the 4 validation steps proposed?
 *   - Under which circumstances could any of them might not be desirable?
 *
 * - {@link #testAccessControlItemsAreProtectedByNodeTypeDefinition()}
 *   Identify the code in the simplifiedroles authorization model that makes sure
 *   the simple policy node and it's properties have JCR item definitions that are
 *   protected.
 *
 *   Discuss why this is needed and what the effect of this measure is.
 *   Complete the test case by
 *   - testing the protected status of access control content using JCR API calls.
 *   - verifying the protected status using JCR write API
 *
 *   Question:
 *   - Can you identify which parts of Oak are responsible for enforcing the protected status?
 *
 * - {@link #testImportNodeWithPolicy}
 *   This is a follow-up on {@link #testAccessControlItemsAreProtectedByNodeTypeDefinition()} as
 *   the protected item definitions are not only enforced upon regular write
 *   operations but also when calling {@link javax.jcr.Session#importXML(String, InputStream, int)},
 *   {@link javax.jcr.Workspace#importXML(String, InputStream, int)} and related calls.
 *
 *   Fix the simplifiedroles authorization model such that the test passes.
 *   Hint: you need to implement a custom implementation of {@link ProtectedNodeImporter}
 *   and ensure the {@link AuthorizationConfiguration#getProtectedItemImporters()}
 *   exposes it to the security setup.
 *
 * - {@link #testImportNodeWithPolicyAndUnknownPrincipal}
 *   Variant of {@link #testImportNodeWithPolicy} that attempts to import a policy
 *   referring to an {@code Principal} that is not known to any of the providers.
 *
 *   Questions:
 *   - What do you need to do to the setup and possibly your importer code such
 *     that importing unknown principals is allowed?
 *   - In case your importer didn't check the validity of the principals:
 *     Discuss the adjustments you would need to make to your importer in order
 *     to enforce the different levels of validation check.
 *
 *
 * Advanced Exercise
 * -----------------------------------------------------------------------------
 *
 * The AccessControlManager stub doesn't implement {@link org.apache.jackrabbit.api.security.JackrabbitAccessControlManager}.
 * Take a look at the additional methods defined by the extension in Jackrabbit API.
 *
 * As you can see the extra methods all related to access control management by
 * {@link Principal}.
 *
 * Questions:
 * - Would it be possible/sensible to have your implementation additionally implement
 *   the methods {@link org.apache.jackrabbit.api.security.JackrabbitAccessControlManager}?
 *
 * - If you think it's possible, what would the implementation look like?
 *
 * - Can you spot any obstacles with that approach?
 *
 *
 * Related Exercises:
 * -----------------------------------------------------------------------------
 *
 * - {@link L5_CustomPermissionEvaluationTest}
 *
 * </pre>
 */
public class L4_CustomAccessControlManagementTest extends AbstractSecurityTest {

    @Override
    protected SecurityProvider initSecurityProvider() {
        ThreeRolesAuthorizationConfiguration threeRolesAuthorizationConfiguration = new ThreeRolesAuthorizationConfiguration();
        threeRolesAuthorizationConfiguration.setParameters(ConfigurationParameters.of("supportedPath", "/test"));

        CustomPrincipalConfiguration pc = new CustomPrincipalConfiguration();
        pc.setParameters(ConfigurationParameters.of("knownPrincipals", new String[] {"principalR", "principalE", "principalO"}));

        SecurityProvider sp = super.initSecurityProvider();
        SecurityProviderHelper.updateConfig(sp, threeRolesAuthorizationConfiguration, AuthorizationConfiguration.class);
        SecurityProviderHelper.updateConfig(sp, pc, PrincipalConfiguration.class);

        return sp;
    }

    @Override
    protected ConfigurationParameters getSecurityConfigParameters() {
        return ConfigurationParameters.of("authorizationCompositionType", CompositeAuthorizationConfiguration.CompositionType.OR.toString());
    }

    @Override
    public void before() throws Exception {
        super.before();

        Tree testTree = TreeUtil.addChild(root.getTree("/"), "test", NodeTypeConstants.NT_OAK_UNSTRUCTURED);
        Tree aTree = TreeUtil.addChild(testTree, "a", NodeTypeConstants.NT_OAK_UNSTRUCTURED);
        aTree.setProperty("aProp", "value");

        Tree abTree = TreeUtil.addChild(aTree, "b", NodeTypeConstants.NT_OAK_UNSTRUCTURED);
        abTree.setProperty("abProp", "value");


        TreeUtil.addMixin(aTree, ThreeRolesConstants.MIX_REP_THREE_ROLES_POLICY, root.getTree(NodeTypeConstants.NODE_TYPES_PATH), null);
        Tree rolePolicy = TreeUtil.addChild(aTree, ThreeRolesConstants.REP_3_ROLES_POLICY, ThreeRolesConstants.NT_REP_THREE_ROLES_POLICY);
        rolePolicy.setProperty(ThreeRolesConstants.REP_READERS, Set.of("principalR", EveryonePrincipal.NAME), Type.STRINGS);
        rolePolicy.setProperty(ThreeRolesConstants.REP_EDITORS, Set.of("principalE",getTestUser().getPrincipal().getName()), Type.STRINGS);
        rolePolicy.setProperty(ThreeRolesConstants.REP_OWNERS,  Set.of("principalO"), Type.STRINGS);

        // add one node outside the scope of the supported path
        Tree outside = TreeUtil.addChild(root.getTree("/"), "outside", NodeTypeConstants.NT_OAK_UNSTRUCTURED);

        root.commit();

        // to verify that setup with CompositeAuthorizationConfiguration.CompositionType.OR
        // uncomment the lines below
        /*
        try (ContentSession cs = createTestSession()) {
            Root r = createTestSession().getLatestRoot();
            assertTrue(r.getTree("/test/a").exists());
        }
        */
    }

    private AccessControlManager getAcManager(@NotNull Root root) {
        return getConfig(AuthorizationConfiguration.class).getAccessControlManager(root, NamePathMapper.DEFAULT);
    }

    private Repository buildJcrRepository() {
        return new RepositoryImpl(
                getContentRepository(),
                new DefaultWhiteboard(),
                getSecurityProvider(),
                Jcr.DEFAULT_OBSERVATION_QUEUE_LENGTH,
                null,
                false,
                true);
    }


    /**
     * EXERCISE: complete {@link AccessControlManager#getPolicies(String)} such that
     * the policy that has been 'manually' created in the setup is properly exposed
     * by the access control management API.
     */
    @Test
    public void testGetPolicies() throws Exception {
        AccessControlPolicy[] policies = getAcManager(root).getPolicies("/test/a");

        int len = -1; // EXERCISE: set expected length. 1 is the minimum but there might be more.
        assertEquals(len, policies.length);

        // EXERCISE: additionally assert that the policies is of the type you defined
        for (int i = 0; i < len; i++) {
            assertTrue(policies[i] instanceof AccessControlPolicy); // EXERCISE: replace by type chosen!
        }
    }

    @Test
    public void testGetEffectivePolicies() throws Exception {
        // EXERCISE: set expected number of effective policies for all paths in the map.
        Map<String,Integer> m = Map.of("/", -1, "/test", -1, "/test/a/b", -1, "/outside", -1);

        for (String path : m.keySet()) {
            AccessControlPolicy[] policies = getAcManager(root).getEffectivePolicies(path);

            int len = m.get(path); // EXERCISE: set expected length. 1 is the minimum but there might be more.
            assertEquals(len, policies.length);

            // EXERCISE: additionally assert that the policies is of the type you defined
            for (int i = 0; i < len; i++) {
                assertTrue(policies[i] instanceof AccessControlPolicy); // EXERCISE: replace by type chosen!
            }
        }
    }

    @Test
    public void testGetApplicablePolicies() throws Exception {
        // EXERCISE: set expected number of applicable policies for all paths in the map.
        Map<String,Integer> m = Map.of("/test/a", -1, "/test/a/b", -1, "/outside", -1);

        for (String path : m.keySet()) {
            AccessControlPolicyIterator it = getAcManager(root).getApplicablePolicies(path);
            assertEquals(m.get(path).longValue(), it.getSize());

            // EXERCISE: additionally assert that the policies is of the type you defined
            while (it.hasNext()) {
                assertTrue(it.nextAccessControlPolicy() instanceof AccessControlPolicy); // EXERCISE: replace by type chosen!
            }
        }
    }

    @Test
    public void testSetPolicy() throws Exception {
        Tree t = TreeUtil.addChild(root.getTree("/test"), "another", NodeTypeConstants.NT_OAK_UNSTRUCTURED);
        AccessControlManager acMgr = getAcManager(root);
        AccessControlPolicy[] policies = acMgr.getPolicies(t.getPath());

        // EXERCISE: set your custom policy/policies at /test/another such that
        //           the following assertions pass.
        // ... write your code here

        root.commit();

        PrincipalManager pm = getPrincipalManager(root);
        Map<Principal,Long> m = Map.of(
                getTestUser().getPrincipal(), Permissions.NO_PERMISSION,
                pm.getEveryone(), Permissions.NO_PERMISSION,
                pm.getPrincipal("principalR"), Permissions.READ,
                pm.getPrincipal("principalE"), Permissions.NO_PERMISSION,
                pm.getPrincipal("principalO"), ThreeRolesConstants.SUPPORTED_PERMISSIONS
        );
        for (Principal principal : m.keySet()) {
            PermissionProvider pp = getConfig(AuthorizationConfiguration.class).getPermissionProvider(root, adminSession.getWorkspaceName(), Set.of(principal));
            assertTrue(pp.isGranted(t, null, m.get(principal)));
        }
    }

    @Test
    public void testSetModifiedPolicy() throws Exception {
        AccessControlManager acMgr = getAcManager(root);
        AccessControlPolicy[] policies = acMgr.getPolicies("/test/a");

        // EXERCISE: modify policies such that the testuser principal becomes owner instead of editor
        // ... write your code here

        for (AccessControlPolicy policy : policies) {
            acMgr.setPolicy("/test/a", policy);
        }
        root.commit();

        try (ContentSession cs = createTestSession()) {
            Root r = createTestSession().getLatestRoot();
            PermissionProvider pp = getConfig(AuthorizationConfiguration.class).getPermissionProvider(r, cs.getWorkspaceName(), cs.getAuthInfo().getPrincipals());
            assertTrue(pp.isGranted("/test/a", Permissions.getString(ThreeRolesConstants.SUPPORTED_PERMISSIONS)));
        }
    }

    @Test
    public void testRemovePolicy() throws Exception {
        AccessControlManager acMgr = getAcManager(root);
        for (AccessControlPolicy policy : acMgr.getPolicies("/test/a")) {
            acMgr.removePolicy("/test/a", policy);
        }
        root.commit();
        assertEquals(0, acMgr.getPolicies("/test/a").length);

    }

    @Test
    public void testAccessControlContentIsProtected() throws Exception {
        Tree test = root.getTree("/test");

        try {
            Tree missingMixin = TreeUtil.addChild(test, ThreeRolesConstants.REP_3_ROLES_POLICY, ThreeRolesConstants.NT_REP_THREE_ROLES_POLICY);
            root.commit();
            fail("Adding policy without mixin must fail.");
        } catch (CommitFailedException e) {
            // success
        }

        try {
            test.setProperty(ThreeRolesConstants.REP_OWNERS, 437);
            root.commit();
            fail("Using name of protected policy property outside of the context of a policy must fail.");
        } catch (CommitFailedException e) {
            // success
        }

        try {
            Tree b = root.getTree("/test/a/b");
            TreeUtil.addMixin(b, ThreeRolesConstants.MIX_REP_THREE_ROLES_POLICY, root.getTree(NodeTypeConstants.NODE_TYPES_PATH), null);
            Tree nestedPolicy = TreeUtil.addChild(b, ThreeRolesConstants.REP_3_ROLES_POLICY, ThreeRolesConstants.NT_REP_THREE_ROLES_POLICY);
            root.commit();

            fail("Creation of nested three-roles-policy must fail (NOTE: this is an arbitrary limitation for the sake of simplifying permission evaluation).");
        } catch (CommitFailedException e) {
            // success
        }

        try {
            Tree outside = root.getTree("/outside");
            TreeUtil.addMixin(outside, ThreeRolesConstants.MIX_REP_THREE_ROLES_POLICY, root.getTree(NodeTypeConstants.NODE_TYPES_PATH), null);
            Tree nestedPolicy = TreeUtil.addChild(outside, ThreeRolesConstants.REP_3_ROLES_POLICY, ThreeRolesConstants.NT_REP_THREE_ROLES_POLICY);
            root.commit();

            fail("Creation of nested three-roles-policy outside of the configured supported path must fail.");
        } catch (CommitFailedException e) {
            // success
        }

    }

    @Test
    public void testAccessControlItemsAreProtectedByNodeTypeDefinition() throws Exception {
        ReadOnlyNodeTypeManager ntMgr = ReadOnlyNodeTypeManager.getInstance(root, NamePathMapper.DEFAULT);

        Tree aTree = root.getTree("/test/a");
        Tree policyTree = aTree.getChild(ThreeRolesConstants.REP_3_ROLES_POLICY);

        NodeDefinition policyDef = ntMgr.getDefinition(aTree, policyTree);
        assertTrue(policyDef.isProtected());

        for (String propName : new String[] {ThreeRolesConstants.REP_READERS, ThreeRolesConstants.REP_EDITORS, ThreeRolesConstants.REP_OWNERS}) {
            PropertyDefinition propDef = ntMgr.getDefinition(policyTree, policyTree.getProperty(propName), true);
            assertTrue(propDef.isProtected());
        }

        Repository jcrRepository = buildJcrRepository();

        // EXERCISE: test protected status of items using JCR API calls
        // EXERCISE: verify that the protected status of the access control content is enforced
        // ... write your code here
    }

    @Test
    public void testImportNodeWithPolicy() throws Exception {
        Repository jcrRepository = new RepositoryImpl(
                getContentRepository(),
                new DefaultWhiteboard(),
                getSecurityProvider(),
                Jcr.DEFAULT_OBSERVATION_QUEUE_LENGTH,
                null,
                false,
                false);

        Session adminSession = jcrRepository.login(getAdminCredentials(), null);
        try {
            String xml = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>" +
                    "<sv:node sv:name=\"another2\" xmlns:mix=\"http://www.jcp.org/jcr/mix/1.0\" xmlns:nt=\"http://www.jcp.org/jcr/nt/1.0\" xmlns:fn_old=\"http://www.w3.org/2004/10/xpath-functions\" xmlns:fn=\"http://www.w3.org/2005/xpath-functions\" xmlns:xs=\"http://www.w3.org/2001/XMLSchema\" xmlns:sv=\"http://www.jcp.org/jcr/sv/1.0\" xmlns:rep=\"internal\" xmlns:jcr=\"http://www.jcp.org/jcr/1.0\">" +
                        "<sv:property sv:name=\"jcr:primaryType\" sv:type=\"Name\"><sv:value>oak:Unstructured</sv:value></sv:property>" +
                        "<sv:property sv:name=\"jcr:mixinTypes\" sv:type=\"Name\"><sv:value>rep:ThreeRolesMixin</sv:value></sv:property>" +
                        "<sv:node sv:name=\"rep:threeRolesPolicy\" " +
                            "<sv:property sv:name=\"jcr:primaryType\" sv:type=\"Name\"><sv:value>rep:ThreeRolesPolicy</sv:value></sv:property>" +
                            "<sv:property sv:name=\"rep:readers\" sv:type=\"String\"><sv:value>principalR</sv:value></sv:property>" +
                        "</sv:node>" +
                    "</sv:node>";

            adminSession.importXML("/test", new ByteArrayInputStream(xml.getBytes()), ImportUUIDBehavior.IMPORT_UUID_COLLISION_THROW);

            Node n = adminSession.getNode("/test/another");
            AccessControlPolicy[] policies = adminSession.getAccessControlManager().getPolicies("/test/another");
            assertTrue(policies.length > 0);

        } finally {
            adminSession.refresh(false);
            adminSession.logout();
        }
    }

    @Test
    public void testImportNodeWithPolicyAndUnknownPrincipal() throws Exception {
        Repository jcrRepository = buildJcrRepository();

        Session adminSession = jcrRepository.login(getAdminCredentials(), null);
        try {
            String xml = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>" +
                    "<sv:node sv:name=\"another2\" xmlns:mix=\"http://www.jcp.org/jcr/mix/1.0\" xmlns:nt=\"http://www.jcp.org/jcr/nt/1.0\" xmlns:fn_old=\"http://www.w3.org/2004/10/xpath-functions\" xmlns:fn=\"http://www.w3.org/2005/xpath-functions\" xmlns:xs=\"http://www.w3.org/2001/XMLSchema\" xmlns:sv=\"http://www.jcp.org/jcr/sv/1.0\" xmlns:rep=\"internal\" xmlns:jcr=\"http://www.jcp.org/jcr/1.0\">" +
                        "<sv:property sv:name=\"jcr:primaryType\" sv:type=\"Name\"><sv:value>oak:Unstructured</sv:value></sv:property>" +
                        "<sv:property sv:name=\"jcr:mixinTypes\" sv:type=\"Name\"><sv:value>rep:ThreeRolesMixin</sv:value></sv:property>" +
                        "<sv:node sv:name=\"rep:threeRolesPolicy\" " +
                            "<sv:property sv:name=\"jcr:primaryType\" sv:type=\"Name\"><sv:value>rep:ThreeRolesPolicy</sv:value></sv:property>" +
                            "<sv:property sv:name=\"rep:readers\" sv:type=\"String\"><sv:value>unknownPrincipal</sv:value></sv:property>" +
                        "</sv:node>" +
                    "</sv:node>";

            adminSession.importXML("/test", new ByteArrayInputStream(xml.getBytes()), ImportUUIDBehavior.IMPORT_UUID_COLLISION_THROW);

            Node n = adminSession.getNode("/test/another");
            AccessControlPolicy[] policies = adminSession.getAccessControlManager().getPolicies("/test/another");
            assertTrue(policies.length > 0);

        } finally {
            adminSession.refresh(false);
            adminSession.logout();
        }
    }
}
