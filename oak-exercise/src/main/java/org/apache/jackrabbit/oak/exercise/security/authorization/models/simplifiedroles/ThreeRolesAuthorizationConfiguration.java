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
package org.apache.jackrabbit.oak.exercise.security.authorization.models.simplifiedroles;

import java.io.ByteArrayInputStream;
import java.security.Principal;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.jcr.RepositoryException;
import javax.jcr.security.AccessControlManager;

import com.google.common.collect.ImmutableList;
import org.apache.felix.scr.annotations.Component;
import org.apache.felix.scr.annotations.Deactivate;
import org.apache.felix.scr.annotations.Modified;
import org.apache.felix.scr.annotations.Properties;
import org.apache.felix.scr.annotations.Property;
import org.apache.felix.scr.annotations.Service;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.commons.PropertiesUtil;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeStore;
import org.apache.jackrabbit.oak.plugins.name.NamespaceEditorProvider;
import org.apache.jackrabbit.oak.plugins.nodetype.ReadOnlyNodeTypeManager;
import org.apache.jackrabbit.oak.plugins.nodetype.TypeEditorProvider;
import org.apache.jackrabbit.oak.plugins.nodetype.write.NodeTypeRegistry;
import org.apache.jackrabbit.oak.plugins.tree.TreeLocation;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.CompositeEditorProvider;
import org.apache.jackrabbit.oak.spi.commit.EditorHook;
import org.apache.jackrabbit.oak.spi.commit.MoveTracker;
import org.apache.jackrabbit.oak.spi.commit.Validator;
import org.apache.jackrabbit.oak.spi.commit.ValidatorProvider;
import org.apache.jackrabbit.oak.spi.lifecycle.RepositoryInitializer;
import org.apache.jackrabbit.oak.spi.security.CompositeConfiguration;
import org.apache.jackrabbit.oak.spi.security.ConfigurationBase;
import org.apache.jackrabbit.oak.spi.security.ConfigurationParameters;
import org.apache.jackrabbit.oak.spi.security.Context;
import org.apache.jackrabbit.oak.spi.security.authorization.AuthorizationConfiguration;
import org.apache.jackrabbit.oak.spi.security.authorization.accesscontrol.AccessControlConstants;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.EmptyPermissionProvider;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.PermissionProvider;
import org.apache.jackrabbit.oak.spi.security.authorization.restriction.RestrictionProvider;
import org.apache.jackrabbit.oak.spi.state.ApplyDiff;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.apache.jackrabbit.oak.spi.xml.ProtectedItemImporter;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.jackrabbit.oak.spi.security.RegistrationConstants.OAK_SECURITY_NAME;

@Component(metatype = true, immediate = true, policy = org.apache.felix.scr.annotations.ConfigurationPolicy.REQUIRE)
@Service({AuthorizationConfiguration.class, org.apache.jackrabbit.oak.spi.security.SecurityConfiguration.class})
@Properties({
        @Property(name = "supportedPath",
                label = "Supported Path"),
        @Property(name = CompositeConfiguration.PARAM_RANKING,
                label = "Ranking",
                description = "Ranking of this configuration in a setup with multiple authorization configurations.",
                intValue = 10),
        @Property(name = OAK_SECURITY_NAME,
                propertyPrivate = true,
                value = "org.apache.jackrabbit.oak.exercise.security.authorization.models.simplifiedroles.ThreeRolesAuthorizationConfiguration")
})
public class ThreeRolesAuthorizationConfiguration extends ConfigurationBase implements AuthorizationConfiguration, ThreeRolesConstants {

    private static final Logger log = LoggerFactory.getLogger(ThreeRolesAuthorizationConfiguration.class);


    private String supportedPath;

    @org.apache.felix.scr.annotations.Activate
    private void activate(Map<String, Object> properties) {
        supportedPath = PropertiesUtil.toString(properties.get("supportedPath"), (String) null);
    }

    @Modified
    private void modified(Map<String, Object> properties) {
        supportedPath = PropertiesUtil.toString(properties.get("supportedPath"), (String) null);
    }

    @Deactivate
    private void deactivate(Map<String, Object> properties) {
        supportedPath = null;
    }

    @NotNull
    @Override
    public AccessControlManager getAccessControlManager(@NotNull Root root, @NotNull NamePathMapper namePathMapper) {
        return new ThreeRolesAccessControlManager(root, supportedPath, getSecurityProvider());
    }

    @NotNull
    @Override
    public RestrictionProvider getRestrictionProvider() {
        return RestrictionProvider.EMPTY;
    }

    @NotNull
    @Override
    public PermissionProvider getPermissionProvider(@NotNull Root root, @NotNull String workspaceName, @NotNull Set<Principal> principals) {
        if (supportedPath == null) {
            return EmptyPermissionProvider.getInstance();
        } else {
            return new ThreeRolesPermissionProvider(root, principals, supportedPath, getContext(), getRootProvider());
        }
    }

    @NotNull
    @Override
    public String getName() {
        return AuthorizationConfiguration.NAME;
    }

    @NotNull
    @Override
    public RepositoryInitializer getRepositoryInitializer() {
        String cnd = "<rep='internal'>\n" +
                     "["+MIX_REP_THREE_ROLES_POLICY+"] \n   mixin\n " +
                     "  +"+REP_3_ROLES_POLICY+" ("+NT_REP_THREE_ROLES_POLICY+") protected IGNORE\n\n" +
                     "["+NT_REP_THREE_ROLES_POLICY+"] > "+ AccessControlConstants.NT_REP_POLICY+"\n" +
                     "  - "+REP_READERS +" (STRING) multiple protected IGNORE\n" +
                     "  - "+REP_EDITORS+" (STRING) multiple protected IGNORE\n" +
                     "  - "+REP_OWNERS+" (STRING) multiple protected IGNORE";
        System.out.println(cnd);

        return builder -> {
            NodeState base = builder.getNodeState();
            NodeStore store = new MemoryNodeStore(base);

            Root root = getRootProvider().createSystemRoot(store,
                    new EditorHook(new CompositeEditorProvider(new NamespaceEditorProvider(), new TypeEditorProvider())));

            try {
                if (!ReadOnlyNodeTypeManager.getInstance(root, NamePathMapper.DEFAULT).hasNodeType(MIX_REP_THREE_ROLES_POLICY)) {
                    NodeTypeRegistry.register(root, new ByteArrayInputStream(cnd.getBytes()), "oak exercise");
                        NodeState target = store.getRoot();
                        target.compareAgainstBaseState(base, new ApplyDiff(builder));
                }
            } catch (RepositoryException e) {
                log.error(e.getMessage());
            }
        };
    }

    @NotNull
    @Override
    public List<? extends ValidatorProvider> getValidators(@NotNull String workspaceName, @NotNull Set<Principal> principals, @NotNull MoveTracker moveTracker) {
        return ImmutableList.of(new ValidatorProvider() {
            @Override
            protected Validator getRootValidator(NodeState before, NodeState after, CommitInfo info) {
                // EXERCISE: write a validator that meets the following requirements:
                // 1. check that the item names defined with the node types are only used within the scope of the 2 node types
                // 2. check that the policies are never nested.

                // NOTE: having this validator allows to rely on item names in
                // Context implementation and simplify the permission evaluation
                // i.e. preventing expensive effective node type calculation/verification
                // and checks for nested policies in time critical evaluation steps.
                return null;
            }
        });
    }

    @NotNull
    @Override
    public List<ProtectedItemImporter> getProtectedItemImporters() {
        // EXERCISE
        return ImmutableList.of();
    }

    @NotNull
    @Override
    public Context getContext() {
        /**
         * Simplified implementation of the Context interface that just tests
         * item names without every evaluating node type information.
         * See {@link #getValidators(String, Set, MoveTracker)} above.
         */
        return new Context() {
            @Override
            public boolean definesProperty(@NotNull Tree parent, @NotNull PropertyState property) {
                return definesTree(parent) && NAMES.contains(property.getName());
            }

            @Override
            public boolean definesContextRoot(@NotNull Tree tree) {
                return definesTree(tree);
            }

            @Override
            public boolean definesTree(@NotNull Tree tree) {
                return REP_3_ROLES_POLICY.equals(tree.getName());
            }

            @Override
            public boolean definesLocation(@NotNull TreeLocation location) {
                String name = location.getName();
                return NAMES.contains(name);
            }

            @Override
            public boolean definesInternal(@NotNull Tree tree) {
                return false;
            }
        };
    }

    @Override
    public void setParameters(@NotNull ConfigurationParameters config) {
        super.setParameters(config);
        supportedPath = config.getConfigValue("supportedPath", null, String.class);
    }
}
