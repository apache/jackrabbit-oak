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
package org.apache.jackrabbit.oak.spi.security.authorization.cug.impl;

import java.io.IOException;
import java.io.InputStream;
import java.security.Principal;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.jcr.RepositoryException;
import javax.jcr.security.AccessControlManager;

import org.apache.jackrabbit.guava.common.collect.ImmutableList;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeStore;
import org.apache.jackrabbit.oak.plugins.nodetype.ReadOnlyNodeTypeManager;
import org.apache.jackrabbit.oak.plugins.nodetype.write.NodeTypeRegistry;
import org.apache.jackrabbit.oak.spi.commit.CommitHook;
import org.apache.jackrabbit.oak.spi.commit.MoveTracker;
import org.apache.jackrabbit.oak.spi.commit.ValidatorProvider;
import org.apache.jackrabbit.oak.spi.lifecycle.RepositoryInitializer;
import org.apache.jackrabbit.oak.spi.mount.MountInfoProvider;
import org.apache.jackrabbit.oak.spi.mount.Mounts;
import org.apache.jackrabbit.oak.spi.nodetype.NodeTypeConstants;
import org.apache.jackrabbit.oak.spi.security.ConfigurationBase;
import org.apache.jackrabbit.oak.spi.security.ConfigurationParameters;
import org.apache.jackrabbit.oak.spi.security.Context;
import org.apache.jackrabbit.oak.spi.security.SecurityConfiguration;
import org.apache.jackrabbit.oak.spi.security.SecurityProvider;
import org.apache.jackrabbit.oak.spi.security.authorization.AuthorizationConfiguration;
import org.apache.jackrabbit.oak.spi.security.authorization.cug.CugExclude;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.EmptyPermissionProvider;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.PermissionProvider;
import org.apache.jackrabbit.oak.spi.security.authorization.restriction.RestrictionProvider;
import org.apache.jackrabbit.oak.spi.state.ApplyDiff;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.apache.jackrabbit.oak.spi.xml.ProtectedItemImporter;
import org.jetbrains.annotations.NotNull;
import org.osgi.service.component.annotations.Activate;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.ConfigurationPolicy;
import org.osgi.service.component.annotations.Modified;
import org.osgi.service.component.annotations.Reference;
import org.osgi.service.component.annotations.ReferenceCardinality;
import org.osgi.service.metatype.annotations.AttributeDefinition;
import org.osgi.service.metatype.annotations.Designate;
import org.osgi.service.metatype.annotations.ObjectClassDefinition;

import static org.apache.jackrabbit.oak.spi.security.RegistrationConstants.OAK_SECURITY_NAME;

@Component(
        service = {AuthorizationConfiguration.class, SecurityConfiguration.class},
        property = OAK_SECURITY_NAME + "=org.apache.jackrabbit.oak.spi.security.authorization.cug.impl.CugConfiguration",
        configurationPolicy = ConfigurationPolicy.REQUIRE)
@Designate(ocd = CugConfiguration.Configuration.class)
public class CugConfiguration extends ConfigurationBase implements AuthorizationConfiguration, CugConstants {

    @ObjectClassDefinition(name = "Apache Jackrabbit Oak CUG Configuration",
            description = "Authorization configuration dedicated to setup and evaluate 'Closed User Group' permissions.")
    @interface Configuration {
        @AttributeDefinition(
                name = "Supported Paths",
                description = "Paths under which CUGs can be created and will be evaluated.",
                cardinality = Integer.MAX_VALUE)
        String[] cugSupportedPaths() default {};

        @AttributeDefinition(
                name = "CUG Evaluation Enabled",
                description = "Flag to enable the evaluation of the configured CUG policies.")
        boolean cugEnabled() default false;

        @AttributeDefinition(
                name = "Ranking",
                description = "Ranking of this configuration in a setup with multiple authorization configurations.")
        int configurationRanking() default 200;
    }

    /**
     * Reference to services implementing {@link org.apache.jackrabbit.oak.spi.security.authorization.cug.CugExclude}.
     */
    private CugExclude exclude;

    /**
     * Reference to service implementing {@link MountInfoProvider} to make the
     * CUG authorization model multiplexing aware.
     */
    private MountInfoProvider mountInfoProvider = Mounts.defaultMountInfoProvider();

    private Set<String> supportedPaths = Set.of();

    @SuppressWarnings("UnusedDeclaration")
    public CugConfiguration() {
        super();
    }

    public CugConfiguration(@NotNull SecurityProvider securityProvider) {
        super(securityProvider, securityProvider.getParameters(NAME));
    }

    @NotNull
    @Override
    public AccessControlManager getAccessControlManager(@NotNull Root root, @NotNull NamePathMapper namePathMapper) {
        return new CugAccessControlManager(root, namePathMapper, getSecurityProvider(), supportedPaths, getExclude(), getRootProvider());
    }

    @NotNull
    @Override
    public RestrictionProvider getRestrictionProvider() {
        return RestrictionProvider.EMPTY;
    }

    @NotNull
    @Override
    public PermissionProvider getPermissionProvider(@NotNull Root root, @NotNull String workspaceName, @NotNull Set<Principal> principals) {
        ConfigurationParameters params = getParameters();
        boolean enabled = params.getConfigValue(CugConstants.PARAM_CUG_ENABLED, false);

        if (!enabled || supportedPaths.isEmpty() || getExclude().isExcluded(principals)) {
            return EmptyPermissionProvider.getInstance();
        } else {
            return new CugPermissionProvider(root, workspaceName, principals, supportedPaths, getSecurityProvider().getConfiguration(AuthorizationConfiguration.class).getContext(), getRootProvider(), getTreeProvider());
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
        return builder -> {
            NodeState base = builder.getNodeState();
            NodeStore store = new MemoryNodeStore(base);

            Root root = getRootProvider().createSystemRoot(store, null);
            if (registerCugNodeTypes(root)) {
                NodeState target = store.getRoot();
                target.compareAgainstBaseState(base, new ApplyDiff(builder));
            }
        };
    }

    @NotNull
    @Override
    public List<? extends CommitHook> getCommitHooks(@NotNull String workspaceName) {
        return Collections.singletonList(new NestedCugHook());
    }

    @NotNull
    @Override
    public List<? extends ValidatorProvider> getValidators(@NotNull String workspaceName, @NotNull Set<Principal> principals, @NotNull MoveTracker moveTracker) {
        return ImmutableList.of(new CugValidatorProvider());
    }

    @NotNull
    @Override
    public List<ProtectedItemImporter> getProtectedItemImporters() {
        return Collections.singletonList(new CugImporter(mountInfoProvider));
    }

    @NotNull
    @Override
    public Context getContext() {
        return CugContext.INSTANCE;
    }

    @Override
    public void setParameters(@NotNull ConfigurationParameters config) {
        super.setParameters(config);
        supportedPaths = CugUtil.getSupportedPaths(config, mountInfoProvider);
    }

    //----------------------------------------------------< SCR Integration >---
    @SuppressWarnings("UnusedDeclaration")
    @Activate
    protected void activate(Map<String, Object> properties) {
        setParameters(ConfigurationParameters.of(properties));
    }

    @SuppressWarnings("UnusedDeclaration")
    @Modified
    protected void modified(Map<String, Object> properties) {
        activate(properties);
    }

    @Reference(name="mountInfoProvider")
    public void bindMountInfoProvider(MountInfoProvider mountInfoProvider) {
        this.mountInfoProvider = mountInfoProvider;
    }

    public void unbindMountInfoProvider(MountInfoProvider mountInfoProvider) {
        // set to null (and not default) to comply with OSGi lifecycle,
        // if the reference is unset it means the service is being deactivated
        this.mountInfoProvider = null;
    }

    @Reference(name="exclude", cardinality = ReferenceCardinality.MANDATORY)
    public void bindExclude(CugExclude exclude) {
        this.exclude = exclude;
    }

    public void unbindExclude(CugExclude exclude) {
        this.exclude = null;
    }

    //--------------------------------------------------------------------------
    @NotNull
    private CugExclude getExclude() {
        return (exclude == null) ? new CugExclude.Default() : exclude;
    }

    static boolean registerCugNodeTypes(@NotNull final Root root) {
        try {
            ReadOnlyNodeTypeManager ntMgr = new ReadOnlyNodeTypeManager() {
                @NotNull
                @Override
                protected Tree getTypes() {
                    return root.getTree(NodeTypeConstants.NODE_TYPES_PATH);
                }
            };
            if (!ntMgr.hasNodeType(NT_REP_CUG_POLICY)) {
                try (InputStream stream = CugConfiguration.class.getResourceAsStream("cug_nodetypes.cnd")) {
                    NodeTypeRegistry.register(root, stream, "cug node types");
                    return true;
                }
            }
        } catch (IOException | RepositoryException e) {
            throw new IllegalStateException("Unable to read cug node types", e);
        }
        return false;
    }
}
