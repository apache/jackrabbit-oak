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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
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
import org.apache.jackrabbit.oak.spi.mount.Mount;
import org.apache.jackrabbit.oak.spi.mount.MountInfoProvider;
import org.apache.jackrabbit.oak.spi.nodetype.NodeTypeConstants;
import org.apache.jackrabbit.oak.spi.security.ConfigurationBase;
import org.apache.jackrabbit.oak.spi.security.ConfigurationParameters;
import org.apache.jackrabbit.oak.spi.security.Context;
import org.apache.jackrabbit.oak.spi.security.SecurityConfiguration;
import org.apache.jackrabbit.oak.spi.security.authorization.AuthorizationConfiguration;
import org.apache.jackrabbit.oak.spi.security.authorization.accesscontrol.AccessControlConstants;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.EmptyPermissionProvider;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.PermissionProvider;
import org.apache.jackrabbit.oak.spi.security.authorization.principalbased.Filter;
import org.apache.jackrabbit.oak.spi.security.authorization.principalbased.FilterProvider;
import org.apache.jackrabbit.oak.spi.security.authorization.restriction.RestrictionProvider;
import org.apache.jackrabbit.oak.spi.state.ApplyDiff;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.apache.jackrabbit.oak.spi.xml.ProtectedItemImporter;
import org.jetbrains.annotations.NotNull;
import org.osgi.service.component.annotations.Activate;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.Modified;
import org.osgi.service.component.annotations.Reference;
import org.osgi.service.component.annotations.ReferenceCardinality;
import org.osgi.service.metatype.annotations.AttributeDefinition;
import org.osgi.service.metatype.annotations.Designate;
import org.osgi.service.metatype.annotations.ObjectClassDefinition;

import javax.jcr.RepositoryException;
import javax.jcr.security.AccessControlManager;
import java.io.IOException;
import java.io.InputStream;
import java.security.Principal;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.jackrabbit.oak.spi.security.RegistrationConstants.OAK_SECURITY_NAME;

@Component(
        service = {AuthorizationConfiguration.class, SecurityConfiguration.class},
        property = OAK_SECURITY_NAME + "=org.apache.jackrabbit.oak.spi.security.authorization.principalbased.impl.PrincipalBasedAuthorizationConfiguration")
@Designate(ocd = PrincipalBasedAuthorizationConfiguration.Configuration.class)
public class PrincipalBasedAuthorizationConfiguration extends ConfigurationBase implements AuthorizationConfiguration {

    @ObjectClassDefinition(name = "Apache Jackrabbit Oak Principal Based AuthorizationConfiguration")
    @interface Configuration {
        @AttributeDefinition(
                name = "Ranking",
                description = "Ranking of this configuration in a setup with multiple authorization configurations.")
        int configurationRanking() default 500;
    }

    /**
     * Reference to service implementing {@link FilterProvider} to define the principals for which this module should take effect.
     */
    private FilterProvider filterProvider;

    /**
     * Reference to service implementing {@link MountInfoProvider}
     */
    private MountInfoProvider mountInfoProvider;

    @SuppressWarnings("UnusedDeclaration")
    public PrincipalBasedAuthorizationConfiguration() {
        super();
    }

    @NotNull
    @Override
    public AccessControlManager getAccessControlManager(@NotNull Root root, @NotNull NamePathMapper namePathMapper) {
        return new PrincipalBasedAccessControlManager(new MgrProviderImpl(this, root, namePathMapper), filterProvider);
    }

    @NotNull
    @Override
    public RestrictionProvider getRestrictionProvider() {
        return getParameters().getConfigValue(AccessControlConstants.PARAM_RESTRICTION_PROVIDER, RestrictionProvider.EMPTY, RestrictionProvider.class);
    }

    @NotNull
    @Override
    public PermissionProvider getPermissionProvider(@NotNull Root root, @NotNull String workspaceName, @NotNull Set<Principal> principals) {
        Filter f = filterProvider.getFilter(getSecurityProvider(), getRootProvider().createReadOnlyRoot(root), NamePathMapper.DEFAULT);
        if (!f.canHandle(principals)) {
            return EmptyPermissionProvider.getInstance();
        } else {
            Iterable<String> principalPaths = Iterables.transform(principals, principal -> f.getOakPath(principal));
            return new PrincipalBasedPermissionProvider(root, workspaceName, principalPaths, this);
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
            if (registerNodeTypes(root)) {
                NodeState target = store.getRoot();
                target.compareAgainstBaseState(base, new ApplyDiff(builder));
            }
        };
    }

    @NotNull
    @Override
    public List<? extends CommitHook> getCommitHooks(@NotNull String workspaceName) {
        return Collections.emptyList();
    }

    @NotNull
    @Override
    public List<? extends ValidatorProvider> getValidators(@NotNull String workspaceName, @NotNull Set<Principal> principals, @NotNull MoveTracker moveTracker) {
        return ImmutableList.of(new PrincipalPolicyValidatorProvider(new MgrProviderImpl(this), principals, workspaceName));
    }

    @NotNull
    @Override
    public List<ProtectedItemImporter> getProtectedItemImporters() {
        return Collections.<ProtectedItemImporter>singletonList(new PrincipalPolicyImporter(filterProvider, new MgrProviderImpl(this)));
    }

    @NotNull
    @Override
    public Context getContext() {
        return ContextImpl.INSTANCE;
    }

    //----------------------------------------------------< SCR Integration >---
    @Activate
    public void activate(@NotNull Configuration configuration, @NotNull Map<String, Object> properties) {
        checkConflictingMount();
        setParameters(ConfigurationParameters.of(properties));
    }

    @Modified
    public void modified(@NotNull Configuration configuration, @NotNull Map<String, Object> properties) {
        activate(configuration, properties);
    }

    @Reference(name = "filterProvider", cardinality = ReferenceCardinality.MANDATORY)
    public void bindFilterProvider(@NotNull FilterProvider filterProvider) {
        this.filterProvider = filterProvider;
    }

    public void unbindFilterProvider(@NotNull FilterProvider filterProvider) {
        this.filterProvider = null;
    }

    @Reference(name = "mountInfoProvider", cardinality = ReferenceCardinality.MANDATORY)
    public void bindMountInfoProvider(@NotNull MountInfoProvider mountInfoProvider) {
        this.mountInfoProvider = mountInfoProvider;
    }

    public void unbindMountInfoProvider(@NotNull MountInfoProvider mountInfoProvider) {
        // set to null (and not default) to comply with OSGi lifecycle,
        // if the reference is unset it means the service is being deactivated
        this.mountInfoProvider = null;
    }

    //--------------------------------------------------------------------------
    /**
     * While it is perfectly valid if the filter root is the start of or located below a mount, it's illegal if a given
     * mount would start somewhere in the subtree of the filter root distributing the principal based policies between
     * different mounts.
     */
    private void checkConflictingMount() {
        String filterRoot = filterProvider.getFilterRoot();
        for (Mount mount : mountInfoProvider.getNonDefaultMounts()) {
            if (mount.isUnder(filterRoot)) {
                throw new IllegalStateException("Mount found below filter root " + filterRoot);
            }
        }
    }

    private static boolean registerNodeTypes(@NotNull final Root root) {
        try {
            ReadOnlyNodeTypeManager ntMgr = new ReadOnlyNodeTypeManager() {
                @Override
                protected Tree getTypes() {
                    return root.getTree(NodeTypeConstants.NODE_TYPES_PATH);
                }
            };
            if (!ntMgr.hasNodeType(Constants.NT_REP_PRINCIPAL_POLICY)) {
                try (InputStream stream = PrincipalBasedAuthorizationConfiguration.class.getResourceAsStream("nodetypes.cnd")) {
                    NodeTypeRegistry.register(root, stream, "node types for principal based authorization");
                    return true;
                }
            }
        } catch (IOException | RepositoryException e) {
            throw new IllegalStateException("Unable to read node types for principal based authorization", e);
        }
        return false;
    }
}
