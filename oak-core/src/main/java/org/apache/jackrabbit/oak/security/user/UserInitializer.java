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
package org.apache.jackrabbit.oak.security.user;

import javax.jcr.RepositoryException;

import com.google.common.base.Strings;
import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.api.security.user.UserManager;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.plugins.index.IndexConstants;
import org.apache.jackrabbit.oak.plugins.index.IndexUtils;
import org.apache.jackrabbit.oak.plugins.index.nodetype.NodeTypeIndexProvider;
import org.apache.jackrabbit.oak.plugins.index.property.PropertyIndexProvider;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeStore;
import org.apache.jackrabbit.oak.plugins.tree.factories.RootFactory;
import org.apache.jackrabbit.oak.plugins.tree.TreeUtil;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.lifecycle.WorkspaceInitializer;
import org.apache.jackrabbit.oak.spi.query.CompositeQueryIndexProvider;
import org.apache.jackrabbit.oak.spi.query.QueryIndexProvider;
import org.apache.jackrabbit.oak.spi.query.QueryIndexProviderAware;
import org.apache.jackrabbit.oak.spi.security.ConfigurationParameters;
import org.apache.jackrabbit.oak.spi.security.SecurityProvider;
import org.apache.jackrabbit.oak.spi.security.user.UserConfiguration;
import org.apache.jackrabbit.oak.spi.security.user.UserConstants;
import org.apache.jackrabbit.oak.spi.state.ApplyDiff;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static org.apache.jackrabbit.oak.plugins.memory.ModifiedNodeState.squeeze;

/**
 * Creates initial set of users to be present in a given workspace. This
 * implementation uses the {@code UserManager} such as defined by the
 * user configuration.
 * <p>
 * Currently the following users are created:
 * <p>
 * <ul>
 * <li>An administrator user using {@link UserConstants#PARAM_ADMIN_ID}
 * or {@link UserConstants#DEFAULT_ADMIN_ID} if the config option is missing.</li>
 * <li>An administrator user using {@link UserConstants#PARAM_ANONYMOUS_ID}
 * or {@link UserConstants#DEFAULT_ANONYMOUS_ID} if the config option is
 * missing.</li>
 * </ul>
 * <p>
 * In addition this initializer sets up index definitions for the following
 * user related properties:
 * <p>
 * <ul>
 * <li>{@link UserConstants#REP_AUTHORIZABLE_ID}</li>
 * <li>{@link UserConstants#REP_PRINCIPAL_NAME}</li>
 * <li>{@link UserConstants#REP_MEMBERS}</li>
 * </ul>
 */
class UserInitializer implements WorkspaceInitializer, UserConstants, QueryIndexProviderAware {

    /**
     * logger instance
     */
    private static final Logger log = LoggerFactory.getLogger(UserInitializer.class);

    private final SecurityProvider securityProvider;

    private QueryIndexProvider queryIndexProvider = new CompositeQueryIndexProvider(new PropertyIndexProvider(),
            new NodeTypeIndexProvider());

    UserInitializer(SecurityProvider securityProvider) {
        this.securityProvider = securityProvider;
    }

    //-----------------------------------------------< WorkspaceInitializer >---

    @Override
    public void initialize(NodeBuilder builder, String workspaceName) {
        // squeeze node state before it is passed to store (OAK-2411)
        NodeState base = squeeze(builder.getNodeState());
        MemoryNodeStore store = new MemoryNodeStore(base);

        Root root = RootFactory.createSystemRoot(store, EmptyHook.INSTANCE, workspaceName,
                securityProvider,  queryIndexProvider);

        UserConfiguration userConfiguration = securityProvider.getConfiguration(UserConfiguration.class);
        UserManager userManager = userConfiguration.getUserManager(root, NamePathMapper.DEFAULT);

        String errorMsg = "Failed to initialize user content.";
        try {
            Tree rootTree = root.getTree(PathUtils.ROOT_PATH);
            checkState(rootTree.exists());
            Tree index = TreeUtil.getOrAddChild(rootTree, IndexConstants.INDEX_DEFINITIONS_NAME, JcrConstants.NT_UNSTRUCTURED);

            if (!index.hasChild("authorizableId")) {
                Tree authorizableId = IndexUtils.createIndexDefinition(index, "authorizableId", true,
                        new String[]{REP_AUTHORIZABLE_ID},
                        new String[]{NT_REP_AUTHORIZABLE});
                authorizableId.setProperty("info",
                        "Oak index used by the user management " + 
                        "to enforce uniqueness of rep:authorizableId property values.");
            }
            if (!index.hasChild("principalName")) {
                Tree principalName = IndexUtils.createIndexDefinition(index, "principalName", true,
                        new String[]{REP_PRINCIPAL_NAME},
                        new String[]{NT_REP_AUTHORIZABLE});
                principalName.setProperty("info",
                        "Oak index used by the user management " +
                        "to enforce uniqueness of rep:principalName property values, " +
                        "and to quickly search a principal by name if it was constructed manually.");
            }
            if (!index.hasChild("repMembers")) {
                Tree members = IndexUtils.createIndexDefinition(index, "repMembers", false,
                        new String[]{REP_MEMBERS},
                        new String[]{NT_REP_MEMBER_REFERENCES});
                members.setProperty("info",
                        "Oak index used by the user management to lookup group membership.");
            }

            ConfigurationParameters params = userConfiguration.getParameters();
            String adminId = params.getConfigValue(PARAM_ADMIN_ID, DEFAULT_ADMIN_ID);
            if (userManager.getAuthorizable(adminId) == null) {
                boolean omitPw = params.getConfigValue(PARAM_OMIT_ADMIN_PW, false);
                userManager.createUser(adminId, (omitPw) ? null : adminId);
            }
            String anonymousId = Strings.emptyToNull(params.getConfigValue(PARAM_ANONYMOUS_ID, DEFAULT_ANONYMOUS_ID, String.class));
            if (anonymousId != null && userManager.getAuthorizable(anonymousId) == null) {
                userManager.createUser(anonymousId, null);
            }
            if (root.hasPendingChanges()) {
                root.commit();
            }
        } catch (RepositoryException | CommitFailedException e) {
            log.error(errorMsg, e);
            throw new RuntimeException(e);
        }

        NodeState target = store.getRoot();
        target.compareAgainstBaseState(base, new ApplyDiff(builder));
    }

    @Override
    public void setQueryIndexProvider(QueryIndexProvider provider) {
        this.queryIndexProvider = provider;
    }
}
