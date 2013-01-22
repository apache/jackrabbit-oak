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

import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.api.security.user.UserManager;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.core.RootImpl;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.plugins.index.IndexConstants;
import org.apache.jackrabbit.oak.plugins.index.IndexUtils;
import org.apache.jackrabbit.oak.spi.lifecycle.RepositoryInitializer;
import org.apache.jackrabbit.oak.spi.security.SecurityProvider;
import org.apache.jackrabbit.oak.spi.security.user.UserConfiguration;
import org.apache.jackrabbit.oak.spi.security.user.UserConstants;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.apache.jackrabbit.oak.util.NodeUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Creates initial set of users to be present in the repository. This
 * implementation uses the {@code UserManager} such as defined by the
 * user configuration.
 *
 * Currently the following users are created:
 *
 * <ul>
 *     <li>An administrator user using {@link UserConstants#PARAM_ADMIN_ID}
 *     or {@link UserConstants#DEFAULT_ADMIN_ID} if the config option is missing.</li>
 *     <li>An administrator user using {@link UserConstants#PARAM_ANONYMOUS_ID}
 *     or {@link UserConstants#DEFAULT_ANONYMOUS_ID} if the config option is
 *     missing.</li>
 * </ul>
 *
 * In addition this initializer sets up index definitions for the following
 * user related properties:
 *
 * <ul>
 *     <li>{@link UserConstants#REP_AUTHORIZABLE_ID}</li>
 *     <li>{@link UserConstants#REP_PRINCIPAL_NAME}</li>
 *     <li>{@link UserConstants#REP_MEMBERS}</li>
 * </ul>
 */
public class UserInitializer implements RepositoryInitializer, UserConstants {

    /**
     * logger instance
     */
    private static final Logger log = LoggerFactory.getLogger(UserInitializer.class);

    private final SecurityProvider securityProvider;

    UserInitializer(SecurityProvider securityProvider) {
        this.securityProvider = securityProvider;
    }

    //----------------------------------------------< RepositoryInitializer >---
    @Override
    public void initialize(NodeStore store) {
        Root root = new RootImpl(store);

        UserConfiguration userConfiguration = securityProvider.getUserConfiguration();
        UserManager userManager = userConfiguration.getUserManager(root, NamePathMapper.DEFAULT);

        try {
            NodeUtil rootTree = new NodeUtil(root.getTree("/"));
            NodeUtil index = rootTree.getOrAddChild(IndexConstants.INDEX_DEFINITIONS_NAME, JcrConstants.NT_UNSTRUCTURED);
            IndexUtils.createIndexDefinition(index, "authorizableId", true, UserConstants.REP_AUTHORIZABLE_ID);
            // FIXME OAK-396: rep:principalName only needs to be unique if defined with user/group nodes -> add defining nt-info to uniqueness constraint otherwise ac-editing will fail.
            IndexUtils.createIndexDefinition(index, "principalName", true, UserConstants.REP_PRINCIPAL_NAME);
            IndexUtils.createIndexDefinition(index, "members", false, UserConstants.REP_MEMBERS);

            String adminId = userConfiguration.getConfigurationParameters().getConfigValue(PARAM_ADMIN_ID, DEFAULT_ADMIN_ID);
            if (userManager.getAuthorizable(adminId) == null) {
                // TODO: init admin with null password and force application to set it.
                userManager.createUser(adminId, adminId);
            }
            String anonymousId = userConfiguration.getConfigurationParameters().getConfigValue(PARAM_ANONYMOUS_ID, DEFAULT_ANONYMOUS_ID);
            if (userManager.getAuthorizable(anonymousId) == null) {
                userManager.createUser(anonymousId, null);
            }
            if (root.hasPendingChanges()) {
                root.commit();
            }
        } catch (RepositoryException e) {
            log.error("Failed to initialize user content ", e);
            throw new RuntimeException(e);
        } catch (CommitFailedException e) {
            log.error("Failed to initialize user content ", e);
            throw new RuntimeException(e);
        }
    }
}