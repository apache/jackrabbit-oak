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
package org.apache.jackrabbit.oak.spi.security;

import java.security.Principal;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import javax.annotation.Nonnull;

import org.osgi.annotation.versioning.ProviderType;
import org.apache.jackrabbit.oak.spi.commit.CommitHook;
import org.apache.jackrabbit.oak.spi.commit.MoveTracker;
import org.apache.jackrabbit.oak.spi.commit.ThreeWayConflictHandler;
import org.apache.jackrabbit.oak.spi.commit.ValidatorProvider;
import org.apache.jackrabbit.oak.spi.lifecycle.RepositoryInitializer;
import org.apache.jackrabbit.oak.spi.lifecycle.WorkspaceInitializer;
import org.apache.jackrabbit.oak.spi.xml.ProtectedItemImporter;

/**
 * Base interface for all security related configurations.
 */
@ProviderType
public interface SecurityConfiguration {

    /**
     * Returns the name of this security configuration.
     *
     * @return The name of this configuration.
     */
    @Nonnull
    String getName();

    /**
     * Returns the configuration parameters associated with this security
     * configuration instance. If no parameters are present
     * {@link ConfigurationParameters#EMPTY} should be returned.
     *
     * @return The configuration parameters.
     */
    @Nonnull
    ConfigurationParameters getParameters();

    /**
     * Returns a workspace initializer for this security configuration. If this
     * configuration doesn't require any specific workspace initialization
     * {@link WorkspaceInitializer#DEFAULT} should be returned.
     *
     * @return An instance of {@code WorkspaceInitializer}.
     */
    @Nonnull
    WorkspaceInitializer getWorkspaceInitializer();

    /**
     * Returns a repository initializer for this security configuration. If this
     * configuration doesn't require any specific repository initialization
     * {@link RepositoryInitializer#DEFAULT} should be returned.
     *
     * @return An instance of {@code RepositoryInitializer}.
     */
    @Nonnull
    RepositoryInitializer getRepositoryInitializer();

    /**
     * Returns the list of commit hooks that need to be executed for the
     * specified workspace name.
     *
     * @param workspaceName The name of the workspace.
     * @return A list of commit hooks.
     */
    @Nonnull
    List<? extends CommitHook> getCommitHooks(@Nonnull String workspaceName);

    /**
     * Returns the list of validators that need to be executed for the specified
     * workspace name.
     *
     * @param workspaceName The name of the workspace.
     * @param principals The set of principals associated with the subject
     * that is committing modifications.
     * @param moveTracker The move tracker associated with the commit.
     * @return A list of validators.
     */
    @Nonnull
    List<? extends ValidatorProvider> getValidators(@Nonnull String workspaceName,
                                                    @Nonnull Set<Principal> principals,
                                                    @Nonnull MoveTracker moveTracker);

    /**
     * Returns the list of conflict handlers available for this security configuration.
     *
     * @return A list of {@link org.apache.jackrabbit.oak.spi.commit.ThreeWayConflictHandler}.
     */
    @Nonnull
    List<ThreeWayConflictHandler> getConflictHandlers();

    /**
     * @return The list of protected item importers defined by this configuration.
     */
    @Nonnull
    List<ProtectedItemImporter> getProtectedItemImporters();

    /**
     * @return The context defined by this configuration.
     */
    @Nonnull
    Context getContext();

    /**
     * Default implementation that provides empty initializers, validators,
     * commit hooks and parameters.
     */
    class Default implements SecurityConfiguration {

        @Nonnull
        @Override
        public String getName() {
            return "org.apache.jackrabbit.oak";
        }

        @Nonnull
        @Override
        public ConfigurationParameters getParameters() {
            return ConfigurationParameters.EMPTY;
        }

        @Nonnull
        @Override
        public WorkspaceInitializer getWorkspaceInitializer() {
            return WorkspaceInitializer.DEFAULT;
        }

        @Nonnull
        @Override
        public RepositoryInitializer getRepositoryInitializer() {
            return RepositoryInitializer.DEFAULT;
        }

        @Nonnull
        @Override
        public List<? extends CommitHook> getCommitHooks(@Nonnull String workspaceName) {
            return Collections.emptyList();
        }

        @Nonnull
        @Override
        public List<? extends ValidatorProvider> getValidators(
                @Nonnull String workspaceName, @Nonnull Set<Principal> principals, @Nonnull MoveTracker moveTracker) {
            return Collections.emptyList();
        }

        @Nonnull
        @Override
        public List<ThreeWayConflictHandler> getConflictHandlers() {
            return Collections.emptyList();
        }

        @Nonnull
        @Override
        public List<ProtectedItemImporter> getProtectedItemImporters() {
            return Collections.emptyList();
        }

        @Nonnull
        @Override
        public Context getContext() {
            return Context.DEFAULT;
        }
    }
}
