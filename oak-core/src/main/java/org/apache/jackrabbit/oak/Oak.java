/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.oak;

import java.util.List;
import javax.annotation.Nonnull;

import com.google.common.collect.Lists;
import org.apache.jackrabbit.mk.api.MicroKernel;
import org.apache.jackrabbit.mk.core.MicroKernelImpl;
import org.apache.jackrabbit.oak.api.ContentRepository;
import org.apache.jackrabbit.oak.core.ContentRepositoryImpl;
import org.apache.jackrabbit.oak.kernel.KernelNodeStore;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeStore;
import org.apache.jackrabbit.oak.spi.commit.CommitHook;
import org.apache.jackrabbit.oak.spi.commit.CompositeHook;
import org.apache.jackrabbit.oak.spi.commit.CompositeValidatorProvider;
import org.apache.jackrabbit.oak.spi.commit.ValidatingHook;
import org.apache.jackrabbit.oak.spi.commit.Validator;
import org.apache.jackrabbit.oak.spi.commit.ValidatorProvider;
import org.apache.jackrabbit.oak.spi.query.CompositeQueryIndexProvider;
import org.apache.jackrabbit.oak.spi.query.QueryIndexProvider;
import org.apache.jackrabbit.oak.spi.security.SecurityProvider;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStore;

/**
 * Builder class for constructing {@link ContentRepository} instances with
 * a set of specified plugin components. This class acts as a public facade
 * that hides the internal implementation classes and the details of how
 * they get instantiated and wired together.
 *
 * @since Oak 0.6
 */
public class Oak {

    private final MicroKernel kernel;

    private final List<QueryIndexProvider> queryIndexProviders = Lists.newArrayList();

    private final List<CommitHook> commitHooks = Lists.newArrayList();

    private final List<ValidatorProvider> validatorProviders = Lists.newArrayList();

    private SecurityProvider securityProvider;

    public Oak(MicroKernel kernel) {
        this.kernel = kernel;
    }

    public Oak() {
        this(new MicroKernelImpl());
    }

    /**
     * Associates the given query index provider with the repository to
     * be created.
     *
     * @param provider query index provider
     * @return this builder
     */
    @Nonnull
    public Oak with(@Nonnull QueryIndexProvider provider) {
        queryIndexProviders.add(provider);
        return this;
    }

    /**
     * Associates the given commit hook with the repository to be created.
     *
     * @param hook commit hook
     * @return this builder
     */
    @Nonnull
    public Oak with(@Nonnull CommitHook hook) {
        withValidatorHook();
        commitHooks.add(hook);
        return this;
    }

    /**
     * Turns all currently tracked validators to a validating commit hook
     * and associates that hook with the repository to be created. This way
     * a sequence of {@code with()} calls that alternates between validators
     * and other commit hooks will have all the validators in the correct
     * order while still being able to leverage the performance gains of
     * multiple validators iterating over the changes simultaneously.
     */
    private void withValidatorHook() {
        if (!validatorProviders.isEmpty()) {
            commitHooks.add(new ValidatingHook(
                    CompositeValidatorProvider.compose(validatorProviders)));
            //validatorProviders.clear(); FIXME
        }
    }

    /**
     * Associates the given validator provider with the repository to
     * be created.
     *
     * @param provider validator provider
     * @return this builder
     */
    @Nonnull
    public Oak with(@Nonnull ValidatorProvider provider) {
        validatorProviders.add(provider);
        return this;
    }

    /**
     * Associates the given validator with the repository to be created.
     *
     * @param validator validator
     * @return this builder
     */
    @Nonnull
    public Oak with(@Nonnull final Validator validator) {
        return with(new ValidatorProvider() {
            @Override @Nonnull
            public Validator getRootValidator(
                    NodeState before, NodeState after) {
                return validator;
            }
        });
    }

    @Nonnull
    public Oak with(@Nonnull SecurityProvider securityProvider) {
        this.securityProvider = securityProvider;

        validatorProviders.addAll(securityProvider.getAccessControlProvider().getValidatorProviders());
        validatorProviders.addAll(securityProvider.getUserConfiguration().getValidatorProviders());
        return this;
    }

    public ContentRepository createContentRepository() {
        return new ContentRepositoryImpl(
                kernel,
                CompositeQueryIndexProvider.compose(queryIndexProviders),
                createCommitHook(),
                securityProvider);
    }

    private CommitHook createCommitHook() {
        withValidatorHook();
        return CompositeHook.compose(commitHooks);
    }

    /**
     * Creates a {@link NodeStore} based on the previously set micro kernel,
     * hooks and validators.
     * <p/>
     * This method will return an in memory node store without hooks nor
     * validators if no {@link MicroKernel} was set.
     *
     * @return a {@link NodeStore}.
     */
    public NodeStore createNodeStore() {
        if (kernel != null) {
            KernelNodeStore nodeStore = new KernelNodeStore(kernel);
            nodeStore.setHook(createCommitHook());
            return nodeStore;
        } else {
            return new MemoryNodeStore();
        }
    }
}