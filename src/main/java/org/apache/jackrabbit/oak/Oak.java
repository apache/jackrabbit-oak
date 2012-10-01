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

import org.apache.jackrabbit.mk.api.MicroKernel;
import org.apache.jackrabbit.mk.core.MicroKernelImpl;
import org.apache.jackrabbit.oak.api.ContentRepository;
import org.apache.jackrabbit.oak.core.ContentRepositoryImpl;
import org.apache.jackrabbit.oak.spi.commit.CommitHook;
import org.apache.jackrabbit.oak.spi.commit.CompositeHook;
import org.apache.jackrabbit.oak.spi.commit.CompositeValidatorProvider;
import org.apache.jackrabbit.oak.spi.commit.ValidatingHook;
import org.apache.jackrabbit.oak.spi.commit.Validator;
import org.apache.jackrabbit.oak.spi.commit.ValidatorProvider;
import org.apache.jackrabbit.oak.spi.query.CompositeQueryIndexProvider;
import org.apache.jackrabbit.oak.spi.query.QueryIndexProvider;
import org.apache.jackrabbit.oak.spi.state.NodeState;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

public class Oak {

    private final MicroKernel kernel;

    private final List<QueryIndexProvider> providers = Lists.newArrayList();

    private final List<CommitHook> hooks = Lists.newArrayList();

    private final List<ValidatorProvider> validators = Lists.newArrayList();

    public Oak(MicroKernel kernel) {
        this.kernel = kernel;
    }

    public Oak() {
        this(new MicroKernelImpl());
    }

    public Oak with(QueryIndexProvider provider) {
        providers.add(provider);
        return this;
    }

    public Oak with(CommitHook hook) {
        hooks.add(hook);
        return this;
    }

    public Oak with(ValidatorProvider provider) {
        validators.add(provider);
        return this;
    }

    public Oak with(@Nonnull final Validator validator) {
        return with(new ValidatorProvider() {
            @Override @Nonnull
            public Validator getRootValidator(
                    NodeState before, NodeState after) {
                return validator;
            }
        });
    }

    public ContentRepository createContentRepository() {
        CommitHook hook;
        if (!validators.isEmpty()) {
            hook = CompositeHook.compose(ImmutableList.<CommitHook>builder()
                    .addAll(hooks)
                    .add(new ValidatingHook(
                            CompositeValidatorProvider.compose(validators)))
                    .build());
        } else {
            hook = CompositeHook.compose(hooks);
        }
        return new ContentRepositoryImpl(
                kernel,
                CompositeQueryIndexProvider.compose(providers),
                hook);
    }


}
