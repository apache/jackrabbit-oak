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
package org.apache.jackrabbit.oak.jcr;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import javax.annotation.Nonnull;
import javax.jcr.Repository;

import org.apache.jackrabbit.mk.api.MicroKernel;
import org.apache.jackrabbit.oak.Oak;
import org.apache.jackrabbit.oak.plugins.commit.AnnotatingConflictHandler;
import org.apache.jackrabbit.oak.plugins.commit.ConflictValidatorProvider;
import org.apache.jackrabbit.oak.plugins.index.CompositeIndexHookProvider;
import org.apache.jackrabbit.oak.plugins.index.IndexHookManager;
import org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexHookProvider;
import org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexProvider;
import org.apache.jackrabbit.oak.plugins.index.property.PropertyIndexHookProvider;
import org.apache.jackrabbit.oak.plugins.index.property.PropertyIndexProvider;
import org.apache.jackrabbit.oak.plugins.name.NameValidatorProvider;
import org.apache.jackrabbit.oak.plugins.name.NamespaceValidatorProvider;
import org.apache.jackrabbit.oak.plugins.nodetype.DefaultTypeEditor;
import org.apache.jackrabbit.oak.plugins.nodetype.InitialContent;
import org.apache.jackrabbit.oak.plugins.nodetype.RegistrationValidatorProvider;
import org.apache.jackrabbit.oak.plugins.nodetype.TypeValidatorProvider;
import org.apache.jackrabbit.oak.security.SecurityProviderImpl;
import org.apache.jackrabbit.oak.spi.commit.CommitHook;
import org.apache.jackrabbit.oak.spi.commit.ConflictHandler;
import org.apache.jackrabbit.oak.spi.commit.Validator;
import org.apache.jackrabbit.oak.spi.commit.ValidatorProvider;
import org.apache.jackrabbit.oak.spi.lifecycle.RepositoryInitializer;
import org.apache.jackrabbit.oak.spi.query.QueryIndexProvider;
import org.apache.jackrabbit.oak.spi.security.SecurityProvider;

import static com.google.common.base.Preconditions.checkNotNull;

public class Jcr {

    private final Oak oak;

    private ScheduledExecutorService executor =
            Executors.newScheduledThreadPool(0);

    private SecurityProvider securityProvider;

    private Jcr(Oak oak) {
        this.oak = oak;

        with(new InitialContent());

        with(new DefaultTypeEditor());

        with(new SecurityProviderImpl());

        with(new NameValidatorProvider());
        with(new NamespaceValidatorProvider());
        with(new TypeValidatorProvider());
        with(new RegistrationValidatorProvider());
        with(new ConflictValidatorProvider());

        with(new IndexHookManager(
                new CompositeIndexHookProvider(
                new PropertyIndexHookProvider(), 
                new LuceneIndexHookProvider())));
        with(new AnnotatingConflictHandler());

        with(new PropertyIndexProvider());
        with(new LuceneIndexProvider());
    }

    public Jcr() {
        this(new Oak());
    }

    public Jcr(MicroKernel kernel) {
        this(new Oak(kernel));
    }

    @Nonnull
    public Jcr with(@Nonnull RepositoryInitializer initializer) {
       oak.with(checkNotNull(initializer));
       return this;
    }

    @Nonnull
    public Jcr with(@Nonnull QueryIndexProvider provider) {
        oak.with(checkNotNull(provider));
        return this;
    }

    @Nonnull
    public Jcr with(@Nonnull CommitHook hook) {
        oak.with(checkNotNull(hook));
        return this;
    }

    @Nonnull
    public Jcr with(@Nonnull ValidatorProvider provider) {
        oak.with(checkNotNull(provider));
        return this;
    }

    @Nonnull
    public Jcr with(@Nonnull Validator validator) {
        oak.with(checkNotNull(validator));
        return this;
    }

    @Nonnull
    public Jcr with(@Nonnull SecurityProvider securityProvider) {
        oak.with(checkNotNull(securityProvider));
        this.securityProvider = securityProvider;
        return this;
    }

    @Nonnull
    public Jcr with(@Nonnull ConflictHandler conflictHandler) {
        oak.with(checkNotNull(conflictHandler));
        return this;
    }

    @Nonnull
    public Jcr with(@Nonnull ScheduledExecutorService executor) {
        this.executor = checkNotNull(executor);
        return this;
    }

    public Repository createRepository() {
        return new RepositoryImpl(
                oak.createContentRepository(), executor, securityProvider);
    }

}
