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

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;

import javax.annotation.Nonnull;
import javax.jcr.Repository;

import org.apache.jackrabbit.oak.Oak;
import org.apache.jackrabbit.oak.jcr.repository.RepositoryImpl;
import org.apache.jackrabbit.oak.plugins.commit.ConflictValidatorProvider;
import org.apache.jackrabbit.oak.plugins.commit.JcrConflictHandler;
import org.apache.jackrabbit.oak.plugins.index.IndexEditorProvider;
import org.apache.jackrabbit.oak.plugins.index.nodetype.NodeTypeIndexProvider;
import org.apache.jackrabbit.oak.plugins.index.property.OrderedPropertyIndexEditorProvider;
import org.apache.jackrabbit.oak.plugins.index.property.OrderedPropertyIndexProvider;
import org.apache.jackrabbit.oak.plugins.index.property.PropertyIndexEditorProvider;
import org.apache.jackrabbit.oak.plugins.index.property.PropertyIndexProvider;
import org.apache.jackrabbit.oak.plugins.index.reference.ReferenceEditorProvider;
import org.apache.jackrabbit.oak.plugins.index.reference.ReferenceIndexProvider;
import org.apache.jackrabbit.oak.plugins.itemsave.ItemSaveValidatorProvider;
import org.apache.jackrabbit.oak.plugins.name.NameValidatorProvider;
import org.apache.jackrabbit.oak.plugins.name.NamespaceEditorProvider;
import org.apache.jackrabbit.oak.plugins.nodetype.TypeEditorProvider;
import org.apache.jackrabbit.oak.plugins.nodetype.write.InitialContent;
import org.apache.jackrabbit.oak.plugins.observation.CommitRateLimiter;
import org.apache.jackrabbit.oak.plugins.version.VersionEditorProvider;
import org.apache.jackrabbit.oak.query.QueryEngineSettings;
import org.apache.jackrabbit.oak.security.SecurityProviderImpl;
import org.apache.jackrabbit.oak.spi.commit.CommitHook;
import org.apache.jackrabbit.oak.spi.commit.ConflictHandler;
import org.apache.jackrabbit.oak.spi.commit.Editor;
import org.apache.jackrabbit.oak.spi.commit.EditorHook;
import org.apache.jackrabbit.oak.spi.commit.EditorProvider;
import org.apache.jackrabbit.oak.spi.commit.Observer;
import org.apache.jackrabbit.oak.spi.lifecycle.RepositoryInitializer;
import org.apache.jackrabbit.oak.spi.query.QueryIndexProvider;
import org.apache.jackrabbit.oak.spi.security.SecurityProvider;
import org.apache.jackrabbit.oak.spi.state.NodeStore;

public class Jcr {
    public static final int DEFAULT_OBSERVATION_QUEUE_LENGTH = 1000;

    private final Oak oak;

    private SecurityProvider securityProvider;
    private int observationQueueLength = DEFAULT_OBSERVATION_QUEUE_LENGTH;
    private CommitRateLimiter commitRateLimiter = null;

    public Jcr(Oak oak) {
        this.oak = oak;

        with(new InitialContent());

        with(JcrConflictHandler.JCR_CONFLICT_HANDLER);
        with(new EditorHook(new VersionEditorProvider()));

        with(new SecurityProviderImpl());

        with(new ItemSaveValidatorProvider());
        with(new NameValidatorProvider());
        with(new NamespaceEditorProvider());
        with(new TypeEditorProvider());
        with(new ConflictValidatorProvider());
        with(new ReferenceEditorProvider());
        with(new ReferenceIndexProvider());

        with(new PropertyIndexEditorProvider());

        with(new PropertyIndexProvider());
        with(new OrderedPropertyIndexProvider());
        with(new NodeTypeIndexProvider());
        
        with(new OrderedPropertyIndexEditorProvider());
    }

    public Jcr() {
        this(new Oak());
    }

    public Jcr(NodeStore store) {
        this(new Oak(store));
    }

    @Nonnull
    public final Jcr with(@Nonnull RepositoryInitializer initializer) {
       oak.with(checkNotNull(initializer));
       return this;
    }

    @Nonnull
    public final Jcr with(@Nonnull QueryIndexProvider provider) {
        oak.with(checkNotNull(provider));
        return this;
    }

    @Nonnull
    public final Jcr with(@Nonnull IndexEditorProvider indexEditorProvider) {
        oak.with(checkNotNull(indexEditorProvider));
        return this;
    }

    @Nonnull
    public final Jcr with(@Nonnull CommitHook hook) {
        oak.with(checkNotNull(hook));
        return this;
    }

    @Nonnull
    public final Jcr with(@Nonnull EditorProvider provider) {
        oak.with(checkNotNull(provider));
        return this;
    }

    @Nonnull
    public final Jcr with(@Nonnull Editor editor) {
        oak.with(checkNotNull(editor));
        return this;
    }

    @Nonnull
    public final Jcr with(@Nonnull SecurityProvider securityProvider) {
        oak.with(checkNotNull(securityProvider));
        this.securityProvider = securityProvider;
        return this;
    }

    @Nonnull
    public final Jcr with(@Nonnull ConflictHandler conflictHandler) {
        oak.with(checkNotNull(conflictHandler));
        return this;
    }

    @Nonnull
    public final Jcr with(@Nonnull ScheduledExecutorService executor) {
        oak.with(checkNotNull(executor));
        return this;
    }

    @Nonnull
    public final Jcr with(@Nonnull Executor executor) {
        oak.with(checkNotNull(executor));
        return this;
    }

    @Nonnull
    public final Jcr with(@Nonnull Observer observer) {
        oak.with(checkNotNull(observer));
        return this;
    }

    @Nonnull
    public Jcr withAsyncIndexing() {
        oak.withAsyncIndexing();
        return this;
    }

    @Nonnull
    public Jcr withObservationQueueLength(int observationQueueLength) {
        this.observationQueueLength = observationQueueLength;
        return this;
    }

    @Nonnull
    public Jcr with(CommitRateLimiter commitRateLimiter) {
        oak.with(commitRateLimiter);
        this.commitRateLimiter = commitRateLimiter;
        return this;
    }
    
    @Nonnull
    public Jcr with(QueryEngineSettings qs) {
        oak.with(qs);
        return this;
    }

    public Repository createRepository() {
        return new RepositoryImpl(
                oak.createContentRepository(), 
                oak.getWhiteboard(),
                securityProvider,
                observationQueueLength,
                commitRateLimiter);
    }

}
