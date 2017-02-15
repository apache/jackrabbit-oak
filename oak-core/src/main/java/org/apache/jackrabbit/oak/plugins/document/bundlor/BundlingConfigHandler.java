/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.jackrabbit.oak.plugins.document.bundlor;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.Executor;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.collect.Iterables;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.spi.commit.BackgroundObserver;
import org.apache.jackrabbit.oak.spi.commit.BackgroundObserverMBean;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.DefaultEditor;
import org.apache.jackrabbit.oak.spi.commit.Editor;
import org.apache.jackrabbit.oak.spi.commit.EditorDiff;
import org.apache.jackrabbit.oak.spi.commit.Observable;
import org.apache.jackrabbit.oak.spi.commit.Observer;
import org.apache.jackrabbit.oak.spi.commit.SubtreeEditor;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.EMPTY_NODE;

public class BundlingConfigHandler implements Observer, Closeable {
    public static final String DOCUMENT_NODE_STORE = "rep:documentStore";
    public static final String BUNDLOR = "bundlor";

    public static final String CONFIG_PATH = "/jcr:system/rep:documentStore/bundlor";
    private final Logger log = LoggerFactory.getLogger(getClass());
    private NodeState root = EMPTY_NODE;
    private BackgroundObserver backgroundObserver;
    private Closeable observerRegistration;
    private boolean enabled;

    private volatile BundledTypesRegistry registry = BundledTypesRegistry.NOOP;

    private Editor changeDetector = new SubtreeEditor(new DefaultEditor() {
        @Override
        public void leave(NodeState before, NodeState after) throws CommitFailedException {
            recreateRegistry(after);
        }
    }, Iterables.toArray(PathUtils.elements(CONFIG_PATH), String.class));

    @Override
    public synchronized void contentChanged(@Nonnull NodeState root, @Nonnull CommitInfo info) {
        EditorDiff.process(changeDetector, this.root, root);
        this.root = root;
    }

    public BundlingHandler newBundlingHandler() {
        return new BundlingHandler(registry);
    }

    public void initialize(Observable nodeStore, Executor executor) {
        registerObserver(nodeStore, executor);
        //If bundling is disabled then initialize would not be invoked
        //NOOP registry would get used effectively disabling bundling for
        //new nodes
        enabled = true;
        log.info("Bundling of nodes enabled");
    }

    @Override
    public void close() throws IOException {
        unregisterObserver();
    }

    public BackgroundObserverMBean getMBean(){
        return checkNotNull(backgroundObserver).getMBean();
    }

    public boolean isEnabled() {
        return enabled;
    }

    BundledTypesRegistry getRegistry() {
        return registry;
    }

    private void recreateRegistry(NodeState nodeState) {
        //TODO Any sanity checks
        registry = BundledTypesRegistry.from(nodeState);
        log.info("Refreshing the BundledTypesRegistry");
    }

    private void registerObserver(Observable observable, Executor executor) {
        backgroundObserver = new BackgroundObserver(this, executor, 5);
        observerRegistration = observable.addObserver(backgroundObserver);
    }

    public void unregisterObserver() throws IOException {
        if (backgroundObserver != null) {
            observerRegistration.close();
            backgroundObserver.close();

            observerRegistration = null;
            backgroundObserver = null;
        }
    }

}


