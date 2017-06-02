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
package org.apache.jackrabbit.oak.console;

import java.util.Map;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nonnull;

import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStore;

import com.google.common.collect.Maps;
import org.apache.jackrabbit.oak.spi.whiteboard.Whiteboard;

/**
 * Light weight session to a NodeStore, holding context information.
 */
public class ConsoleSession {

    private final Map<String, Object> context = Maps.newHashMap();

    private final NodeStore store;
    private final Whiteboard whiteboard;

    private ConsoleSession(NodeStore store, Whiteboard whiteboard) {
        this.store = store;
        this.whiteboard = whiteboard;
    }

    public static ConsoleSession create(NodeStore store, Whiteboard whiteboard) {
        return new ConsoleSession(store, whiteboard);
    }

    /**
     * Returns the current working path. This method will return the path
     * of the root node if none is set explicity.
     *
     * @return the current working path.
     */
    public String getWorkingPath() {
        String path = (String) context.get("workingPath");
        if (path == null) {
            path = "/";
            context.put("workingPath", path);
        }
        return path;
    }

    /**
     * Sets a new working path and returns the previously set.
     *
     * @param path the new working path.
     * @return the previously set working path.
     */
    public String setWorkingPath(String path) {
        String old = getWorkingPath();
        context.put("workingPath", path);
        return old;
    }

    /**
     * Creates and returns a checkpoint with the given lifetime in seconds.
     *
     * @param lifetimeSeconds the lifetime of the checkpoint in seconds.
     * @return the checkpoint reference.
     */
    public String checkpoint(long lifetimeSeconds) {
        return store.checkpoint(TimeUnit.SECONDS.toMillis(lifetimeSeconds));
    }

    /**
     * Retrieves the root node from a previously created checkpoint. The root
     * node is available with {@link #getRoot()}.
     *
     * @param checkpoint the checkpoint reference.
     */
    public void retrieve(String checkpoint) {
        context.put("root", store.retrieve(checkpoint));
    }

    /**
     * Returns the currently set root node. If {@link #isAutoRefresh()} is set,
     * a fresh root node is retrieved from the store.
     *
     * @return the current root node.
     */
    public NodeState getRoot() {
        NodeState root = (NodeState) context.get("root");
        if (root == null || isAutoRefresh()) {
            root = store.getRoot();
            context.put("root", root);
        }
        return root;
    }

    /**
     * @return the underlying node store.
     */
    public NodeStore getStore() {
        return store;
    }

    public Whiteboard getWhiteboard() {
        return whiteboard;
    }

    /**
     * The node state for the current working path. Possibly non-existent.
     *
     * @return the working node state.
     */
    @Nonnull
    public NodeState getWorkingNode() {
        NodeState current = getRoot();
        for (String element : PathUtils.elements(getWorkingPath())) {
            current = current.getChildNode(element);
        }
        return current;
    }

    /**
     * Enables or disables auto-refresh of the root node state on
     * {@link #getRoot()}.
     *
     * @param enable enables or disables auto-refresh.
     */
    public void setAutoRefresh(boolean enable) {
        if (enable) {
            context.put("auto-refresh", "true");
        } else {
            context.remove("auto-refresh");
        }
    }

    /**
     * @return <code>true</code> if auto-refresh is enabled; <code>false</code>
     *          otherwise.
     */
    public boolean isAutoRefresh() {
        return context.containsKey("auto-refresh");
    }

    /**
     * Performs a manual refresh of the root node state.
     */
    public void refresh() {
        context.put("root", store.getRoot());
    }
}
