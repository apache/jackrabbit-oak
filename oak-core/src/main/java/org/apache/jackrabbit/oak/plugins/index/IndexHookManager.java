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
package org.apache.jackrabbit.oak.plugins.index;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.spi.commit.CommitHook;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;

/**
 * Keeps existing IndexHooks updated.
 * 
 * <p>
 * The existing index list is obtained via the IndexHookProvider.
 * </p>
 * 
 * @see IndexHook
 * @see IndexHookProvider
 * 
 */
public class IndexHookManager implements CommitHook {

    private final IndexHookProvider provider;

    public static final IndexHookManager of(IndexHookProvider provider) {
        return new IndexHookManager(provider);
    }

    protected IndexHookManager(IndexHookProvider provider) {
        this.provider = provider;
    }

    @Override
    public NodeState processCommit(NodeState before, NodeState after)
            throws CommitFailedException {
        NodeBuilder builder = after.builder();

        // <type, <path, indexhook>>
        Map<String, Map<String, List<IndexHook>>> updates = new HashMap<String, Map<String, List<IndexHook>>>();
        after.compareAgainstBaseState(before, new IndexHookManagerDiff(
                provider, builder, updates));
        apply(updates);
        return builder.getNodeState();
    }

    private void apply(Map<String, Map<String, List<IndexHook>>> updates)
            throws CommitFailedException {
        try {
            for (String type : updates.keySet()) {
                for (List<IndexHook> hooks : updates.get(type).values()) {
                    for (IndexHook hook : hooks) {
                        hook.apply();
                    }
                }
            }
        } finally {
            for (String type : updates.keySet()) {
                for (List<IndexHook> hooks : updates.get(type).values()) {
                    for (IndexHook hook : hooks) {
                        try {
                            hook.close();
                        } catch (IOException e) {
                            e.printStackTrace();
                            throw new CommitFailedException(
                                    "Failed to close the index hook", e);
                        }
                    }
                }
            }
        }
    }
}
