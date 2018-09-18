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
package org.apache.jackrabbit.oak.plugins.tree.impl;

import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.plugins.tree.RootProvider;
import org.apache.jackrabbit.oak.plugins.tree.factories.RootFactory;
import org.apache.jackrabbit.oak.spi.commit.CommitHook;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.osgi.service.component.annotations.Component;

@Component(service = {RootProvider.class})
public class RootProviderService implements RootProvider {

    @NotNull
    @Override
    public Root createReadOnlyRoot(@NotNull NodeState rootState) {
        return RootFactory.createReadOnlyRoot(rootState);
    }

    @NotNull
    @Override
    public Root createReadOnlyRoot(@NotNull Root root) {
        return RootFactory.createReadOnlyRoot(root);
    }

    @NotNull
    @Override
    public Root createSystemRoot(@NotNull NodeStore store, @Nullable CommitHook hook) {
        return RootFactory.createSystemRoot(store, hook, null, null, null);
    }
}
