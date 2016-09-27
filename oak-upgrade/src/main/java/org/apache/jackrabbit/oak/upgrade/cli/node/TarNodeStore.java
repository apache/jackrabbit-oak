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
package org.apache.jackrabbit.oak.upgrade.cli.node;

import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.apache.jackrabbit.oak.spi.state.ProxyNodeStore;

public class TarNodeStore extends ProxyNodeStore {

    private final NodeStore ns;

    private final SuperRootProvider superRootProvider;

    public TarNodeStore(NodeStore ns, SuperRootProvider superRootProvider) {
        this.ns = ns;
        this.superRootProvider = superRootProvider;
    }

    public void setSuperRoot(NodeBuilder builder) {
        superRootProvider.setSuperRoot(builder);
    }

    public NodeState getSuperRoot() {
        return superRootProvider.getSuperRoot();
    }

    @Override
    protected NodeStore getNodeStore() {
        return ns;
    }

    interface SuperRootProvider {

        void setSuperRoot(NodeBuilder builder);

        NodeState getSuperRoot();

    }
}
