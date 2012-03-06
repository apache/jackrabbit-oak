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
package org.apache.jackrabbit.mk.model;

import org.apache.jackrabbit.mk.store.RevisionProvider;
import org.apache.jackrabbit.mk.store.NotFoundException;

/**
 *
 */
public final class NodeProxy {

    private final String id;
    private final Node node;

    public NodeProxy(String id) {
        this.id = id;
        this.node = null;
    }

    public NodeProxy(Node node) {
        this.node = node;
        this.id = null;
    }

    public String getId() {
        return id;
    }

    public Node getNode() {
        return node;
    }

    public Node getTarget(RevisionProvider provider) throws NotFoundException, Exception {
        if (node != null) {
            return node;
        } else {
            return provider.getNode(id);
        }
    }
}
