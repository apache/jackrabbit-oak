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
package org.apache.jackrabbit.oak.plugins.identifier;

import static org.apache.jackrabbit.JcrConstants.JCR_UUID;
import static org.apache.jackrabbit.JcrConstants.MIX_REFERENCEABLE;

import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.plugins.nodetype.TypePredicate;
import org.apache.jackrabbit.oak.spi.state.NodeState;

/**
 * Simple utility class for lazily tracking the current identifier during
 * a tree traversal that recurses down a subtree.
 */
public class IdentifierTracker {

    private final TypePredicate referenceable;

    private final IdentifierTracker parent;

    private final String name;

    private String identifier;

    public IdentifierTracker(NodeState root) {
        this.referenceable = new TypePredicate(root, MIX_REFERENCEABLE);
        this.parent = null;
        this.name = null;

        String uuid = root.getString(JCR_UUID);
        if (uuid != null && referenceable.apply(root)) {
            this.identifier = uuid;
        } else {
            this.identifier = "/";
        }
    }

    private IdentifierTracker(
            IdentifierTracker parent, String name, String uuid) {
        this.referenceable = parent.referenceable;
        this.parent = parent;
        this.name = name;
        this.identifier = uuid; // possibly null
    }

    public IdentifierTracker getChildTracker(String name, NodeState state) {
        String uuid = state.getString(JCR_UUID);
        if (uuid != null && !referenceable.apply(state)) {
            uuid = null; // discard jcr:uuid value of a non-referenceable node
        }
        return new IdentifierTracker(this, name, uuid);
    }

    public String getIdentifier() {
        if (identifier == null) { // implies parent != null
            identifier = PathUtils.concat(parent.getIdentifier(), name);
        }
        return identifier;
    }

}
