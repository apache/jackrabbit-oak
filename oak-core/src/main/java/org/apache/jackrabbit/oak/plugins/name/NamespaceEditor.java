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
package org.apache.jackrabbit.oak.plugins.name;

import static javax.jcr.NamespaceRegistry.PREFIX_JCR;
import static javax.jcr.NamespaceRegistry.PREFIX_MIX;
import static javax.jcr.NamespaceRegistry.PREFIX_NT;
import static org.apache.jackrabbit.JcrConstants.JCR_PRIMARYTYPE;
import static org.apache.jackrabbit.JcrConstants.JCR_SYSTEM;
import static org.apache.jackrabbit.oak.api.Type.STRING;
import static org.apache.jackrabbit.oak.spi.namespace.NamespaceConstants.REP_NAMESPACES;
import static org.apache.jackrabbit.oak.spi.namespace.NamespaceConstants.REP_NSDATA;
import static org.apache.jackrabbit.oak.spi.namespace.NamespaceConstants.REP_URIS;
import static org.apache.jackrabbit.oak.plugins.name.Namespaces.isValidPrefix;
import static org.apache.jackrabbit.oak.plugins.name.Namespaces.safeGet;

import java.util.Locale;
import java.util.Set;

import com.google.common.collect.ImmutableSet;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.plugins.tree.TreeFactory;
import org.apache.jackrabbit.oak.spi.commit.DefaultEditor;
import org.apache.jackrabbit.oak.spi.commit.Editor;
import org.apache.jackrabbit.oak.spi.namespace.NamespaceConstants;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;

/**
 * TODO document
 */
class NamespaceEditor extends DefaultEditor {

    private final NodeBuilder builder;

    private boolean modified = false;

    private final NodeState namespaces;

    public NamespaceEditor(NodeState root, NodeBuilder builder) {
        this.namespaces = root.getChildNode(JCR_SYSTEM).getChildNode(
                REP_NAMESPACES);
        this.builder = builder;
    }

    @Override
    public void propertyAdded(PropertyState after) throws CommitFailedException {
        String prefix = after.getName();
        // ignore jcr:primaryType
        if (JCR_PRIMARYTYPE.equals(prefix)) {
            return;
        }

        if (namespaces.hasProperty(prefix)) {
            throw new CommitFailedException(CommitFailedException.NAMESPACE, 1,
                    "Namespace mapping already registered: " + prefix);
        } else if (isValidPrefix(prefix)) {
            if (after.isArray() || !STRING.equals(after.getType())) {
                throw new CommitFailedException(
                        CommitFailedException.NAMESPACE, 2,
                        "Invalid namespace mapping: " + prefix);
            } else if (prefix.toLowerCase(Locale.ENGLISH).startsWith("xml")) {
                throw new CommitFailedException(
                        CommitFailedException.NAMESPACE, 3,
                        "XML prefixes are reserved: " + prefix);
            } else if (containsValue(namespaces, after.getValue(STRING))) {
                throw modificationNotAllowed(prefix);
            }
        } else {
            throw new CommitFailedException(CommitFailedException.NAMESPACE, 4,
                    "Not a valid namespace prefix: " + prefix);
        }
        modified = true;
    }

    private static boolean containsValue(NodeState namespaces, String value) {
        return safeGet(TreeFactory.createReadOnlyTree(namespaces.getChildNode(REP_NSDATA)),
                REP_URIS).contains(value);
    }

    @Override
    public void propertyChanged(PropertyState before, PropertyState after)
            throws CommitFailedException {
        // TODO allow changes if there is no content referencing the mappings
        throw modificationNotAllowed(after.getName());
    }

    @Override
    public void propertyDeleted(PropertyState before)
            throws CommitFailedException {

        // FIXME Desired Behavior: if we enable it, there are a few generic
        // #unregister tests that fail
        // TODO allow changes if there is no content referencing the mappings
        // throw modificationNotAllowed(before.getName());

        // FIXME Best effort backwards compatible:
        if (jcrSystemNS.contains(before.getName())) {
            throw modificationNotAllowed(before.getName());
        }
        modified = true;
    }

    private static Set<String> jcrSystemNS = ImmutableSet.of(PREFIX_JCR,
            PREFIX_NT, PREFIX_MIX, NamespaceConstants.PREFIX_SV);

    private static CommitFailedException modificationNotAllowed(String prefix) {
        return new CommitFailedException(CommitFailedException.NAMESPACE, 5,
                "Namespace modification not allowed: " + prefix);
    }

    @Override
    public void leave(NodeState before, NodeState after)
            throws CommitFailedException {
        if (!modified) {
            return;
        }
        Namespaces.buildIndexNode(builder.child(JCR_SYSTEM).child(
                REP_NAMESPACES));
    }

    @Override
    public Editor childNodeChanged(String name, NodeState before,
            NodeState after) throws CommitFailedException {
        if (REP_NSDATA.equals(name) && !before.equals(after)) {
            throw modificationNotAllowed(name);
        }
        return null;
    }

}
