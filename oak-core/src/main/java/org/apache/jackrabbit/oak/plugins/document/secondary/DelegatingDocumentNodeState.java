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

package org.apache.jackrabbit.oak.plugins.document.secondary;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.plugins.document.AbstractDocumentNodeState;
import org.apache.jackrabbit.oak.plugins.document.NodeStateDiffer;
import org.apache.jackrabbit.oak.plugins.document.RevisionVector;
import org.apache.jackrabbit.oak.plugins.memory.MemoryChildNodeEntry;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeBuilder;
import org.apache.jackrabbit.oak.spi.state.ChildNodeEntry;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static org.apache.jackrabbit.oak.commons.PathUtils.denotesRoot;

/**
 * NodeState wrapper which wraps another NodeState (mostly SegmentNodeState)
 * so as to expose it as an {@link AbstractDocumentNodeState} by extracting
 * the meta properties which are stored as hidden properties
 */
class DelegatingDocumentNodeState extends AbstractDocumentNodeState {
    //Hidden props holding DocumentNodeState meta properties
    static final String PROP_PATH = ":doc-path";
    static final String PROP_REVISION = ":doc-rev";
    static final String PROP_LAST_REV = ":doc-lastRev";

    private static final Predicate<PropertyState> NOT_META_PROPS = new Predicate<PropertyState>() {
        @Override
        public boolean apply(@Nullable PropertyState input) {
            return !input.getName().startsWith(":doc-");
        }
    };

    private final NodeState delegate;
    private final RevisionVector rootRevision;
    private final boolean fromExternalChange;
    private final RevisionVector lastRevision;
    private final RevisionVector readRevision;
    private final String path;

    /**
     * Wraps a given NodeState as a {@link DelegatingDocumentNodeState} if
     * it has required meta properties otherwise just returns the passed NodeState
     *
     * @param delegate nodeState to wrap
     * @return wrapped state or original state
     */
    public static NodeState wrapIfPossible(NodeState delegate, NodeStateDiffer differ) {
        if (hasMetaProps(delegate)) {
            String revVector = getRequiredProp(delegate, PROP_REVISION);
            return new DelegatingDocumentNodeState(delegate, RevisionVector.fromString(revVector), differ);
        }
        return delegate;
    }

    public static boolean hasMetaProps(NodeState delegate) {
        return delegate.hasProperty(PROP_REVISION);
    }

    public static AbstractDocumentNodeState wrap(NodeState delegate, NodeStateDiffer differ) {
        String revVector = getRequiredProp(delegate, PROP_REVISION);
        return new DelegatingDocumentNodeState(delegate, RevisionVector.fromString(revVector), differ);
    }

    public DelegatingDocumentNodeState(NodeState delegate, RevisionVector rootRevision, NodeStateDiffer differ) {
        this(delegate, rootRevision, false, differ);
    }

    public DelegatingDocumentNodeState(NodeState delegate, RevisionVector rootRevision,
                                       boolean fromExternalChange, NodeStateDiffer differ) {
        super(differ);
        this.delegate = delegate;
        this.rootRevision = rootRevision;
        this.fromExternalChange = fromExternalChange;
        this.path = getRequiredProp(PROP_PATH);
        this.readRevision = RevisionVector.fromString(getRequiredProp(PROP_REVISION));
        this.lastRevision = RevisionVector.fromString(getRequiredProp(PROP_LAST_REV));
    }

    private DelegatingDocumentNodeState(DelegatingDocumentNodeState original,
                                        RevisionVector rootRevision, boolean fromExternalChange) {
        super(original.differ);
        this.delegate = original.delegate;
        this.rootRevision = rootRevision;
        this.fromExternalChange = fromExternalChange;
        this.path = original.path;
        this.readRevision = original.readRevision;
        this.lastRevision = original.lastRevision;
    }

    //~----------------------------------< AbstractDocumentNodeState >

    @Override
    public String getPath() {
        return path;
    }

    @Override
    public RevisionVector getRevision() {
        return readRevision;
    }

    @Override
    public RevisionVector getLastRevision() {
        return lastRevision;
    }

    @Override
    public RevisionVector getRootRevision() {
        return rootRevision;
    }

    @Override
    public boolean isFromExternalChange() {
        return fromExternalChange;
    }

    @Override
    public AbstractDocumentNodeState withRootRevision(@Nonnull RevisionVector root, boolean externalChange) {
        if (rootRevision.equals(root) && fromExternalChange == externalChange) {
            return this;
        } else {
            return new DelegatingDocumentNodeState(this, root, externalChange);
        }
    }

    @Override
    public boolean hasNoChildren() {
        //Passing max as 1 so as to minimize any overhead.
        return delegate.getChildNodeCount(1) == 0;
    }

    //~----------------------------------< NodeState >

    @Override
    public boolean exists() {
        return true;
    }

    @Nonnull
    @Override
    public Iterable<? extends PropertyState> getProperties() {
        return Iterables.filter(delegate.getProperties(), NOT_META_PROPS);
    }

    @Override
    public boolean hasChildNode(@Nonnull String name) {
        return delegate.hasChildNode(name);
    }

    @Nonnull
    @Override
    public NodeState getChildNode(@Nonnull String name) throws IllegalArgumentException {
        return decorate(delegate.getChildNode(name));
    }

    @Nonnull
    @Override
    public Iterable<? extends ChildNodeEntry> getChildNodeEntries() {
        return Iterables.transform(delegate.getChildNodeEntries(), new Function<ChildNodeEntry, ChildNodeEntry>() {
            @Nullable
            @Override
            public ChildNodeEntry apply(@Nullable ChildNodeEntry input) {
                return new MemoryChildNodeEntry(input.getName(), decorate(input.getNodeState()));
            }
        });
    }

    @Nonnull
    @Override
    public NodeBuilder builder() {
        checkState(!denotesRoot(getPath()), "Builder cannot be opened for root " +
                "path for state of type [%s]", delegate.getClass());
        return new MemoryNodeBuilder(this);
    }

    //~--------------------------------------------< internal >

    private NodeState decorate(NodeState childNode) {
        if (childNode.exists()) {
            return new DelegatingDocumentNodeState(childNode, rootRevision, fromExternalChange, differ);
        }
        return childNode;
    }

    private String getRequiredProp(String name){
        return getRequiredProp(delegate, name);
    }

    private static String getRequiredProp(NodeState state, String name){
        return checkNotNull(state.getString(name), "No property [%s] found in [%s]", name, state);
    }
}
