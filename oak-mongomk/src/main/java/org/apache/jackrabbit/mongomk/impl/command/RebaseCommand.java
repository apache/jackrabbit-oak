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
package org.apache.jackrabbit.mongomk.impl.command;

import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.UUID;

import org.apache.jackrabbit.mk.model.tree.DiffBuilder;
import org.apache.jackrabbit.mk.model.tree.NodeState;
import org.apache.jackrabbit.mongomk.api.model.Node;
import org.apache.jackrabbit.mongomk.impl.MongoNodeStore;
import org.apache.jackrabbit.mongomk.impl.action.FetchCommitAction;
import org.apache.jackrabbit.mongomk.impl.action.FetchHeadRevisionIdAction;
import org.apache.jackrabbit.mongomk.impl.model.CommitBuilder;
import org.apache.jackrabbit.mongomk.impl.model.MongoCommit;
import org.apache.jackrabbit.mongomk.impl.model.NodeImpl;
import org.apache.jackrabbit.mongomk.impl.model.tree.MongoNodeDelta;
import org.apache.jackrabbit.mongomk.impl.model.tree.MongoNodeState;
import org.apache.jackrabbit.mongomk.impl.model.tree.SimpleMongoNodeStore;
import org.apache.jackrabbit.mongomk.util.MongoUtil;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkArgument;
import static org.apache.jackrabbit.mongomk.util.MongoUtil.fromMongoRepresentation;
import static org.apache.jackrabbit.mongomk.util.MongoUtil.getBaseRevision;
import static org.apache.jackrabbit.mongomk.util.MongoUtil.toMongoRepresentation;

/**
 * A {@code Command} for {@link org.apache.jackrabbit.mongomk.impl.MongoMicroKernel#rebase(String, String)}
 */
public class RebaseCommand extends BaseCommand<String> {
    private static final Logger LOG = LoggerFactory.getLogger(RebaseCommand.class);

    private final String branchRevisionId;
    private final String newBaseRevisionId;

    public RebaseCommand(MongoNodeStore nodeStore, String branchRevisionId, String newBaseRevisionId) {
        super(nodeStore);
        this.branchRevisionId = branchRevisionId;
        this.newBaseRevisionId = newBaseRevisionId;
    }

    private MongoCommit getCommit(String revisionId) throws Exception {
        return new FetchCommitAction(nodeStore, toMongoRepresentation(revisionId)).execute();
    }

    @Override
    public String execute() throws Exception {
        MongoCommit branchCommit = getCommit(branchRevisionId);
        String branchId = branchCommit.getBranchId();
        checkArgument(branchId != null, "Can only rebase a private branch commit");

        MongoCommit newBaseCommit;
        if (newBaseRevisionId == null) {
            Long headRevision = new FetchHeadRevisionIdAction(nodeStore).execute();
            newBaseCommit = new FetchCommitAction(nodeStore, headRevision).execute();
        }
        else {
            newBaseCommit = getCommit(newBaseRevisionId);
        }
        checkArgument(newBaseCommit.getBranchId() == null, "Cannot rebase onto private branch commit");

        long branchHeadRev = branchCommit.getRevisionId();
        long branchBaseRev = getBaseRevision(branchId);
        long newBaseRev = newBaseCommit.getRevisionId();

        if (newBaseRev == branchBaseRev) {
            return branchRevisionId;
        }

        Node branchHeadRoot = getNode("/", branchHeadRev, branchId);
        Node branchBaseRoot = getNode("/", branchBaseRev);
        Node newBaseRoot = getNode("/", newBaseRev);
        branchHeadRoot = rebaseNode(copy(newBaseRoot), branchBaseRoot, branchHeadRoot, "/");

        String diff = new DiffBuilder(
                MongoUtil.wrap(newBaseRoot),
                MongoUtil.wrap(branchHeadRoot),
                "/", -1, new SimpleMongoNodeStore(), "")
            .build();

        MongoCommit newBranchInitialCommit = (MongoCommit)CommitBuilder.build("", "",
                fromMongoRepresentation(newBaseRev), MongoNodeStore.INITIAL_COMMIT_MESSAGE);
        newBranchInitialCommit.setBranchId(fromMongoRepresentation(newBaseRev) + '-' + UUID.randomUUID().toString());
        String newBranchRevId = nodeStore.commit(newBranchInitialCommit);

        if (diff.isEmpty()) {
            return newBranchRevId;
        }

        MongoCommit newCommit = (MongoCommit) CommitBuilder.build("", diff, newBranchRevId,
                "rebased " + branchHeadRev + " onto " + newBaseRev);
        newCommit.setBranchId(newBranchInitialCommit.getBranchId());
        return nodeStore.commit(newCommit);
    }

    private static NodeImpl rebaseNode(Node baseNode, Node fromNode, Node toNode, String path) {
        MongoNodeDelta theirDelta = new MongoNodeDelta(new SimpleMongoNodeStore(),
                MongoUtil.wrap(fromNode), MongoUtil.wrap(baseNode));
        MongoNodeDelta ourDelta = new MongoNodeDelta(new SimpleMongoNodeStore(),
                MongoUtil.wrap(fromNode), MongoUtil.wrap(toNode));

        NodeImpl stagedNode = (NodeImpl)baseNode;

        // Apply our changes.
        for (Entry<String, String> added : ourDelta.getAddedProperties().entrySet()) {
            String name = added.getKey();
            String ourValue = added.getValue();
            String theirValue = theirDelta.getAddedProperties().get(name);

            if (theirValue != null && !theirValue.equals(ourValue)) {
                markConflict(stagedNode, "addExistingProperty", name, ourValue);
            }
            else {
                stagedNode.getProperties().put(name, ourValue);
            }
        }

        for (Entry<String, String> removed : ourDelta.getRemovedProperties().entrySet()) {
            String name = removed.getKey();
            String ourValue = removed.getValue();

            if (theirDelta.getRemovedProperties().containsKey(name)) {
                markConflict(stagedNode, "deleteDeletedProperty", name, ourValue);
            }
            else if (theirDelta.getChangedProperties().containsKey(name)) {
                markConflict(stagedNode, "deleteChangedProperty", name, ourValue);
            }
            else {
                stagedNode.getProperties().remove(name);
            }
        }

        for (Entry<String, String> changed : ourDelta.getChangedProperties().entrySet()) {
            String name = changed.getKey();
            String ourValue = changed.getValue();
            String theirValue = theirDelta.getChangedProperties().get(name);

            if (theirDelta.getRemovedProperties().containsKey(name)) {
                markConflict(stagedNode, "changeDeletedProperty", name, ourValue);
            }
            else if (theirValue != null && !theirValue.equals(ourValue)) {
                markConflict(stagedNode, "changeChangedProperty", name, ourValue);
            }
            else {
                stagedNode.getProperties().put(name, ourValue);
            }
        }

        for (Entry<String, NodeState> added : ourDelta.getAddedChildNodes().entrySet()) {
            String name = added.getKey();
            NodeState ourState = added.getValue();
            NodeState theirState = theirDelta.getAddedChildNodes().get(name);

            if (theirState != null && !theirState.equals(ourState)) {
                markConflict(stagedNode, "addExistingNode", name, ourState);
            }
            else {
                stagedNode.addChildNodeEntry(unwrap(ourState));
            }
        }

        for (Entry<String, NodeState> removed : ourDelta.getRemovedChildNodes().entrySet()) {
            String name = removed.getKey();
            NodeState ourState = removed.getValue();

            if (theirDelta.getRemovedChildNodes().containsKey(name)) {
                markConflict(stagedNode, "deleteDeletedNode", name, ourState);
            }
            else if (theirDelta.getChangedChildNodes().containsKey(name)) {
                markConflict(stagedNode, "deleteChangedNode", name, ourState);
            }
            else {
                stagedNode.removeChildNodeEntry(name);
            }
        }

        for (Entry<String, NodeState> changed : ourDelta.getChangedChildNodes().entrySet()) {
            String name = changed.getKey();
            NodeState ourState = changed.getValue();

            Node changedBase = baseNode.getChildNodeEntry(name);
            if (changedBase == null) {
                markConflict(stagedNode, "changeDeletedNode", name, ourState);
                continue;
            }

            Node changedFrom = fromNode.getChildNodeEntry(name);
            Node changedTo = toNode.getChildNodeEntry(name);
            String changedPath = PathUtils.concat(path, name);
            rebaseNode(changedBase, changedFrom, changedTo, changedPath);
        }

        return stagedNode;
    }

    private static Node unwrap(NodeState state) {
        return ((MongoNodeState) state).unwrap();
    }

    private static void markConflict(NodeImpl parent, String conflictType, String name, String ourValue) {
        NodeImpl marker = getOrAddConflictMarker(parent, conflictType);
        marker.getProperties().put(name, ourValue);
    }

    private static void markConflict(NodeImpl parent, String conflictType, String name, NodeState ourState) {
        NodeImpl marker = getOrAddConflictMarker(parent, conflictType);
        marker.addChildNodeEntry(unwrap(ourState));
    }

    private static NodeImpl getOrAddConflictMarker(NodeImpl parent, String name) {
        NodeImpl conflict = getOrAddNode(parent, ":conflict");
        return getOrAddNode(conflict, name);
    }

    private static NodeImpl getOrAddNode(NodeImpl parent, String name) {
        Node child = parent.getChildNodeEntry(name);
        if (child == null) {
            child = new NodeImpl(PathUtils.concat(parent.getPath(), name));
            parent.addChildNodeEntry(child);
        }
        return (NodeImpl) child;
    }

    private Node getNode(String path, long revisionId) throws Exception {
        return getNode(path, revisionId, null);
    }

    private Node getNode(String path, long revisionId, String branchId) throws Exception {
        GetNodesCommand command = new GetNodesCommand(nodeStore, path, revisionId);
        command.setBranchId(branchId);
        return command.execute();
    }

    private static NodeImpl copy(Node node) {
        NodeImpl copy = new NodeImpl(node.getPath());
        copy.setRevisionId(node.getRevisionId());
        for (Map.Entry<String, String> entry : node.getProperties().entrySet()) {
            copy.addProperty(entry.getKey(), entry.getValue());
        }
        for (Iterator<Node> it = node.getChildNodeEntries(0, -1); it.hasNext(); ) {
            Node child = it.next();
            copy.addChildNodeEntry(copy(child));
        }
        return copy;
    }

}
