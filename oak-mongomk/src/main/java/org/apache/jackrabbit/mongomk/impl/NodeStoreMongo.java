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
package org.apache.jackrabbit.mongomk.impl;

import java.util.List;
import java.util.Map;

import org.apache.jackrabbit.mk.api.MicroKernelException;
import org.apache.jackrabbit.mk.json.JsopBuilder;
import org.apache.jackrabbit.mk.model.Id;
import org.apache.jackrabbit.mk.model.tree.DiffBuilder;
import org.apache.jackrabbit.mk.model.tree.NodeState;
import org.apache.jackrabbit.mongomk.api.NodeStore;
import org.apache.jackrabbit.mongomk.api.command.Command;
import org.apache.jackrabbit.mongomk.api.command.CommandExecutor;
import org.apache.jackrabbit.mongomk.api.model.Commit;
import org.apache.jackrabbit.mongomk.api.model.Node;
import org.apache.jackrabbit.mongomk.command.CommitCommandMongo;
import org.apache.jackrabbit.mongomk.command.GetHeadRevisionCommandMongo;
import org.apache.jackrabbit.mongomk.command.GetNodesCommandMongo;
import org.apache.jackrabbit.mongomk.command.NodeExistsCommandMongo;
import org.apache.jackrabbit.mongomk.impl.command.CommandExecutorImpl;
import org.apache.jackrabbit.mongomk.impl.model.CommitBuilder;
import org.apache.jackrabbit.mongomk.impl.model.NodeImpl;
import org.apache.jackrabbit.mongomk.impl.model.tree.MongoNodeDelta;
import org.apache.jackrabbit.mongomk.impl.model.tree.MongoNodeState;
import org.apache.jackrabbit.mongomk.impl.model.tree.MongoNodeStore;
import org.apache.jackrabbit.mongomk.model.CommitMongo;
import org.apache.jackrabbit.mongomk.query.FetchCommitQuery;
import org.apache.jackrabbit.mongomk.query.FetchHeadRevisionIdQuery;
import org.apache.jackrabbit.mongomk.query.FetchCommitsQuery;
import org.apache.jackrabbit.mongomk.util.MongoUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implementation of {@link NodeStore} for the {@code MongoDB}.
 */
public class NodeStoreMongo implements NodeStore {

    private static final Logger LOG = LoggerFactory.getLogger(NodeStoreMongo.class);
    private static final long WAIT_FOR_COMMIT_POLL_MILLIS = 1000;

    private final CommandExecutor commandExecutor;
    private final MongoConnection mongoConnection;

    /**
     * Constructs a new {@code NodeStoreMongo}.
     *
     * @param mongoConnection The {@link MongoConnection}.
     */
    public NodeStoreMongo(MongoConnection mongoConnection) {
        this.mongoConnection = mongoConnection;
        commandExecutor = new CommandExecutorImpl();
    }

    @Override
    public String commit(Commit commit) throws Exception {
        Command<Long> command = new CommitCommandMongo(mongoConnection, commit);
        Long revision = commandExecutor.execute(command);
        return MongoUtil.fromMongoRepresentation(revision);
    }

    @Override
    public String diff(String fromRevision, String toRevision, String path, int depth)
            throws Exception {
        path = (path == null || path.isEmpty())? "/" : path;

        if (depth < -1) {
            throw new IllegalArgumentException("depth");
        }

        Long fromRevisionId, toRevisionId;
        if (fromRevision == null || toRevision == null) {
            Long head = new FetchHeadRevisionIdQuery(mongoConnection).execute();
            fromRevisionId = fromRevision == null? head : MongoUtil.toMongoRepresentation(fromRevision);
            toRevisionId = toRevision == null ? head : MongoUtil.toMongoRepresentation(toRevision);;
        } else {
            fromRevisionId = MongoUtil.toMongoRepresentation(fromRevision);
            toRevisionId = MongoUtil.toMongoRepresentation(toRevision);;
        }

        if (fromRevisionId.equals(toRevisionId)) {
            return "";
        }

        if ("/".equals(path)) {
            CommitMongo toCommit = new FetchCommitQuery(mongoConnection, toRevisionId).execute();
            if (toCommit.getBaseRevId() == fromRevisionId) {
                // Specified range spans a single commit:
                // use diff stored in commit instead of building it dynamically
                return toCommit.getDiff();
            }
        }

        NodeState beforeState = wrap(getNode(path, fromRevisionId));
        NodeState afterState = wrap(getNode(path, toRevisionId));

        return new DiffBuilder(beforeState, afterState, path, depth,
                new MongoNodeStore(), path).build();
    }

    @Override
    public String getHeadRevision() throws Exception {
        Long headRevision = commandExecutor.execute(new GetHeadRevisionCommandMongo(mongoConnection));
        return MongoUtil.fromMongoRepresentation(headRevision);
    }

    @Override
    public Node getNodes(String path, String revisionId, int depth, long offset,
            int maxChildNodes, String filter) throws Exception {
        GetNodesCommandMongo command = new GetNodesCommandMongo(mongoConnection, path,
                MongoUtil.toMongoRepresentation(revisionId));
        command.setDepth(depth);
        return commandExecutor.execute(command);
    }

    @Override
    public String merge(String branchRevisionId, String message) throws Exception {
        FetchCommitQuery query = new FetchCommitQuery(mongoConnection,
                MongoUtil.toMongoRepresentation(branchRevisionId));
        CommitMongo commit = query.execute();
        String branchId = commit.getBranchId();
        if (branchId == null) {
            throw new Exception("Can only merge a private branch commit");
        }

        Long rootNodeId = commit.getRevisionId();
        Long currentHead =  new FetchHeadRevisionIdQuery(mongoConnection).execute();

        Node ourRoot = getNode("/", rootNodeId, branchId);

        // FIXME - branchRootId might need to be real branch root it, rather
        // than base revision id.
        Long branchRootId = commit.getBaseRevId();

        // Merge nodes from head to branch.
        ourRoot = mergeNodes(ourRoot, currentHead, branchRootId);

        // FIXME - Handle the case when there are no changes.

        String diff = new DiffBuilder(wrap(getNode("/", currentHead)),
                wrap(ourRoot), "/", -1,
                new MongoNodeStore(), "").build();

        if (diff.isEmpty()) {
            LOG.debug("Merge of empty branch {} with differing content hashes encountered, " +
                    "ignore and keep current head {}", branchRevisionId, currentHead);
            return MongoUtil.fromMongoRepresentation(currentHead);
        }

        Commit newCommit = CommitBuilder.build("", diff,
                MongoUtil.fromMongoRepresentation(currentHead), message);

        return commit(newCommit);
    }

    @Override
    public boolean nodeExists(String path, String revisionId) throws Exception {
        String branchId = null;
        if (revisionId != null) {
            FetchCommitQuery query = new FetchCommitQuery(mongoConnection,
                    MongoUtil.toMongoRepresentation(revisionId));
            CommitMongo baseCommit = query.execute();
            branchId = baseCommit.getBranchId();
        }

        NodeExistsCommandMongo command = new NodeExistsCommandMongo(mongoConnection, path,
                MongoUtil.toMongoRepresentation(revisionId));
        command.setBranchId(branchId);
        return commandExecutor.execute(command);
    }

    @Override
    public String getJournal(String fromRevisionId, String toRevisionId, String path) {
        path = (path == null || "".equals(path)) ? "/" : path;
        boolean filtered = !"/".equals(path);

        // FIXME There's more work here.

        Long fromRevision = MongoUtil.toMongoRepresentation(fromRevisionId);
        Long toRevision = toRevisionId == null? new FetchHeadRevisionIdQuery(mongoConnection).execute()
                : MongoUtil.toMongoRepresentation(toRevisionId);

        List<CommitMongo> commits = new FetchCommitsQuery(mongoConnection,
                fromRevision, toRevision).execute();

        CommitMongo toCommit = getCommit(commits, toRevision);
        if (toCommit.getBranchId() != null) {
            throw new MicroKernelException("Branch revisions are not supported: " + toRevisionId);
        }

        CommitMongo fromCommit;
        if (toRevision == fromRevision) {
            fromCommit = toCommit;
        } else {
            fromCommit = getCommit(commits, fromRevision);
            if (fromCommit == null || (fromCommit.getTimestamp() > toCommit.getTimestamp())) {
                // negative range, return empty journal
                return "[]";
            }
        }
        if (fromCommit.getBranchId() != null) {
            throw new MicroKernelException("Branch revisions are not supported: " + fromRevisionId);
        }

        JsopBuilder commitBuff = new JsopBuilder().array();
        // iterate over commits in chronological order,
        // starting with oldest commit
        for (int i = commits.size() - 1; i >= 0; i--) {
            CommitMongo commit = commits.get(i);
            //if (commit.getParentId() == null) {
            //   continue;
            //}
            String diff = commit.getDiff();
            // FIXME Check that filter really works.
            if (!filtered || commit.getAffectedPaths().contains(path)) {
                commitBuff.object()
                .key("id").value(MongoUtil.fromMongoRepresentation(commit.getRevisionId()))
                .key("ts").value(commit.getTimestamp())
                .key("msg").value(commit.getMessage())
                .key("changes").value(diff).endObject();
            }
        }
        return commitBuff.endArray().toString();
    }

    @Override
    public String getRevisionHistory(long since, int maxEntries, String path) {
      path = (path == null || "".equals(path)) ? "/" : path;
      boolean filtered = !"/".equals(path);
      maxEntries = maxEntries < 0 ? Integer.MAX_VALUE : maxEntries;

      FetchCommitsQuery query = new FetchCommitsQuery(mongoConnection, 0L,
              Long.MAX_VALUE);
      query.setMaxEntries(maxEntries);
      query.includeBranchCommits(false);

      List<CommitMongo> history = query.execute();
      JsopBuilder buff = new JsopBuilder().array();
      for (int i = history.size() - 1; i >= 0; i--) {
          CommitMongo commit = history.get(i);
          if (commit.getTimestamp() >= since) {
              // FIXME Check that filter really works.
              if (!filtered || commit.getAffectedPaths().contains(path)) {
                  buff.object()
                  .key("id").value(MongoUtil.fromMongoRepresentation(commit.getRevisionId()))
                  .key("ts").value(commit.getTimestamp())
                  .key("msg").value(commit.getMessage())
                  .endObject();
              }
          }
      }

      return buff.endArray().toString();
    }

    @Override
    public String waitForCommit(String oldHeadRevisionId, long timeout) throws InterruptedException {
        long startTimestamp = System.currentTimeMillis();
        long initialHeadRevisionId = new FetchHeadRevisionIdQuery(mongoConnection).execute();

        if (timeout <= 0) {
            return MongoUtil.fromMongoRepresentation(initialHeadRevisionId);
        }

        long oldHeadRevision = MongoUtil.toMongoRepresentation(oldHeadRevisionId);
        if (oldHeadRevision < initialHeadRevisionId) {
            return MongoUtil.fromMongoRepresentation(initialHeadRevisionId);
        }

        long waitForCommitPollMillis = Math.min(WAIT_FOR_COMMIT_POLL_MILLIS, timeout);
        while (true) {
            long headRevisionId = new FetchHeadRevisionIdQuery(mongoConnection).execute();
            long now = System.currentTimeMillis();
            if (headRevisionId != initialHeadRevisionId || now - startTimestamp >= timeout) {
                return MongoUtil.fromMongoRepresentation(headRevisionId);
            }
            Thread.sleep(waitForCommitPollMillis);
        }
    }

    private CommitMongo getCommit(List<CommitMongo> commits, Long revisionId) {
        for (CommitMongo commit : commits) {
            if (commit.getRevisionId() == revisionId) {
                return commit;
            }
        }
        return null;
    }

    private Node getNode(String path, Long revisionId) throws Exception {
        return getNode(path, revisionId, null);
    }

    private Node getNode(String path, Long revisionId, String branchId) throws Exception {
        // FIXME - Should this use FetchNodesByPath instead?
        GetNodesCommandMongo command = new GetNodesCommandMongo(mongoConnection,
                path, revisionId);
        command.setBranchId(branchId);
        return command.execute();
    }

    // FIXME - Make sure merge related functions work as expected.
    private NodeImpl mergeNodes(Node ourRoot, Long newBaseRevisionId,
            Long commonAncestorRevisionId) throws Exception {

        Node baseRoot = getNode("/", commonAncestorRevisionId);
        Node theirRoot = getNode("/", newBaseRevisionId);

        // Recursively merge 'our' changes with 'their' changes...
        NodeImpl mergedNode = mergeNode(baseRoot, ourRoot, theirRoot, "/");

        return mergedNode;
    }

    private NodeImpl mergeNode(Node baseNode, Node ourNode, Node theirNode,
            String path) throws Exception {
        MongoNodeDelta theirChanges = new MongoNodeDelta(new MongoNodeStore(),
                wrap(baseNode), wrap(theirNode));
        MongoNodeDelta ourChanges = new MongoNodeDelta(new MongoNodeStore(),
                wrap(baseNode), wrap(ourNode));

        NodeImpl stagedNode = (NodeImpl)theirNode; //new NodeImpl(path);

        // Merge non-conflicting changes
        stagedNode.getProperties().putAll(ourChanges.getAddedProperties());
        stagedNode.getProperties().putAll(ourChanges.getChangedProperties());
        for (String name : ourChanges.getRemovedProperties().keySet()) {
            stagedNode.getProperties().remove(name);
        }

        for (Map.Entry<String, NodeState> entry : ourChanges.getAddedChildNodes().entrySet()) {
            MongoNodeState nodeState = (MongoNodeState)entry.getValue();
            stagedNode.addChild(nodeState.unwrap());
            //stagedNode.addChild(new NodeImpl(entry.getKey()));
        }
        for (Map.Entry<String, Id> entry : ourChanges.getChangedChildNodes().entrySet()) {
            stagedNode.addChild(new NodeImpl(entry.getKey()));
        }
        // FIXME
//        for (String name : ourChanges.getRemovedChildNodes().keySet()) {
//            stagedNode.remove(name);
//        }

//        List<NodeDelta.Conflict> conflicts = theirChanges.listConflicts(ourChanges);
//        // resolve/report merge conflicts
//        for (NodeDelta.Conflict conflict : conflicts) {
//            String conflictName = conflict.getName();
//            String conflictPath = PathUtils.concat(path, conflictName);
//            switch (conflict.getType()) {
//                case PROPERTY_VALUE_CONFLICT:
//                    throw new Exception(
//                            "concurrent modification of property " + conflictPath
//                                    + " with conflicting values: \""
//                                    + ourNode.getProperties().get(conflictName)
//                                    + "\", \""
//                                    + theirNode.getProperties().get(conflictName));
//
//                case NODE_CONTENT_CONFLICT: {
//                    if (ourChanges.getChangedChildNodes().containsKey(conflictName)) {
//                        // modified subtrees
//                        StoredNode baseChild = store.getNode(baseNode.getChildNodeEntry(conflictName).getId());
//                        StoredNode ourChild = store.getNode(ourNode.getChildNodeEntry(conflictName).getId());
//                        StoredNode theirChild = store.getNode(theirNode.getChildNodeEntry(conflictName).getId());
//                        // merge the dirty subtrees recursively
//                        mergeNode(baseChild, ourChild, theirChild, PathUtils.concat(path, conflictName));
//                    } else {
//                        // todo handle/merge colliding node creation
//                        throw new Exception("colliding concurrent node creation: " + conflictPath);
//                    }
//                    break;
//                }
//
//                case REMOVED_DIRTY_PROPERTY_CONFLICT:
//                    stagedNode.getProperties().remove(conflictName);
//                    break;
//
//                case REMOVED_DIRTY_NODE_CONFLICT:
//                    stagedNode.remove(conflictName);
//                    break;
//            }
//
//        }
        return stagedNode;
    }

    private NodeState wrap(Node node) {
        return node != null? new MongoNodeState(node) : null;
    }
}