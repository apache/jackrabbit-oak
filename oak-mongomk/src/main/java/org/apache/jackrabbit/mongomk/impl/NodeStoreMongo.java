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

import java.util.ArrayList;
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
import org.apache.jackrabbit.mongomk.query.FetchCommitsQuery;
import org.apache.jackrabbit.mongomk.query.FetchHeadRevisionIdQuery;
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

        long fromRevisionId, toRevisionId;
        if (fromRevision == null || toRevision == null) {
            long head = getHeadRevision(true);
            fromRevisionId = fromRevision == null? head : MongoUtil.toMongoRepresentation(fromRevision);
            toRevisionId = toRevision == null ? head : MongoUtil.toMongoRepresentation(toRevision);;
        } else {
            fromRevisionId = MongoUtil.toMongoRepresentation(fromRevision);
            toRevisionId = MongoUtil.toMongoRepresentation(toRevision);;
        }

        if (fromRevisionId == toRevisionId) {
            return "";
        }

        if ("/".equals(path)) {
            CommitMongo toCommit = getCommit(toRevisionId);
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
        command.setBranchId(getBranchId(revisionId));
        command.setDepth(depth);
        return commandExecutor.execute(command);
    }

    @Override
    public String merge(String branchRevisionId, String message) throws Exception {
        CommitMongo commit = getCommit(MongoUtil.toMongoRepresentation(branchRevisionId));
        String branchId = commit.getBranchId();
        if (branchId == null) {
            throw new Exception("Can only merge a private branch commit");
        }

        long rootNodeId = commit.getRevisionId();
        long currentHead =  getHeadRevision(false);

        Node ourRoot = getNode("/", rootNodeId, branchId);

        // FIXME - branchRootId might need to be real branch root it, rather
        // than base revision id.
        long branchRootId = commit.getBaseRevId();

        // Merge nodes from head to branch.
        ourRoot = mergeNodes(ourRoot, currentHead, branchRootId);

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
        String branchId = getBranchId(revisionId);
        NodeExistsCommandMongo command = new NodeExistsCommandMongo(mongoConnection, path,
                MongoUtil.toMongoRepresentation(revisionId));
        command.setBranchId(branchId);
        return commandExecutor.execute(command);
    }

    @Override
    public String getJournal(String fromRevisionId, String toRevisionId, String path)
            throws Exception {
        path = (path == null || "".equals(path)) ? "/" : path;
        boolean filtered = !"/".equals(path);

        long fromRevision = MongoUtil.toMongoRepresentation(fromRevisionId);
        long toRevision = toRevisionId == null? getHeadRevision(true)
                : MongoUtil.toMongoRepresentation(toRevisionId);

        List<CommitMongo> commits = getCommits(fromRevision, toRevision);

        CommitMongo toCommit = extractCommit(commits, toRevision);
        if (toCommit.getBranchId() != null) {
            throw new MicroKernelException("Branch revisions are not supported: " + toRevisionId);
        }

        CommitMongo fromCommit;
        if (toRevision == fromRevision) {
            fromCommit = toCommit;
        } else {
            fromCommit = extractCommit(commits, fromRevision);
            if (fromCommit == null || (fromCommit.getTimestamp() > toCommit.getTimestamp())) {
                // negative range, return empty journal
                return "[]";
            }
        }
        if (fromCommit.getBranchId() != null) {
            throw new MicroKernelException("Branch revisions are not supported: " + fromRevisionId);
        }

        JsopBuilder commitBuff = new JsopBuilder().array();
        // Iterate over commits in chronological order, starting with oldest commit
        for (int i = commits.size() - 1; i >= 0; i--) {
            CommitMongo commit = commits.get(i);

            String diff = commit.getDiff();
            if (filtered) {
                try {
                    diff = new DiffBuilder(
                            wrap(getNode("/", commit.getBaseRevId())),
                            wrap(getNode("/", commit.getRevisionId())),
                            "/", -1, new MongoNodeStore(), path).build();
                    if (diff.isEmpty()) {
                        continue;
                    }
                } catch (Exception e) {
                    throw new MicroKernelException(e);
                }
            }
            commitBuff.object()
            .key("id").value(MongoUtil.fromMongoRepresentation(commit.getRevisionId()))
            .key("ts").value(commit.getTimestamp())
            .key("msg").value(commit.getMessage())
            .key("changes").value(diff).endObject();
        }
        return commitBuff.endArray().toString();
    }

    @Override
    public String getRevisionHistory(long since, int maxEntries, String path) {
      path = (path == null || "".equals(path)) ? "/" : path;
      boolean filtered = !"/".equals(path);

      maxEntries = maxEntries < 0 ? Integer.MAX_VALUE : maxEntries;

      FetchCommitsQuery query = new FetchCommitsQuery(mongoConnection);
      query.setMaxEntries(maxEntries);
      query.includeBranchCommits(false);

      List<CommitMongo> commits = query.execute();
      List<CommitMongo> history = new ArrayList<CommitMongo>();
      for (int i = commits.size() - 1; i >= 0; i--) {
          CommitMongo commit = commits.get(i);
          if (commit.getTimestamp() >= since) {
              if (filtered) {
                  try {
                      String diff = new DiffBuilder(
                              wrap(getNode("/", commit.getBaseRevId())),
                              wrap(getNode("/", commit.getRevisionId())),
                              "/", -1, new MongoNodeStore(), path).build();
                      if (!diff.isEmpty()) {
                          history.add(commit);
                      }
                  } catch (Exception e) {
                      throw new MicroKernelException(e);
                  }
              } else {
                  history.add(commit);
              }
          }
      }

      JsopBuilder buff = new JsopBuilder().array();
      for (CommitMongo commit : history) {
          buff.object()
          .key("id").value(MongoUtil.fromMongoRepresentation(commit.getRevisionId()))
          .key("ts").value(commit.getTimestamp())
          .key("msg").value(commit.getMessage())
          .endObject();
      }
      return buff.endArray().toString();
    }

    @Override
    public String waitForCommit(String oldHeadRevisionId, long timeout) throws Exception {
        long startTimestamp = System.currentTimeMillis();
        long initialHeadRevisionId = getHeadRevision(true);

        if (timeout <= 0) {
            return MongoUtil.fromMongoRepresentation(initialHeadRevisionId);
        }

        long oldHeadRevision = MongoUtil.toMongoRepresentation(oldHeadRevisionId);
        if (oldHeadRevision < initialHeadRevisionId) {
            return MongoUtil.fromMongoRepresentation(initialHeadRevisionId);
        }

        long waitForCommitPollMillis = Math.min(WAIT_FOR_COMMIT_POLL_MILLIS, timeout);
        while (true) {
            long headRevisionId = getHeadRevision(true);
            long now = System.currentTimeMillis();
            if (headRevisionId != initialHeadRevisionId || now - startTimestamp >= timeout) {
                return MongoUtil.fromMongoRepresentation(headRevisionId);
            }
            Thread.sleep(waitForCommitPollMillis);
        }
    }

    private CommitMongo extractCommit(List<CommitMongo> commits, long revisionId) {
        for (CommitMongo commit : commits) {
            if (commit.getRevisionId() == revisionId) {
                return commit;
            }
        }
        return null;
    }

    private String getBranchId(String revisionId) throws Exception {
        if (revisionId == null) {
            return null;
        }

        CommitMongo baseCommit = getCommit(MongoUtil.toMongoRepresentation(revisionId));
        return baseCommit.getBranchId();
    }

    private CommitMongo getCommit(long revisionId) throws Exception {
        FetchCommitQuery query = new FetchCommitQuery(mongoConnection, revisionId);
        return query.execute();
    }

    private List<CommitMongo> getCommits(long fromRevisionId, long toRevisionId) throws Exception {
        FetchCommitsQuery query = new FetchCommitsQuery(mongoConnection, fromRevisionId,
                toRevisionId);
        return query.execute();
    }
    private long getHeadRevision(boolean includeBranchCommits) throws Exception {
        FetchHeadRevisionIdQuery query = new FetchHeadRevisionIdQuery(mongoConnection);
        query.includeBranchCommits(includeBranchCommits);
        return query.execute();
    }

    private Node getNode(String path, Long revisionId) throws Exception {
        return getNode(path, revisionId, null);
    }

    private Node getNode(String path, Long revisionId, String branchId) throws Exception {
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

        // Apply our changes.
        stagedNode.getProperties().putAll(ourChanges.getAddedProperties());
        stagedNode.getProperties().putAll(ourChanges.getChangedProperties());
        for (String name : ourChanges.getRemovedProperties().keySet()) {
            stagedNode.getProperties().remove(name);
        }

        for (Map.Entry<String, NodeState> entry : ourChanges.getAddedChildNodes().entrySet()) {
            MongoNodeState nodeState = (MongoNodeState)entry.getValue();
            stagedNode.addChildNodeEntry(nodeState.unwrap());
            //stagedNode.addChild(new NodeImpl(entry.getKey()));
        }
        for (Map.Entry<String, Id> entry : ourChanges.getChangedChildNodes().entrySet()) {
            if (!theirChanges.getChangedChildNodes().containsKey(entry.getKey())) {
                stagedNode.addChildNodeEntry(new NodeImpl(entry.getKey()));
            }
        }
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