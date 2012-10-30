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

import org.apache.jackrabbit.mk.api.MicroKernelException;
import org.apache.jackrabbit.mk.json.JsopBuilder;
import org.apache.jackrabbit.mk.model.tree.DiffBuilder;
import org.apache.jackrabbit.mongomk.api.NodeStore;
import org.apache.jackrabbit.mongomk.api.command.Command;
import org.apache.jackrabbit.mongomk.api.command.CommandExecutor;
import org.apache.jackrabbit.mongomk.api.model.Commit;
import org.apache.jackrabbit.mongomk.api.model.Node;
import org.apache.jackrabbit.mongomk.command.CommitCommandMongo;
import org.apache.jackrabbit.mongomk.command.DiffCommandMongo;
import org.apache.jackrabbit.mongomk.command.GetHeadRevisionCommandMongo;
import org.apache.jackrabbit.mongomk.command.GetJournalCommandMongo;
import org.apache.jackrabbit.mongomk.command.GetNodesCommandMongo;
import org.apache.jackrabbit.mongomk.command.MergeCommandMongo;
import org.apache.jackrabbit.mongomk.command.NodeExistsCommandMongo;
import org.apache.jackrabbit.mongomk.impl.command.DefaultCommandExecutor;
import org.apache.jackrabbit.mongomk.impl.model.tree.MongoNodeStore;
import org.apache.jackrabbit.mongomk.model.CommitMongo;
import org.apache.jackrabbit.mongomk.query.FetchCommitQuery;
import org.apache.jackrabbit.mongomk.query.FetchCommitsQuery;
import org.apache.jackrabbit.mongomk.query.FetchHeadRevisionIdQuery;
import org.apache.jackrabbit.mongomk.util.MongoUtil;

/**
 * FIXME- Create commands out of methods if not already done so.
 *
 * Implementation of {@link NodeStore} for the {@code MongoDB}.
 */
public class NodeStoreMongo implements NodeStore {

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
        commandExecutor = new DefaultCommandExecutor();
    }

    @Override
    public String commit(Commit commit) throws Exception {
        Command<Long> command = new CommitCommandMongo(mongoConnection, commit);
        long revision = commandExecutor.execute(command);
        return MongoUtil.fromMongoRepresentation(revision);
    }

    @Override
    public String diff(String fromRevision, String toRevision, String path, int depth)
            throws Exception {
        Command<String> command = new DiffCommandMongo(mongoConnection,
                fromRevision, toRevision, path, depth);
        return commandExecutor.execute(command);
    }

    @Override
    public String getHeadRevision() throws Exception {
        GetHeadRevisionCommandMongo command = new GetHeadRevisionCommandMongo(mongoConnection);
        long headRevision = commandExecutor.execute(command);
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
        MergeCommandMongo command = new MergeCommandMongo(mongoConnection,
                branchRevisionId, message);
        return commandExecutor.execute(command);
    }

    @Override
    public boolean nodeExists(String path, String revisionId) throws Exception {
        NodeExistsCommandMongo command = new NodeExistsCommandMongo(mongoConnection, path,
                MongoUtil.toMongoRepresentation(revisionId));
        String branchId = getBranchId(revisionId);
        command.setBranchId(branchId);
        return commandExecutor.execute(command);
    }

    @Override
    public String getJournal(String fromRevisionId, String toRevisionId, String path)
            throws Exception {
        GetJournalCommandMongo command = new GetJournalCommandMongo(mongoConnection,
                fromRevisionId, toRevisionId, path);
        return commandExecutor.execute(command);
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
                              MongoUtil.wrap(getNode("/", commit.getBaseRevId())),
                              MongoUtil.wrap(getNode("/", commit.getRevisionId())),
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
}