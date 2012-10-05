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

import org.apache.jackrabbit.mk.json.JsopBuilder;
import org.apache.jackrabbit.mk.model.tree.DiffBuilder;
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
import org.apache.jackrabbit.mongomk.impl.model.tree.NodeStateMongo;
import org.apache.jackrabbit.mongomk.impl.model.tree.NodeStoreMongod;
import org.apache.jackrabbit.mongomk.model.CommitMongo;
import org.apache.jackrabbit.mongomk.query.FetchCommitQuery;
import org.apache.jackrabbit.mongomk.query.FetchHeadRevisionIdQuery;
import org.apache.jackrabbit.mongomk.query.FetchValidCommitsQuery;
import org.apache.jackrabbit.mongomk.util.MongoUtil;

/**
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
            if (toCommit.getBaseRevisionId() == fromRevisionId) {
                // Specified range spans a single commit:
                // use diff stored in commit instead of building it dynamically
                return toCommit.getDiff();
            }
        }

        // FIXME - FetchNodesByPath?
        GetNodesCommandMongo command = new GetNodesCommandMongo(mongoConnection, path, fromRevisionId, -1);
        Node before = command.execute();

        command = new GetNodesCommandMongo(mongoConnection, path, toRevisionId, -1);
        Node after = command.execute();

        return new DiffBuilder(new NodeStateMongo(before), new NodeStateMongo(after),
                path, depth, new NodeStoreMongod(), path).build();
    }

    @Override
    public String getHeadRevision() throws Exception {
        Long headRevision = commandExecutor.execute(new GetHeadRevisionCommandMongo(mongoConnection));
        return MongoUtil.fromMongoRepresentation(headRevision);
    }

    @Override
    public Node getNodes(String path, String revisionId, int depth, long offset,
            int maxChildNodes, String filter) throws Exception {
        Command<Node> command = new GetNodesCommandMongo(mongoConnection, path,
                MongoUtil.toMongoRepresentation(revisionId), depth);
        return commandExecutor.execute(command);
    }

    @Override
    public boolean nodeExists(String path, String revisionId) throws Exception {
        Command<Boolean> command = new NodeExistsCommandMongo(mongoConnection, path,
                MongoUtil.toMongoRepresentation(revisionId));
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

        List<CommitMongo> commits = new FetchValidCommitsQuery(mongoConnection,
                fromRevision, toRevision, 0).execute();

        CommitMongo toCommit = getCommit(commits, toRevision);

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

      List<CommitMongo> history = new FetchValidCommitsQuery(mongoConnection, 0L,
              Long.MAX_VALUE, maxEntries).execute();
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
}