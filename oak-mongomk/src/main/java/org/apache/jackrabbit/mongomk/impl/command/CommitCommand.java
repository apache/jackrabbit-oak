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
package org.apache.jackrabbit.mongomk.impl.command;

import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.jackrabbit.mongomk.api.instruction.Instruction;
import org.apache.jackrabbit.mongomk.api.model.Commit;
import org.apache.jackrabbit.mongomk.impl.MongoNodeStore;
import org.apache.jackrabbit.mongomk.impl.action.FetchCommitsAction;
import org.apache.jackrabbit.mongomk.impl.action.FetchHeadRevisionIdAction;
import org.apache.jackrabbit.mongomk.impl.action.FetchNodesAction;
import org.apache.jackrabbit.mongomk.impl.action.ReadAndIncHeadRevisionAction;
import org.apache.jackrabbit.mongomk.impl.action.SaveAndSetHeadRevisionAction;
import org.apache.jackrabbit.mongomk.impl.action.SaveCommitAction;
import org.apache.jackrabbit.mongomk.impl.action.SaveNodesAction;
import org.apache.jackrabbit.mongomk.impl.command.exception.ConflictingCommitException;
import org.apache.jackrabbit.mongomk.impl.instruction.CommitCommandInstructionVisitor;
import org.apache.jackrabbit.mongomk.impl.model.MongoCommit;
import org.apache.jackrabbit.mongomk.impl.model.MongoNode;
import org.apache.jackrabbit.mongomk.impl.model.MongoSync;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.QueryBuilder;
import com.mongodb.WriteConcern;
import com.mongodb.WriteResult;

/**
 * {@code Command} for {@code MongoMicroKernel#commit(String, String, String, String)}
 */
public class CommitCommand extends BaseCommand<Long> {

    private static final Logger logger = LoggerFactory.getLogger(CommitCommand.class);

    private final MongoCommit commit;

    private Set<String> affectedPaths;
    private Map<String, MongoNode> existingNodes;
    private List<MongoCommit> validCommits;
    private MongoSync mongoSync;
    private Set<MongoNode> nodes;
    private Long revisionId;
    private final Long initialBaseRevisionId;
    private Long baseRevisionId;
    private String branchId;

    /**
     * Constructs a new {@code CommitCommandMongo}.
     *
     * @param nodeStore Node store.
     * @param commit {@link Commit}
     */
    public CommitCommand(MongoNodeStore nodeStore, Commit commit) {
        super(nodeStore);
        this.commit = (MongoCommit)commit;
        this.initialBaseRevisionId = commit.getBaseRevisionId();
    }

    @Override
    public Long execute() throws Exception {
        int retries = 0;
        boolean success = false;
        do {
            mongoSync = new ReadAndIncHeadRevisionAction(nodeStore).execute();
            revisionId = mongoSync.getNextRevisionId() - 1;
            if (initialBaseRevisionId != null) {
                baseRevisionId = initialBaseRevisionId;
            } else {
                baseRevisionId = mongoSync.getHeadRevisionId();
            }
            logger.debug("Committing @{} with diff: {}", revisionId, commit.getDiff());
            readValidCommits();
            readBranchIdFromBaseCommit();
            createMongoNodes();
            prepareCommit();
            // If base revision is older than the head revision, need to read
            // and merge nodes at the head revision.
            FetchHeadRevisionIdAction action = new FetchHeadRevisionIdAction(nodeStore, branchId);
            long headRevisionId = action.execute();
            if (baseRevisionId < headRevisionId) {
                readExistingNodes();
                mergeNodes();
            }
            prepareMongoNodes();
            new SaveNodesAction(nodeStore, nodes).execute();
            new SaveCommitAction(nodeStore, commit).execute();
            success = saveAndSetHeadRevision();
            if (!success) {
                retries++;
            }
        } while (!success);

        String msg = "Commit @{}: success";
        if (retries > 0) {
            msg += " with {} retries.";
        }
        logger.debug(msg, revisionId, retries);
        return revisionId;
    }

    private void readValidCommits() {
        validCommits = new FetchCommitsAction(nodeStore, mongoSync.getHeadRevisionId()).execute();
    }

    @Override
    public int getNumOfRetries() {
        return 100;
    }

    @Override
    public boolean needsRetry(Exception e) {
        return e instanceof ConflictingCommitException;
    }

    private void readBranchIdFromBaseCommit() throws Exception {
        String commitBranchId = commit.getBranchId();
        if (commitBranchId != null) {
            // This is a newly created branch, so no need to check the base
            // commit's branch id.
            branchId = commitBranchId;
            return;
        }

        Long baseRevisionId = commit.getBaseRevisionId();
        if (baseRevisionId == null) {
            return;
        }

        for (MongoCommit commit : validCommits) {
            if (baseRevisionId.equals(commit.getRevisionId())) {
                branchId = commit.getBranchId();
            }
        }
    }

    private void createMongoNodes() throws Exception {
        CommitCommandInstructionVisitor visitor = new CommitCommandInstructionVisitor(
                nodeStore, baseRevisionId, validCommits);
        visitor.setBranchId(branchId);

        for (Instruction instruction : commit.getInstructions()) {
            instruction.accept(visitor);
        }

        Map<String, MongoNode> pathNodeMap = visitor.getPathNodeMap();
        affectedPaths = pathNodeMap.keySet();
        nodes = new HashSet<MongoNode>(pathNodeMap.values());
    }

    private void prepareCommit() throws Exception {
        commit.setAffectedPaths(affectedPaths);
        commit.setBaseRevisionId(mongoSync.getHeadRevisionId());
        commit.setRevisionId(revisionId);
        if (commit.getBranchId() == null && branchId != null) {
            commit.setBranchId(branchId);
        }
        commit.removeField("_id"); // In case this is a retry.
    }

    private void readExistingNodes() {
        FetchNodesAction action = new FetchNodesAction(nodeStore, affectedPaths,
                mongoSync.getHeadRevisionId());
        action.setBranchId(branchId);
        action.setValidCommits(validCommits);
        existingNodes = action.execute();
    }

    private void mergeNodes() {
        for (MongoNode existingNode : existingNodes.values()) {
            for (MongoNode committingNode : nodes) {
                if (existingNode.getPath().equals(committingNode.getPath())) {
                    if(logger.isDebugEnabled()){
                        logger.debug("Found existing node to merge: {}", existingNode.getPath());
                        logger.debug("Existing node: {}", existingNode);
                        logger.debug("Committing node: {}", committingNode);
                    }
                    Map<String, Object> existingProperties = existingNode.getProperties();
                    if (!existingProperties.isEmpty()) {
                        committingNode.setProperties(existingProperties);

                        logger.debug("Merged properties for {}: {}", existingNode.getPath(),
                                existingProperties);
                    }

                    List<String> existingChildren = existingNode.getChildren();
                    if (existingChildren != null) {
                        committingNode.setChildren(existingChildren);

                        logger.debug("Merged children for {}: {}", existingNode.getPath(), existingChildren);
                    }

                    logger.debug("Merged node for {}: {}", existingNode.getPath(), committingNode);

                    break;
                }
            }
        }
    }

    private void prepareMongoNodes() {
        for (MongoNode committingNode : nodes) {
            logger.debug("Preparing children (added and removed) of {}", committingNode.getPath());
            logger.debug("Committing node: {}", committingNode);

            List<String> children = committingNode.getChildren();
            if (children == null) {
                children = new LinkedList<String>();
            }

            List<String> addedChildren = committingNode.getAddedChildren();
            if (addedChildren != null) {
                children.addAll(addedChildren);
            }

            List<String> removedChildren = committingNode.getRemovedChildren();
            if (removedChildren != null) {
                children.removeAll(removedChildren);
            }

            if (!children.isEmpty()) {
                Set<String> temp = new HashSet<String>(children); // remove all duplicates
                committingNode.setChildren(new LinkedList<String>(temp));
            } else {
                committingNode.setChildren(null);
            }

            Map<String, Object> properties = committingNode.getProperties();

            Map<String, Object> addedProperties = committingNode.getAddedProps();
            if (addedProperties != null) {
                properties.putAll(addedProperties);
            }

            Map<String, Object> removedProperties = committingNode.getRemovedProps();
            if (removedProperties != null) {
                for (Map.Entry<String, Object> entry : removedProperties.entrySet()) {
                    properties.remove(entry.getKey());
                }
            }

            if (!properties.isEmpty()) {
                committingNode.setProperties(properties);
            } else {
                committingNode.setProperties(null);
            }

            committingNode.setRevisionId(revisionId);
            if (branchId != null) {
                committingNode.setBranchId(branchId);
            }

            logger.debug("Prepared committing node: {}", committingNode);
        }
    }

    /**
     * Protected for testing purposed only.
     *
     * @return True if the operation was successful.
     * @throws Exception If an exception happens.
     */
    protected boolean saveAndSetHeadRevision() throws Exception {
        long assumedHeadRevision = this.mongoSync.getHeadRevisionId();
        MongoSync mongoSync = new SaveAndSetHeadRevisionAction(nodeStore,
                assumedHeadRevision, revisionId).execute();
        if (mongoSync == null) {
            // There have been commit(s) in the meantime. If it's a conflicting
            // update, retry the whole operation and count against number of retries.
            // If not, need to retry again (in order to write commits and nodes properly)
            // but don't count these retries against number of retries.
            if (conflictingCommitsExist(assumedHeadRevision)) {
                String message = String.format("Commit @%s: failed due to a conflicting commit."
                        + " Affected paths: %s", revisionId, commit.getAffectedPaths());
                logger.warn(message);
                markAsFailed();
                throw new ConflictingCommitException(message);
            } else {
                logger.info("Commit @{}: failed due to a concurrent commit."
                        + " Affected paths: {}", revisionId, commit.getAffectedPaths());
                markAsFailed();
                return false;
            }
        }
        return true;
    }

    private boolean conflictingCommitsExist(long baseRevisionId) {
        QueryBuilder queryBuilder = QueryBuilder.start(MongoCommit.KEY_FAILED).notEquals(Boolean.TRUE)
                .and(MongoCommit.KEY_BASE_REVISION_ID).is(baseRevisionId)
                .and(MongoCommit.KEY_REVISION_ID).greaterThan(0L)
                .and(MongoCommit.KEY_REVISION_ID).notEquals(revisionId);
        DBObject query = queryBuilder.get();
        DBCollection collection = nodeStore.getCommitCollection();
        MongoCommit conflictingCommit = (MongoCommit)collection.findOne(query);
        for (String affectedPath : conflictingCommit.getAffectedPaths()) {
            if (affectedPaths.contains(affectedPath)) {
                return true;
            }
        }
        return false;
    }

    private void markAsFailed() throws Exception {
        DBCollection commitCollection = nodeStore.getCommitCollection();
        DBObject query = QueryBuilder.start("_id").is(commit.getObjectId("_id")).get();
        DBObject update = new BasicDBObject("$set", new BasicDBObject(MongoCommit.KEY_FAILED, Boolean.TRUE));
        WriteResult writeResult = commitCollection.update(query, update,
                false /*upsert*/, false /*multi*/, WriteConcern.SAFE);
        nodeStore.evict(commit);
        if (writeResult.getError() != null) {
            // FIXME This is potentially a bug that we need to handle.
            throw new Exception(String.format("Update wasn't successful: %s", writeResult));
        }
    }
}