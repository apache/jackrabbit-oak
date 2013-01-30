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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.jackrabbit.mongomk.api.instruction.Instruction;
import org.apache.jackrabbit.mongomk.api.model.Commit;
import org.apache.jackrabbit.mongomk.impl.MongoNodeStore;
import org.apache.jackrabbit.mongomk.impl.action.FetchCommitAction;
import org.apache.jackrabbit.mongomk.impl.action.FetchHeadRevisionIdAction;
import org.apache.jackrabbit.mongomk.impl.action.FetchNodesAction;
import org.apache.jackrabbit.mongomk.impl.action.ReadAndIncHeadRevisionAction;
import org.apache.jackrabbit.mongomk.impl.action.SaveAndSetHeadRevisionAction;
import org.apache.jackrabbit.mongomk.impl.action.SaveCommitAction;
import org.apache.jackrabbit.mongomk.impl.action.SaveNodesAction;
import org.apache.jackrabbit.mongomk.impl.command.exception.ConcurrentCommitException;
import org.apache.jackrabbit.mongomk.impl.command.exception.ConflictingCommitException;
import org.apache.jackrabbit.mongomk.impl.instruction.CommitCommandInstructionVisitor;
import org.apache.jackrabbit.mongomk.impl.model.MongoCommit;
import org.apache.jackrabbit.mongomk.impl.model.MongoNode;
import org.apache.jackrabbit.mongomk.impl.model.MongoSync;
import org.bson.types.ObjectId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
    private MongoSync mongoSync;
    private Map<String, MongoNode> nodes;
    private Long revisionId;
    private final Long initialBaseRevisionId;
    private Long baseRevisionId;
    private String branchId;
    private int retries;

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
        mongoSync = new ReadAndIncHeadRevisionAction(nodeStore).execute();
        revisionId = mongoSync.getNextRevisionId() - 1;
        if (initialBaseRevisionId != null) {
            baseRevisionId = initialBaseRevisionId;
        } else {
            baseRevisionId = mongoSync.getHeadRevisionId();
        }
        logger.debug("Committing @{} with diff: {}", revisionId, commit.getDiff());
        readBranchIdFromBaseCommit();
        createMongoNodes();
        prepareCommit();
        readAndMergeExistingNodes();
        prepareMongoNodes();
        try {
            saveNodesAndCommits();
            saveAndSetHeadRevision();
            cacheNodes();
        } catch (ConcurrentCommitException e) {
            retries++;
            logger.debug("Commit @{}: failure. Retries:" + retries);
            throw e;
        }

        String msg = "Commit @{}: success";
        if (retries > 0) {
            msg += " with {} retries.";
        }
        logger.debug(msg, revisionId, retries);
        return revisionId;
    }

    @Override
    public int getNumOfRetries() {
        return 100;
    }

    @Override
    public boolean needsRetry(Exception e) {
        return e instanceof ConcurrentCommitException;
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

        FetchCommitAction action = new FetchCommitAction(nodeStore, baseRevisionId);
        MongoCommit commit = action.execute();
        branchId = commit.getBranchId();
    }

    private void createMongoNodes() throws Exception {
        CommitCommandInstructionVisitor visitor = new CommitCommandInstructionVisitor(
                nodeStore, baseRevisionId, null);
        visitor.setBranchId(branchId);

        for (Instruction instruction : commit.getInstructions()) {
            instruction.accept(visitor);
        }

        nodes = visitor.getPathNodeMap();
        affectedPaths = nodes.keySet();
    }

    private void prepareCommit() throws Exception {
        commit.setAffectedPaths(affectedPaths);
        commit.setBaseRevisionId(branchId == null?
                mongoSync.getHeadRevisionId() : baseRevisionId);
        commit.setRevisionId(revisionId);
        if (commit.getBranchId() == null && branchId != null) {
            commit.setBranchId(branchId);
        }
        commit.removeField("_id"); // In case this is a retry.
    }

    private void readAndMergeExistingNodes() throws Exception {
        // If base revision is older than the head revision, need to read
        // and merge nodes at the head revision.
        long headRevisionId;
        if (branchId == null) {
            headRevisionId = mongoSync.getHeadRevisionId();
        } else {
            headRevisionId = new FetchHeadRevisionIdAction(nodeStore, branchId).execute();
        }

        if (baseRevisionId < headRevisionId) {
            readExistingNodes();
            mergeNodes();
        }
    }

    private void readExistingNodes() throws Exception {
        if (affectedPaths == null || affectedPaths.isEmpty()) {
            existingNodes = Collections.emptyMap();
            return;
        }

        long revisionId = branchId == null? mongoSync.getHeadRevisionId() : baseRevisionId;
        FetchNodesAction action = new FetchNodesAction(nodeStore, affectedPaths, revisionId);
        action.setBranchId(branchId);
        existingNodes = action.execute();
    }

    private void mergeNodes() throws ConflictingCommitException {
        for (MongoNode committingNode : nodes.values()) {
            MongoNode existingNode = existingNodes.get(committingNode.getPath());
            if (existingNode == null) {
                continue; // Nothing to merge.
            }

            if (logger.isDebugEnabled()){
                logger.debug("Found existing node to merge: {}", existingNode.getPath());
                logger.debug("Existing node: {}", existingNode);
                logger.debug("Committing node: {}", committingNode);
            }

            // FIXME - More sophisticated conflict handing is needed.
            if (existingNode.isDeleted() && !committingNode.isDeleted()) {
                // A node we modify has been deleted in the meantime.
                throw new ConflictingCommitException("Node has been deleted in the meantime: "
                        + existingNode.getPath());
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
        }
    }

    private void prepareMongoNodes() {
        for (MongoNode committingNode : nodes.values()) {
            logger.debug("Preparing children (added and removed) of {}", committingNode.getPath());
            logger.debug("Committing node: {}", committingNode);

            List<String> children = committingNode.getChildren();
            if (children == null) {
                children = new LinkedList<String>();
            } else {
                children = new ArrayList<String>(children);
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

    private void saveNodesAndCommits() throws Exception {
        long headRevisionId = new FetchHeadRevisionIdAction(nodeStore, branchId).execute();
        if (branchId == null && headRevisionId != mongoSync.getHeadRevisionId()) {
            // Head revision moved on in trunk in the meantime, no need to save
            throw new ConcurrentCommitException();
        }
        new SaveNodesAction(nodeStore, nodes.values()).execute();
        new SaveCommitAction(nodeStore, commit).execute();
    }

    /**
     * Protected for testing purposed only.
     *
     * @return True if the operation was successful.
     * @throws Exception If an exception happens.
     */
    protected void saveAndSetHeadRevision() throws Exception {
        // Don't update the head revision id for branches.
        if (branchId != null) {
            return;
        }

        long assumedHeadRevision = mongoSync.getHeadRevisionId();
        MongoSync mongoSync = new SaveAndSetHeadRevisionAction(nodeStore,
                assumedHeadRevision, revisionId).execute();
        if (mongoSync == null) { // There have been concurrent commit(s).
            cleanupCommitAndNodes();
            throw new ConcurrentCommitException();
        }
    }

    private void cleanupCommitAndNodes() throws Exception {

        logger.debug("Cleaning up commit and nodes related to Commit @{}", revisionId);

        // Clean up the commit.
        DBCollection collection = nodeStore.getCommitCollection();
        DBObject query = QueryBuilder.start("_id").is(commit.getObjectId("_id")).get();

        WriteResult writeResult = collection.remove(query, WriteConcern.SAFE);
        if (writeResult.getError() != null) {
            throw new Exception(String.format("Remove wasn't successful: %s", writeResult));
        }

        // Collect ids for the nodes
        Collection<MongoNode> nodesCollection = nodes.values();
        int nodesSize = nodesCollection.size();
        DBObject[] nodesArray = nodesCollection.toArray(new DBObject[nodesSize]);
        ObjectId[] objectIds = new ObjectId[nodesSize];
        for (int i = 0; i < nodesSize; i++) {
            objectIds[i] = (ObjectId)nodesArray[i].get("_id");
        }

        // Clean up nodes.
        collection = nodeStore.getNodeCollection();
        query = QueryBuilder.start("_id").in(objectIds).get();

        writeResult = collection.remove(query, WriteConcern.SAFE);
        if (writeResult.getError() != null) {
            throw new Exception(String.format("Remove wasn't successful: %s", writeResult));
        }
    }

    private void cacheNodes() {
        for (MongoNode node : nodes.values()) {
            nodeStore.cache(node);
        }
    }
}