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
package org.apache.jackrabbit.mongomk.command;

import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.jackrabbit.mongomk.MongoConnection;
import org.apache.jackrabbit.mongomk.api.command.AbstractCommand;
import org.apache.jackrabbit.mongomk.api.model.Commit;
import org.apache.jackrabbit.mongomk.api.model.Instruction;
import org.apache.jackrabbit.mongomk.model.CommitCommandInstructionVisitor;
import org.apache.jackrabbit.mongomk.model.CommitMongo;
import org.apache.jackrabbit.mongomk.model.HeadMongo;
import org.apache.jackrabbit.mongomk.model.NodeMongo;
import org.apache.jackrabbit.mongomk.query.FetchNodesForRevisionQuery;
import org.apache.jackrabbit.mongomk.query.ReadAndIncHeadRevisionQuery;
import org.apache.jackrabbit.mongomk.query.SaveAndSetHeadRevisionQuery;
import org.apache.jackrabbit.mongomk.query.SaveCommitQuery;
import org.apache.jackrabbit.mongomk.query.SaveNodesQuery;
import org.apache.log4j.Logger;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.QueryBuilder;
import com.mongodb.WriteResult;

/**
 * A {@code Command} for committing into {@code MongoDB}.
 *
 * @author <a href="mailto:pmarx@adobe.com>Philipp Marx</a>
 */
public class CommitCommandMongo extends AbstractCommand<String> {

    private static final Logger logger = Logger.getLogger(CommitCommandMongo.class);

    private final Commit commit;
    private final MongoConnection mongoConnection;

    private Set<String> affectedPaths;
    private CommitMongo commitMongo;
    private List<NodeMongo> existingNodes;
    private HeadMongo headMongo;
    private Set<NodeMongo> nodeMongos;
    private String revisionId;

    /**
     * Constructs a new {@code CommitCommandMongo}.
     *
     * @param mongoConnection {@link MongoConnection}
     * @param commit {@link Commit}
     */
    public CommitCommandMongo(MongoConnection mongoConnection, Commit commit) {
        this.mongoConnection = mongoConnection;
        this.commit = commit;
    }

    @Override
    public String execute() throws Exception {
        logger.debug(String.format("Trying to commit: %s", commit.getDiff()));

        readAndIncHeadRevision();
        createRevision();
        createMongoNodes();
        createMongoCommit();
        readExistingNodes();
        mergeNodes();
        prepareMongoNodes();
        saveNodes();
        saveCommit();
        boolean success = saveAndSetHeadRevision();

        logger.debug(String.format("Success was: %b", success));

        if (!success) {
            markAsFailed();

            throw new ConflictingCommitException();
        }

        addRevisionId();

        return revisionId;
    }

    @Override
    public int getNumOfRetries() {
        return 10;
    }

    @Override
    public boolean needsRetry(Exception e) {
        return e instanceof ConflictingCommitException;
    }

    /**
     * This is protected for testing purposed only.
     *
     * @return N/A
     * @throws Exception
     */
    protected boolean saveAndSetHeadRevision() throws Exception {
        boolean success = true;

        HeadMongo headMongo = new SaveAndSetHeadRevisionQuery(mongoConnection, this.headMongo.getHeadRevisionId(),
                HeadMongo.toDBRepresentation(revisionId)).execute();
        if (headMongo == null) {
            // TODO: Check for conflicts!
            logger.warn(String
                    .format("Encounterd a conflicting update, thus can't commit revision %s and will be retried with new revision",
                            revisionId));

            success = false;
        }

        return success;
    }

    private void addRevisionId() {
        commit.setRevisionId(revisionId);
    }

    private void createMongoCommit() throws Exception {
        commitMongo = CommitMongo.fromCommit(commit);
        commitMongo.setRevisionId(revisionId);
        commitMongo.setAffectedPaths(new LinkedList<String>(affectedPaths));
        commitMongo.setBaseRevId(headMongo.getHeadRevisionId());
    }

    private void createMongoNodes() throws Exception {
        CommitCommandInstructionVisitor visitor = new CommitCommandInstructionVisitor(mongoConnection,
                headMongo.getHeadRevisionId());
        for (Instruction instruction : commit.getInstructions()) {
            instruction.accept(visitor);
        }

        Map<String, NodeMongo> pathNodeMap = visitor.getPathNodeMap();

        affectedPaths = pathNodeMap.keySet(); // TODO Original copies and moved nodes must be included!
        nodeMongos = new HashSet<NodeMongo>(pathNodeMap.values());
        for (NodeMongo nodeMongo : nodeMongos) {
            nodeMongo.setRevisionId(revisionId);
        }
    }

    private void createRevision() {
        revisionId = String.valueOf(headMongo.getNextRevisionId() - 1);
    }

    private void markAsFailed() throws Exception {
        DBCollection commitCollection = mongoConnection.getCommitCollection();
        DBObject query = QueryBuilder.start("_id").is(commitMongo.getObjectId("_id")).get();
        DBObject update = new BasicDBObject("$set", new BasicDBObject(CommitMongo.KEY_FAILED, Boolean.TRUE));
        WriteResult writeResult = commitCollection.update(query, update);
        if (writeResult.getError() != null) {
            throw new Exception(String.format("Update wasn't successful: %s", writeResult)); // TODO now what?
        }
    }

    private void mergeNodes() {
        for (NodeMongo existingNode : existingNodes) {
            for (NodeMongo committingNode : nodeMongos) {
                if (existingNode.getPath().equals(committingNode.getPath())) {
                    logger.debug(String.format("Found existing node to merge: %s", existingNode.getPath()));
                    logger.debug(String.format("Existing node: %s", existingNode));
                    logger.debug(String.format("Committing node: %s", committingNode));

                    Map<String, Object> existingProperties = existingNode.getProperties();

                    if (existingProperties != null) {
                        committingNode.setProperties(existingProperties);

                        logger.debug(String.format("Merged properties for %s: %s", existingNode.getPath(),
                                existingProperties));
                    }

                    List<String> existingChildren = existingNode.getChildren();

                    if (existingChildren != null) {
                        committingNode.setChildren(existingChildren);

                        logger.debug(String.format("Merged children for %s: %s", existingNode.getPath(), existingChildren));
                    }

                    committingNode.setBaseRevisionId(existingNode.getRevisionId());

                    logger.debug(String.format("Merged node for %s: %s", existingNode.getPath(), committingNode));

                    break;
                }
            }
        }
    }

    private void prepareMongoNodes() {
        for (NodeMongo committingNode : nodeMongos) {
            logger.debug(String.format("Preparing children (added and removed) of %s", committingNode.getPath()));
            logger.debug(String.format("Committing node: %s", committingNode));

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
            if (properties == null) {
                properties = new HashMap<String, Object>();
            }

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

            logger.debug(String.format("Prepared committing node: %s", committingNode));
        }
    }

    private void readAndIncHeadRevision() throws Exception {
        headMongo = new ReadAndIncHeadRevisionQuery(mongoConnection).execute();
    }

    private void readExistingNodes() {
        Set<String> paths = new HashSet<String>();
        for (NodeMongo nodeMongo : nodeMongos) {
            paths.add(nodeMongo.getPath());
        }

        existingNodes = new FetchNodesForRevisionQuery(mongoConnection, paths,
                String.valueOf(headMongo.getHeadRevisionId())).execute();
    }

    private void saveCommit() throws Exception {
        new SaveCommitQuery(mongoConnection, commitMongo).execute();
    }

    private void saveNodes() throws Exception {
        new SaveNodesQuery(mongoConnection, nodeMongos).execute();
    }
}
