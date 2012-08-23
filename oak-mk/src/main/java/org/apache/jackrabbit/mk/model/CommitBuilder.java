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
package org.apache.jackrabbit.mk.model;

import org.apache.jackrabbit.mk.json.JsonObject;
import org.apache.jackrabbit.mk.json.JsopBuilder;
import org.apache.jackrabbit.mk.model.tree.DiffBuilder;
import org.apache.jackrabbit.mk.store.NotFoundException;
import org.apache.jackrabbit.mk.store.RevisionStore;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 *
 */
public class CommitBuilder {

    private static final Logger LOG = LoggerFactory.getLogger(CommitBuilder.class);

    /** revision changes are based upon */
    private Id baseRevId;

    private final String msg;

    private final RevisionStore store;

    // staging area
    private final StagedNodeTree stagedTree;

    // change log
    private final List<Change> changeLog = new ArrayList<Change>();

    public CommitBuilder(Id baseRevId, String msg, RevisionStore store) throws Exception {
        this.baseRevId = baseRevId;
        this.msg = msg;
        this.store = store;
        stagedTree = new StagedNodeTree(store, baseRevId);
    }

    public void addNode(String parentNodePath, String nodeName, JsonObject node) throws Exception {
        Change change = new AddNode(parentNodePath, nodeName, node);
        change.apply();
        // update change log
        changeLog.add(change);
    }

    public void removeNode(String nodePath) throws NotFoundException, Exception {
        Change change = new RemoveNode(nodePath);
        change.apply();
        // update change log
        changeLog.add(change);
    }

    public void moveNode(String srcPath, String destPath) throws NotFoundException, Exception {
        Change change = new MoveNode(srcPath, destPath);
        change.apply();
        // update change log
        changeLog.add(change);
    }

    public void copyNode(String srcPath, String destPath) throws NotFoundException, Exception {
        Change change = new CopyNode(srcPath, destPath);
        change.apply();
        // update change log
        changeLog.add(change);
    }

    public void setProperty(String nodePath, String propName, String propValue) throws Exception {
        Change change = new SetProperty(nodePath, propName, propValue);
        change.apply();
        // update change log
        changeLog.add(change);
    }

    public Id /* new revId */ doCommit() throws Exception {
        return doCommit(false);
    }

    public Id /* new revId */ doCommit(boolean createBranch) throws Exception {
        if (stagedTree.isEmpty() && !createBranch) {
            // nothing to commit
            return baseRevId;
        }

        StoredCommit baseCommit = store.getCommit(baseRevId);
        if (createBranch && baseCommit.getBranchRootId() != null) {
            throw new Exception("cannot branch off a private branch");
        }

        boolean privateCommit = createBranch || baseCommit.getBranchRootId() != null;

        if (!privateCommit) {
            Id currentHead = store.getHeadCommitId();
            if (!currentHead.equals(baseRevId)) {
                // todo gracefully handle certain conflicts (e.g. changes on moved sub-trees, competing deletes etc)
                // update base revision to more recent current head
                baseRevId = currentHead;
                // reset staging area
                stagedTree.reset(baseRevId);
                // replay change log on new base revision
                for (Change change : changeLog) {
                    change.apply();
                }
            }
        }

        RevisionStore.PutToken token = store.createPutToken();
        Id rootNodeId =
                changeLog.isEmpty() ? baseCommit.getRootNodeId() : stagedTree.persist(token);

        Id newRevId;

        if (!privateCommit) {
            store.lockHead();
            try {
                Id currentHead = store.getHeadCommitId();
                if (!currentHead.equals(baseRevId)) {
                    // there's a more recent head revision
                    // perform a three-way merge
                    rootNodeId = stagedTree.merge(store.getNode(rootNodeId), currentHead, baseRevId, token);
                    // update base revision to more recent current head
                    baseRevId = currentHead;
                }

                if (store.getCommit(currentHead).getRootNodeId().equals(rootNodeId)) {
                    // the commit didn't cause any changes,
                    // no need to create new commit object/update head revision
                    return currentHead;
                }
                // persist new commit
                MutableCommit newCommit = new MutableCommit();
                newCommit.setParentId(baseRevId);
                newCommit.setCommitTS(System.currentTimeMillis());
                newCommit.setMsg(msg);
                StringBuilder diff = new StringBuilder();
                for (Change change : changeLog) {
                    if (diff.length() > 0) {
                        diff.append('\n');
                    }
                    diff.append(change.asDiff());
                }
                newCommit.setChanges(diff.toString());
                newCommit.setRootNodeId(rootNodeId);
                newCommit.setBranchRootId(null);
                newRevId = store.putHeadCommit(token, newCommit, null, null);
            } finally {
                store.unlockHead();
            }
        } else {
            // private commit/branch
            MutableCommit newCommit = new MutableCommit();
            newCommit.setParentId(baseCommit.getId());
            newCommit.setCommitTS(System.currentTimeMillis());
            newCommit.setMsg(msg);
            StringBuilder diff = new StringBuilder();
            for (Change change : changeLog) {
                if (diff.length() > 0) {
                    diff.append('\n');
                }
                diff.append(change.asDiff());
            }
            newCommit.setChanges(diff.toString());
            newCommit.setRootNodeId(rootNodeId);
            if (createBranch) {
                newCommit.setBranchRootId(baseCommit.getId());
            } else {
                newCommit.setBranchRootId(baseCommit.getBranchRootId());
            }
            newRevId = store.putCommit(token, newCommit);
        }

        // reset instance
        stagedTree.reset(newRevId);
        changeLog.clear();

        return newRevId;
    }

    public Id /* new revId */ doMerge() throws Exception {
        StoredCommit branchCommit = store.getCommit(baseRevId);
        Id branchRootId = branchCommit.getBranchRootId();
        if (branchRootId == null) {
            throw new Exception("can only merge a private branch commit");
        }

        RevisionStore.PutToken token = store.createPutToken();
        Id rootNodeId =
                changeLog.isEmpty() ? branchCommit.getRootNodeId() : stagedTree.persist(token);

        Id newRevId;

        store.lockHead();
        try {
            Id currentHead = store.getHeadCommitId();

            StoredNode ourRoot = store.getNode(rootNodeId);

            rootNodeId = stagedTree.merge(ourRoot, currentHead, branchRootId, token);

            if (store.getCommit(currentHead).getRootNodeId().equals(rootNodeId)) {
                // the merge didn't cause any changes,
                // no need to create new commit object/update head revision
                return currentHead;
            }
            MutableCommit newCommit = new MutableCommit();
            newCommit.setParentId(currentHead);
            newCommit.setCommitTS(System.currentTimeMillis());
            newCommit.setMsg(msg);
            // dynamically build diff of merged commit
            String diff = new DiffBuilder(
                    store.getNodeState(store.getRootNode(currentHead)),
                    store.getNodeState(store.getNode(rootNodeId)),
                    "/", -1, store, "").build();
            if (diff.isEmpty()) {
                LOG.debug("merge of empty branch {} with differing content hashes encountered, ignore and keep current head {}",
                        baseRevId, currentHead);
                return currentHead;
            }
            newCommit.setChanges(diff);
            newCommit.setRootNodeId(rootNodeId);
            newCommit.setBranchRootId(null);
            newRevId = store.putHeadCommit(token, newCommit, branchRootId, baseRevId);
        } finally {
            store.unlockHead();
        }

        // reset instance
        stagedTree.reset(newRevId);
        changeLog.clear();

        return newRevId;
    }

    //--------------------------------------------------------< inner classes >

    abstract class Change {
        abstract void apply() throws Exception;
        abstract String asDiff();
    }

    class AddNode extends Change {
        String parentNodePath;
        String nodeName;
        JsonObject node;

        AddNode(String parentNodePath, String nodeName, JsonObject node) {
            this.parentNodePath = parentNodePath;
            this.nodeName = nodeName;
            this.node = node;
        }

        @Override
        void apply() throws Exception {
            stagedTree.add(parentNodePath, nodeName, node);
        }

        @Override
        String asDiff() {
            JsopBuilder diff = new JsopBuilder();
            diff.tag('+').key(PathUtils.concat(parentNodePath, nodeName));
            node.toJson(diff);
            return diff.toString();
        }
    }

    class RemoveNode extends Change {
        String nodePath;

        RemoveNode(String nodePath) {
            this.nodePath = nodePath;
        }

        @Override
        void apply() throws Exception {
            stagedTree.remove(nodePath);
        }

        @Override
        String asDiff() {
            JsopBuilder diff = new JsopBuilder();
            diff.tag('-').value(nodePath);
            return diff.toString();
        }
    }

    class MoveNode extends Change {
        String srcPath;
        String destPath;

        MoveNode(String srcPath, String destPath) {
            this.srcPath = srcPath;
            this.destPath = destPath;
        }

        @Override
        void apply() throws Exception {
            stagedTree.move(srcPath, destPath);
        }

        @Override
        String asDiff() {
            JsopBuilder diff = new JsopBuilder();
            diff.tag('>').key(srcPath).value(destPath);
            return diff.toString();
        }
    }

    class CopyNode extends Change {
        String srcPath;
        String destPath;

        CopyNode(String srcPath, String destPath) {
            this.srcPath = srcPath;
            this.destPath = destPath;
        }

        @Override
        void apply() throws Exception {
            stagedTree.copy(srcPath, destPath);
        }

        @Override
        String asDiff() {
            JsopBuilder diff = new JsopBuilder();
            diff.tag('*').key(srcPath).value(destPath);
            return diff.toString();
        }
    }

    class SetProperty extends Change {
        String nodePath;
        String propName;
        String propValue;

        SetProperty(String nodePath, String propName, String propValue) {
            this.nodePath = nodePath;
            this.propName = propName;
            this.propValue = propValue;
        }

        @Override
        void apply() throws Exception {
            stagedTree.setProperty(nodePath, propName, propValue);
        }

        @Override
        String asDiff() {
            JsopBuilder diff = new JsopBuilder();
            diff.tag('^').key(PathUtils.concat(nodePath, propName));
            if (propValue != null) {
                diff.encodedValue(propValue);
            } else {
                diff.value(null);
            }
            return diff.toString();
        }
    }
}
