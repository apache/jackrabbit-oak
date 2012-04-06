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
package org.apache.jackrabbit.mk.store;

import java.util.Iterator;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.jackrabbit.mk.model.ChildNode;
import org.apache.jackrabbit.mk.model.ChildNodeEntriesMap;
import org.apache.jackrabbit.mk.model.Id;
import org.apache.jackrabbit.mk.model.MutableCommit;
import org.apache.jackrabbit.mk.model.MutableNode;
import org.apache.jackrabbit.mk.model.StoredCommit;
import org.apache.jackrabbit.mk.model.StoredNode;

/**
 * Revision garbage collector that copies reachable revisions from a "from" revision
 * store to a "to" revision store. It assumes that both stores share the same blob
 * store.
 * 
 * In the current design, the head revision and all the nodes it references are
 * reachable.
 */
public class CopyingGC extends AbstractRevisionStore {

    /**
     * From store.
     */
    private RevisionStore rsFrom;
    
    /**
     * To store.
     */
    private RevisionStore rsTo;
    
    /**
     * GC run state constants.
     */
    private static final int STOPPED = 0;
    private static final int STARTING = 1;
    private static final int STARTED = 2;

    /**
     * GC run state.
     */
    private final AtomicInteger runState = new AtomicInteger();

    /**
     * Create a new instance of this class.
     * 
     * @param rsFrom from store
     * @param rsTo to store 
     */
    public CopyingGC(RevisionStore rsFrom, RevisionStore rsTo) {
        this.rsFrom = rsFrom;
        this.rsTo = rsTo;
    }
    
    public void gc() {
        if (!runState.compareAndSet(STOPPED, STARTING)) {
            /* already running */
            return;
        }
        
        try {
            /* copy head commit */
            MutableCommit commitTo = new MutableCommit(rsFrom.getHeadCommit());
            commitTo.setParentId(rsTo.getHeadCommitId());

            rsTo.lockHead();
            
            try {
                rsTo.putHeadCommit(commitTo);
            } finally {
                rsTo.unlockHead();
            }
            
            /* now start putting all further changes to the "to" store */
            runState.set(STARTED);

            /* copy node hierarchy */
            copy(rsFrom.getNode(commitTo.getRootNodeId()));
            
        } catch (Exception e) {
            /* unable to perform GC */
            e.printStackTrace();
            runState.set(STOPPED);
            return;
        }
        
        /* switch from and to space */
        rsFrom = rsTo;
        
        runState.set(STOPPED);

        rsTo = null;
    }
    
    /**
     * Copy a node and all its descendants into a target store
     * @param node source node
     * @throws Exception if an error occurs
     */
    private void copy(StoredNode node) throws Exception {
        try {
            rsTo.getNode(node.getId());
            return;
        } catch (NotFoundException e) {
            // ignore, better add a has() method
        }
        rsTo.putNode(new MutableNode(node, rsTo, null));

        Iterator<ChildNode> iter = node.getChildNodeEntries(0, -1);
        while (iter.hasNext()) {
            ChildNode c = iter.next();
            copy(rsFrom.getNode(c.getId()));
        }
    }
    
    // ---------------------------------------------------------- RevisionStore

    public StoredNode getNode(Id id) throws NotFoundException, Exception {
        if (runState.get() == STARTED) {
            try {
                return rsTo.getNode(id);
            } catch (NotFoundException e) {
                /* ignore */
            }
        }
        try {
            return rsFrom.getNode(id);
        } catch (NotFoundException e) {
//            System.out.println(rsFrom + " --> " + id + " failed!");
            throw e;
        }
    }

    public StoredCommit getCommit(Id id) throws NotFoundException,
            Exception {
        
        return rsFrom.getCommit(id);
    }

    public ChildNodeEntriesMap getCNEMap(Id id) throws NotFoundException,
            Exception {
        
        return rsFrom.getCNEMap(id);
    }

    public StoredNode getRootNode(Id commitId) throws NotFoundException,
            Exception {

        return rsFrom.getRootNode(commitId);
    }

    public StoredCommit getHeadCommit() throws Exception {
        return runState.get() == STARTED ? rsTo.getHeadCommit() : rsFrom.getHeadCommit(); 
    }

    public Id getHeadCommitId() throws Exception {
        return runState.get() == STARTED ? rsTo.getHeadCommitId() : rsFrom.getHeadCommitId();
    }

    public Id putNode(MutableNode node) throws Exception {
        if (runState.get() == STARTED) {
            Id id = rsTo.putNode(node);
//            System.out.println(rsTo + " <-- " + node.toString() + "(" + id + ")");
            return id;
        } else {
            Id id = rsFrom.putNode(node);
//            System.out.println(rsFrom + " <-- " + node.toString() + "(" + id + ")");
            return id;
        }
    }

    public Id putCNEMap(ChildNodeEntriesMap map) throws Exception {
        return runState.get() == STARTED ? rsTo.putCNEMap(map) : rsFrom.putCNEMap(map);
    }

    // TODO: potentially dangerous, if lock & unlock interfere with GC start
    public void lockHead() {
        if (runState.get() == STARTED) {
            rsTo.lockHead();
        } else {
            rsFrom.lockHead();
        }
    }

    public Id putHeadCommit(MutableCommit commit) throws Exception {
        return runState.get() == STARTED ? rsTo.putHeadCommit(commit) : rsFrom.putHeadCommit(commit);
    }

    // TODO: potentially dangerous, if lock & unlock interfere with GC start
    public void unlockHead() {
        if (runState.get() == STARTED) {
            rsTo.unlockHead();
        } else {
            rsFrom.unlockHead();
        }
    }
}
