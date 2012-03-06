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

import java.io.File;
import java.util.Iterator;

import org.apache.jackrabbit.mk.MicroKernelImpl;
import org.apache.jackrabbit.mk.Repository;
import org.apache.jackrabbit.mk.fs.FileUtils;
import org.apache.jackrabbit.mk.json.fast.Jsop;
import org.apache.jackrabbit.mk.json.fast.JsopArray;
import org.apache.jackrabbit.mk.model.ChildNode;
import org.apache.jackrabbit.mk.model.MutableCommit;
import org.apache.jackrabbit.mk.model.MutableNode;
import org.apache.jackrabbit.mk.model.Node;
import org.apache.jackrabbit.mk.model.StoredCommit;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Use-case: start off a new revision store that contains just the head revision
 * and its nodes.
 * 
 * TODO: if copying starts at some point in time and ends some time later, copy
 *       all revisions that are accessed in the meantime to the new store.
 *       This must be done in a way that ensures the integrity of the parental
 *       relationship (because there may be missing intermediate commits).
 */
public class CopyHeadRevisionTest {

    @Before
    public void setup() throws Exception {
        FileUtils.deleteRecursive("target/mk1", false);
        FileUtils.deleteRecursive("target/mk2", false);
    }

    @After
    public void tearDown() throws Exception {
    }
    
    @Test
    public void testCopyHeadRevisionToNewStore() throws Exception {
        DefaultRevisionStore rsFrom = new DefaultRevisionStore();
        rsFrom.initialize(new File("target/mk1"));
                
        MicroKernelImpl mkFrom = new MicroKernelImpl(new Repository(rsFrom));
        mkFrom.commit("/",  "+\"a\" : { \"c\":{}, \"d\":{} }", mkFrom.getHeadRevision(), null);
        mkFrom.commit("/",  "+\"b\" : {}", mkFrom.getHeadRevision(), null);
        mkFrom.commit("/b", "+\"e\" : {}", mkFrom.getHeadRevision(), null);

        DefaultRevisionStore rsTo = new DefaultRevisionStore(); 
        rsTo.initialize(new File("target/mk2"));

        copyHeadRevision(rsFrom, rsTo);

        MicroKernelImpl mkTo = new MicroKernelImpl(new Repository(rsTo));

        // Assert both old and new MK have same head revision
        Assert.assertEquals(mkFrom.getHeadRevision(), mkTo.getHeadRevision());
        
        // Assert both old and new MK have same contents
        Assert.assertEquals(
                mkFrom.getNodes("/", mkFrom.getHeadRevision(), 2, 0, -1),
                mkTo.getNodes("/", mkTo.getHeadRevision(), 2, 0, -1));

        // Assert new MK has only 2 revisions (initial and head)
        JsopArray revs = (JsopArray) Jsop.parse(mkTo.getRevisions(0, Integer.MAX_VALUE));
        Assert.assertEquals(2, revs.size());
    }
    
    /**
     * Copy the head revision (commit and nodes) from a source provider to a
     * target store.
     * 
     * @param from source provider
     * @param to target store
     * @throws Exception if an error occurs
     */
    private void copyHeadRevision(RevisionProvider from, RevisionStore to)
            throws Exception {
        
        StoredCommit commitFrom = from.getHeadCommit();
        
        Node nodeFrom = from.getNode(commitFrom.getRootNodeId());
        copy(nodeFrom, to);
        
        MutableCommit commitTo = new MutableCommit(commitFrom);
        commitTo.setParentId(to.getHeadCommitId());
        
        String revId = to.putCommit(commitTo);
        to.setHeadCommitId(revId);
    }
    
    /**
     * Copy a node and all its descendants into a target store
     * @param node source node
     * @param store target store
     * @throws Exception if an error occurs
     */
    private void copy(Node node, RevisionStore store) 
            throws Exception {

        store.putNode(new MutableNode(node, store));
        
        Iterator<ChildNode> iter = node.getChildNodes(0, -1);
        while (iter.hasNext()) {
            ChildNode c = iter.next();
            copy(c.getNode(), store);
        }
    }
}
