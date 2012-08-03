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
package org.apache.jackrabbit.mk.persistence;

import org.apache.jackrabbit.mk.model.ChildNodeEntries;
import org.apache.jackrabbit.mk.model.ChildNodeEntriesMap;
import org.apache.jackrabbit.mk.model.Commit;
import org.apache.jackrabbit.mk.model.Id;
import org.apache.jackrabbit.mk.model.Node;
import org.apache.jackrabbit.mk.model.StoredCommit;
import org.apache.jackrabbit.mk.model.StoredNode;
import org.apache.jackrabbit.mk.store.NotFoundException;

import java.io.Closeable;
import java.io.File;

/**
 * Defines the methods exposed by a persistence manager, that stores head
 * revision id, nodes, child node entries and blobs.
 */
public interface Persistence extends Closeable {

    public void initialize(File homeDir) throws Exception;
    
    Id readHead() throws Exception;

    void writeHead(Id id) throws Exception;

    /**
     * Read a node from storage.
     * 
     * @param node node to read, with id given in {@link StoredNode#getId()}
     * @throws NotFoundException if no such node is found
     * @throws Exception if some other error occurs
     */
    void readNode(StoredNode node) throws NotFoundException, Exception;

    Id writeNode(Node node) throws Exception;

    ChildNodeEntriesMap readCNEMap(Id id) throws NotFoundException, Exception;

    Id writeCNEMap(ChildNodeEntries map) throws Exception;

    StoredCommit readCommit(Id id) throws NotFoundException, Exception;

    /**
     * Persist a commit with an id provided by the caller.
     * 
     * @param id commit id
     * @param commit commit
     * @throws Exception if an error occurs
     */
    void writeCommit(Id id, Commit commit) throws Exception;
}
