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
package org.apache.jackrabbit.mk.store.pm;

import java.io.File;

import org.apache.jackrabbit.mk.model.ChildNodeEntriesMap;
import org.apache.jackrabbit.mk.model.Commit;
import org.apache.jackrabbit.mk.model.Id;
import org.apache.jackrabbit.mk.model.Node;
import org.apache.jackrabbit.mk.model.StoredCommit;
import org.apache.jackrabbit.mk.store.Binding;
import org.apache.jackrabbit.mk.store.NotFoundException;

/**
 * Defines the methods exposed by a persistence manager, that stores head
 * revision id, nodes, child node entries and blobs.
 * 
 * TODO: instead of deserializing objects on their own, return Binding
 *       instances, such as in #readNodeBinding.
 */
public interface PersistenceManager {

    void initialize(File homeDir) throws Exception;

    void close();

    String readHead() throws Exception;

    void writeHead(String id) throws Exception;

    Binding readNodeBinding(Id id) throws NotFoundException, Exception;

    Id writeNode(Node node) throws Exception;

    ChildNodeEntriesMap readCNEMap(Id id) throws NotFoundException, Exception;

    Id writeCNEMap(ChildNodeEntriesMap map) throws Exception;

    StoredCommit readCommit(String id) throws NotFoundException, Exception;

    void writeCommit(byte[] rawId, Commit commit) throws Exception;
}
