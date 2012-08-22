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

import org.apache.jackrabbit.mk.model.Commit;
import org.apache.jackrabbit.mk.model.Id;

/**
 * Advanced persistence implementation offering GC support.
 * <p>
 * The persistence implementation must ensure that objects written between {@link #start()}
 * and {@link #sweep()} are not swept, in other words, they must be marked implicitely.
 */
public interface GCPersistence extends Persistence {

    /**
     * Start a GC cycle. All objects written to the persistence in subsequent calls are
     * marked implicitely, i.e. they must be retained on {@link #sweep()}.
     */
    void start();
    
    /**
     * Mark a commit.
     * 
     * @param id
     *            commit id
     * @return {@code true} if the commit was not marked before;
     *         {@code false} otherwise
     * 
     * @throws Exception if an error occurs
     */
    boolean markCommit(Id id) throws Exception;
    
    /**
     * Replace a commit. Introduced to replace dangling parent commits where
     * a parent commit might be collected.
     * 
     * @param id
     *            commit id
     * @param 
     * @return {@code true} if the commit was not marked before;
     *         {@code false} otherwise
     * 
     * @throws Exception if an error occurs
     */
    void replaceCommit(Id id, Commit commit) throws Exception;
    
    /**
     * Mark a node.
     * 
     * @param id
     *            node id
     * @return {@code true} if the node was not marked before;
     *         {@code false} otherwise
     * 
     * @throws Exception if an error occurs
     */
    boolean markNode(Id id) throws Exception;

    /**
     * Mark a child node entry map.
     * 
     * @param id
     *            child node entry map id
     * @return {@code true} if the child node entry map was not marked before;
     *         {@code false} otherwise
     * 
     * @throws Exception if an error occurs
     */
    boolean markCNEMap(Id id) throws Exception;
    
    /**
     * Sweep all objects that are not marked and were written before the GC started.
     * 
     * @return number of swept items or <code>-1</code> if number is unknown
     * @throws Exception if an error occurs
     */
    int sweep() throws Exception;
}
