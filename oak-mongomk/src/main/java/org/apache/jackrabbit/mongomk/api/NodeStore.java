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
package org.apache.jackrabbit.mongomk.api;

import org.apache.jackrabbit.mk.api.MicroKernel;
import org.apache.jackrabbit.mk.api.MicroKernelException;
import org.apache.jackrabbit.mongomk.api.model.Commit;
import org.apache.jackrabbit.mongomk.api.model.Node;

/**
 * The <code>NodeStore</code> interface deals with all node related operations
 * of the {@code MicroKernel}.
 *
 * <p>
 * Since binary storage and node storage most likely use different back-end
 * technologies two separate interfaces for these operations are provided.
 * </p>
 *
 * <p>
 * This interface is not only a partly {@code MicroKernel} but also provides a
 * different layer of abstraction by converting the {@link String} parameters
 * into higher level objects to ease the development for implementors of the
 * {@code MicroKernel}.
 * </p>
 *
 * @see {@code BlobStore}
 */
public interface NodeStore {

    /**
     * @see MicroKernel#commit(String, String, String, String)
     *
     * @param commit The {@link Commit} object to store in the back-end.
     * @return The revision id of this commit.
     * @throws Exception If an error occurred while committing.
     */
    String commit(Commit commit) throws Exception;

    /**
     * @see MicroKernel#diff(String, String, String, int)
     *
     * @param fromRevisionId a revision id, if {@code null} the current head revision is assumed
     * @param toRevisionId another revision id, if {@code null} the current head revision is assumed
     * @param path optional path filter; if {@code null} or {@code ""}.
     * The default ({@code "/"}) will be assumed, i.e. no filter will be applied
     * @param depth Depth limit; if {@code -1} no limit will be applied
     * @return JSON diff representation of the changes
     * @throws MicroKernelException if any of the specified revisions doesn't exist or if another error occurs
     */
    String diff(String fromRevisionId, String toRevisionId, String path, int depth)
            throws Exception;

    /**
     * @see MicroKernel#getHeadRevision()
     *
     * @return The revision id of the head revision.
     * @throws Exception If an error occurred while retrieving the head revision.
     */
    String getHeadRevision() throws Exception;

    /**
     * @see MicroKernel#getJournal(String, String, String)
     *
     * @param fromRevisionId id of first revision to be returned in journal
     * @param toRevisionId id of last revision to be returned in journal,
     * if {@code null} the current head revision is assumed
     * @param path optional path filter; if {@code null} or {@code ""}
     * the default ({@code "/"}) will be assumed, i.e. no filter will be applied
     * @return a chronological list of revisions in JSON format
     * @throws Exception if an error occurred while getting the journal.
     */
    String getJournal(String fromRevisionId, String toRevisionId, String path)
            throws Exception;

    /**
     * @see MicroKernel#getRevisionHistory(long, int, String)
     *
     * @param since timestamp (ms) of earliest revision to be returned
     * @param maxEntries maximum #entries to be returned; if < 0, no limit will be applied.
     * @param path optional path filter; if {@code null} or {@code ""} the default
     *  ({@code "/"}) will be assumed, i.e. no filter will be applied
     * @return a list of revisions in chronological order in JSON format.
     * @throws Exception if an error occurred while getting the revision history.
     */
    String getRevisionHistory(long since, int maxEntries, String path) throws Exception;;

    /**
     * @see MicroKernel#getNodes(String, String, int, long, int, String)
     *
     * @param path The path of the root of nodes to retrieve.
     * @param revisionId The revision id of the nodes or {@code null} if the latest head revision
     * should be retrieved.
     * @param depth The maximum depth of the retrieved node tree or -1 to retrieve all nodes.
     * @param offset The offset of the child list to retrieve.
     * @param maxChildNodes The count of children to retrieve or -1 to retrieve all children.
     * @param filter An optional filter for the retrieved nodes.
     * @return The {@link Node} of the root node.
     * @throws Exception If an error occurred while retrieving the nodes.
     */
    Node getNodes(String path, String revisionId, int depth, long offset, int maxChildNodes,
            String filter) throws Exception;

    /**
     * @see MicroKernel#merge(String, String)
     *
     * @param branchRevisionId Branch revision id to merge.
     * @param message Merge message.
     * @return The revision id after merge.
     * @throws Exception If an error occurred while merging.
     */
    String merge(String branchRevisionId, String message) throws Exception;

    /**
     * @see MicroKernel#rebase(String, String)
     *
     * @param branchRevisionId Branch revision id to rebase.
     * @param newBaseRevisionId  New base revision to rebase onto.
     * @return  The revision id of the rebased branch.
     * @throws Exception If an error occurred while rebasing.
     */
    String rebase(String branchRevisionId, String newBaseRevisionId) throws Exception;

    /**
     * @see MicroKernel#nodeExists(String, String)
     *
     * @param path The path of the node to test.
     * @param revisionId The revision id of the node or {@code null} for the head revision.
     * @return {@code true} if the node for the specific revision exists else {@code false}.
     * @throws Exception If an error occurred while testing the node.
     */
    boolean nodeExists(String path, String revisionId) throws Exception;

    /**
     * @see MicroKernel#waitForCommit(String, long)
     *
     * @param oldHeadRevisionId id of earlier head revision
     * @param timeout the maximum time to wait in milliseconds
     * @return the id of the head revision
     * @throws Exception if an error occurred while waiting.
     */
    String waitForCommit(String oldHeadRevisionId, long timeout) throws Exception;
}