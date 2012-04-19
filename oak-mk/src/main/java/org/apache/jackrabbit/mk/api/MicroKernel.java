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
package org.apache.jackrabbit.mk.api;

import java.io.InputStream;

/**
 * The MicroKernel <b>design goals/principles</b>:
 * <ul>
 * <li>manage huge trees of nodes and properties efficiently</li>
 * <li>MVCC-based concurrency control
 * (writers don't interfere with readers, snapshot isolation)</li>
 * <li>GIT/SVN-inspired DAG-based versioning model</li>
 * <li>highly scalable concurrent read & write operations</li>
 * <li>stateless API</li>
 * <li>portable to C</li>
 * <li>efficient support for large number of child nodes</li>
 * <li>integrated API for efficiently storing/retrieving large binaries</li>
 * <li>human-readable data serialization (JSON)</li>
 * </ul>
 * <p/>
 * The MicroKernel <b>Data Model</b>:
 * <ul>
 * <li>simple JSON-inspired data model: just nodes and properties</li>
 * <li>a node consists of an unordered set of name -&gt; item mappings. each
 * property and child node is uniquely named and a single name can only
 * refer to a property or a child node, not both at the same time.
 * <li>properties are represented as name/value pairs</li>
 * <li>supported property types: string, number, boolean, array</li>
 * <li>a property value is stored and used as an opaque, unparsed character sequence</li>
 * </ul>
 */
public interface MicroKernel {

    /**
     * Dispose this instance.
     */
    void dispose();

    //---------------------------------------------------------< REVISION ops >

    /**
     * Return the id of the current head revision.
     *
     * @return the id of the head revision
     * @throws MicroKernelException if an error occurs
     */
    String getHeadRevision() throws MicroKernelException;

    /**
     * Returns a chronological list of all revisions since a specific point
     * in time.
     * <p/>
     * Format:
     * <pre>
     * [ { "id" : "<revisionId>", "ts" : <revisionTimestamp> }, ... ]
     * </pre>
     *
     * @param since      timestamp (ms) of earliest revision to be returned
     * @param maxEntries maximum #entries to be returned;
     *                   if < 0, no limit will be applied.
     * @return a chronological list of revisions in JSON format.
     * @throws MicroKernelException if an error occurs
     */
    String /* jsonArray */ getRevisions(long since, int maxEntries)
            throws MicroKernelException;

    /**
     * Waits for a commit to occur that is more recent than {@code oldHeadRevisionId}.
     * <p/>
     * This method allows for efficient polling for new revisions. The method
     * will return the id of the current head revision if it is more recent than
     * {@code oldHeadRevisionId}, or waits if either the specified amount of time
     * has elapsed or a new head revision has become available.
     * <p/>
     * if a zero or negative {@code timeout} value has been specified the method
     * will return immediately, i.e. calling {@code waitForCommit(0)} is
     * equivalent to calling {@code getHeadRevision()}.
     *
     * @param oldHeadRevisionId id of previous head revision
     * @param timeout the maximum time to wait in milliseconds
     * @return the id of the head revision
     * @throws MicroKernelException if an error occurs
     * @throws InterruptedException if the thread was interrupted
     */
    String waitForCommit(String oldHeadRevisionId, long timeout) throws MicroKernelException, InterruptedException;

    /**
     * Returns a revision journal, starting with {@code fromRevisionId}
     * and ending with @code toRevisionId}.
     * <p/>
     * Format:
     * <pre>
     * [ { "id" : "&lt;revisionId&gt;", "ts" : "&lt;revisionTimestamp&gt;", "msg" : "&lt;commitMessage&gt;", "changes" : "&lt;JSON diff&gt;" }, ... ]
     * </pre>
     *
     * @param fromRevisionId id of first revision to be returned in journal
     * @param toRevisionId   id of last revision to be returned in journal,
     *                       if {@code null} the current head revision is assumed
     * @param filter         (optional) filter criteria
     *                       (e.g. path, property names, etc);
     *                       TODO specify format and semantics
     * @return a chronological list of revisions in JSON format
     * @throws MicroKernelException if an error occurs
     */
    String /* jsonArray */ getJournal(String fromRevisionId, String toRevisionId,
                                      String filter)
            throws MicroKernelException;

    /**
     * Returns the JSON diff representation of the changes between the specified
     * revisions. The changes will be consolidated if the specified range
     * covers intermediary revisions. The revisions need not be in a specified
     * chronological order.
     *
     * <p/>
     * Format:
     * <pre>
     * [ { "id" : "&lt;revisionId&gt;", "ts" : "&lt;revisionTimestamp&gt;", "msg" : "&lt;commitMessage&gt;", "changes" : "&lt;JSON diff&gt;" }, ... ]
     * </pre>
     *
     * @param fromRevisionId a revision id
     * @param toRevisionId   another revision id, if {@code null} the current head revision is assumed
     * @param filter         (optional) filter criteria
     *                       (e.g. path, property names, etc);
     *                       TODO specify format and semantics
     * @return JSON diff representation of the changes
     * @throws MicroKernelException if an error occurs
     */
    String /* JSON diff */ diff(String fromRevisionId, String toRevisionId,
                                String filter)
            throws MicroKernelException;

    //-------------------------------------------------------------< READ ops >

    /**
     * Determines whether the specified node exists.
     *
     * @param path       path denoting node
     * @param revisionId revision id, if {@code null} the current head revision is assumed
     * @return {@code true} if the specified node exists, otherwise {@code false}
     * @throws MicroKernelException if an error occurs
     */
    boolean nodeExists(String path, String revisionId) throws MicroKernelException;

    /**
     * Returns the number of child nodes of the specified node.
     * <p/>
     * This is a convenience method since this information could gathered by
     * calling {@code getNodes(path, revisionId, 0, 0, 0)} and evaluating
     * the {@code :childNodeCount} property.
     *
     *
     * @param path       path denoting node
     * @param revisionId revision id, if {@code null} the current head revision is assumed
     * @return the number of child nodes
     * @throws MicroKernelException if the specified node does not exist or if an error occurs
     */
    long getChildNodeCount(String path, String revisionId) throws MicroKernelException;

    /**
     * Returns the node tree rooted at the specified parent node with depth 1.
     * Depth 1 means all properties of the node are returned, including the direct
     * child nodes and their properties (including
     * {@code :childNodeCount}). Example:
     * <pre>
     * {
     *     "someprop": "someval",
     *     ":childNodeCount": 2,
     *     "child1" : {
     *          "prop1": "foo",
     *          ":childNodeCount": 2
     *      },
     *      "child2": {
     *          "prop1": "bar"
     *          ":childNodeCount": 0
     *      }
     * }
     * </pre>
     * Remarks:
     * <ul>
     * <li>If the property {@code :childNodeCount} equals 0, then the
     * node does not have any child nodes.
     * <li>If the value of {@code :childNodeCount} is larger than the number
     * of returned child nodes, then the node has more child nodes than those
     * included in the tree. Large number of child nodes can be retrieved in
     * chunks using {@link #getNodes(String, String, int, long, int, String)}</li>
     * </ul>
     * This method is a convenience method for
     * {@code getNodes(path, revisionId, 1, 0, -1, null)}
     *
     * @param path       path denoting root of node tree to be retrieved
     * @param revisionId revision id, if {@code null} the current head revision is assumed
     * @return node tree in JSON format or {@code null} if the specified node does not exist
     * @throws MicroKernelException if the specified revision does not exist or if another error occurs
     */
    String /* jsonTree */ getNodes(String path, String revisionId) throws MicroKernelException;

    /**
     * Returns the node tree rooted at the specified parent node with the
     * specified depth, maximum child node count and offset. The depth of the
     * returned tree is governed by the {@code depth} parameter:
     * <table>
     * <tr>
     * <td>depth = 0</td>
     * <td>properties, including {@code :childNodeCount} and
     * child node names (i.e. empty child node objects)</td>
     * </tr>
     * <tr>
     * <td>depth = 1</td>
     * <td>properties, child nodes and their properties (including
     * {@code :childNodeCount})</td>
     * </tr>
     * <tr>
     * <td>depth = 2</td>
     * <td>[and so on...]</td>
     * </tr>
     * </table>
     * <p/>
     * The {@code offset} and {@code count} parameters are only applied to the
     * direct child nodes of the root of the returned node tree.
     *
     * @param path       path denoting root of node tree to be retrieved
     * @param revisionId revision id, if {@code null} the current head revision is assumed
     * @param depth      maximum depth of returned tree
     * @param offset     start position in the iteration order of child nodes (0 to start at the
     *                   beginning)
     * @param count      maximum number of child nodes to retrieve (-1 for all)
     * @param filter     (optional) filter criteria
     *                   (e.g. names of properties to be included, etc);
     *                   TODO specify format and semantics
     * @return node tree in JSON format or {@code null} if the specified node does not exist
     * @throws MicroKernelException if the specified revision does not exist or if another error occurs
     */
    String /* jsonTree */ getNodes(String path, String revisionId, int depth, long offset, int count, String filter) throws MicroKernelException;

    //------------------------------------------------------------< WRITE ops >

    /**
     * Applies the specified changes on the specified target node.
     * <p>
     * If {@code path.length() == 0} the paths specified in the
     * {@code jsonDiff} are expected to be absolute.
     * <p>
     * The implementation tries to merge changes if the revision id of the
     * commit is set accordingly. As an example, deleting a node is allowed if
     * the node existed in the given revision, even if it was deleted in the
     * meantime.
     *
     * @param path path denoting target node
     * @param jsonDiff changes to be applied in JSON diff format.
     * @param revisionId id of revision the changes are based on,
     *                   if {@code null} the current head revision is assumed
     * @param message commit message
     * @return id of newly created revision
     * @throws MicroKernelException if an error occurs
     */
    String /* revisionId */ commit(String path, String jsonDiff, String revisionId, String message)
            throws MicroKernelException;

    /**
     * Creates a <i>private</i> branch revision off the specified <i>public</i>
     * trunk revision.
     * <p/>
     * A {@code MicroKernelException} is thrown if {@code trunkRevisionId} doesn't
     * exist, if it's not a <i>trunk</i> revision (i.e. it's not reachable
     * by traversing the revision history backwards starting from the current
     * head revision) or if another error occurs.
     *
     * @param trunkRevisionId id of public trunk revision to base branch on,
     *                        if {@code null} the current head revision is assumed
     * @return id of newly created private branch revision
     * @throws MicroKernelException if {@code trunkRevisionId} doesn't exist,
     *                              if it's not a <i>trunk</i> revision
     *                              or if another error occurs
     * @see #merge(String, String)
     */
    String /* revisionId */ branch(String trunkRevisionId)
            throws MicroKernelException;

    /**
     * Merges the specified <i>private</i> branch revision with the current
     * head revision.
     * <p/>
     * A {@code MicroKernelException} is thrown if {@code branchRevisionId} doesn't
     * exist, if it's not a branch revision, if the merge fails because of
     * conflicting changes or if another error occurs.
     *
     * @param branchRevisionId id of private branch revision
     * @param message commit message
     * @return id of newly created head revision
     * @throws MicroKernelException if {@code branchRevisionId} doesn't exist,
     *                              if it's not a branch revision, if the merge
     *                              fails because of conflicting changes or if
     *                              another error occurs.
     * @see #branch(String)
     */
    String /* revisionId */ merge(String branchRevisionId, String message)
            throws MicroKernelException;

    //--------------------------------------------------< BLOB READ/WRITE ops >

    /**
     * Returns the length of the specified blob.
     *
     * @param blobId blob identifier
     * @return length of the specified blob
     * @throws MicroKernelException if an error occurs
     */
    long getLength(String blobId) throws MicroKernelException;

    /**
     * Reads up to {@code length} bytes of data from the specified blob into
     * the given array of bytes.  An attempt is made to read as many as
     * {@code length} bytes, but a smaller number may be read.
     * The number of bytes actually read is returned as an integer.
     *
     * @param blobId blob identifier
     * @param pos    the offset within the blob
     * @param buff   the buffer into which the data is read.
     * @param off    the start offset in array {@code buff}
     *               at which the data is written.
     * @param length the maximum number of bytes to read
     * @return the total number of bytes read into the buffer, or
     *         {@code -1} if there is no more data because the end of
     *         the blob content has been reached.
     * @throws MicroKernelException if an error occurs
     */
    int /* count */ read(String blobId, long pos, byte[] buff, int off, int length)
            throws MicroKernelException;

    /**
     * Stores the content of the given stream and returns an associated
     * identifier for later retrieval.
     * <p>
     * If identical stream content has been stored previously, then the existing
     * identifier will be returned instead of storing a redundant copy.
     * <p>
     * The stream is closed by this method.
     *
     * @param in InputStream providing the blob content
     * @return blob identifier associated with the given content
     * @throws MicroKernelException if an error occurs
     */
    String /* blobId */ write(InputStream in) throws MicroKernelException;
}
