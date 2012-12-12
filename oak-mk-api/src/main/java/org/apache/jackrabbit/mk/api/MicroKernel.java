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
 * The MicroKernel <b>Design Goals and Principles</b>:
 * <ul>
 * <li>manage huge trees of nodes and properties efficiently</li>
 * <li>MVCC-based concurrency control
 * (writers don't interfere with readers, snapshot isolation)</li>
 * <li>GIT/SVN-inspired DAG-based versioning model</li>
 * <li>highly scalable concurrent read & write operations</li>
 * <li>session-less API (there's no concept of sessions; an implementation doesn't need to track/manage session state)</li>
 * <li>easily portable to C</li>
 * <li>easy to remote</li>
 * <li>efficient support for large number of child nodes</li>
 * <li>integrated API for efficiently storing/retrieving large binaries</li>
 * <li>human-readable data serialization (JSON)</li>
 * </ul>
 * <p>
 * The MicroKernel <b>Data Model</b>:
 * </p>
 * <ul>
 * <li>simple JSON-inspired data model: just nodes and properties</li>
 * <li>a node consists of an unordered set of name -&gt; item mappings. each
 * property and child node is uniquely named and a single name can only
 * refer to a property or a child node, not both at the same time.
 * <li>properties are represented as name/value pairs</li>
 * <li>supported property types: string, number, boolean, array</li>
 * <li>a property value is stored and used as an opaque, unparsed character sequence</li>
 * </ul>
 * <p>
 * The <b>Retention Policy for Revisions</b>:
 * <p>
 * TODO specify retention policy for old revisions, i.e. minimal guaranteed retention period (OAK-114)
 * </p>
 * <p>
 * The <b>Retention Policy for Binaries</b>:
 * </p>
 * <p>
 * The MicroKernel implementation is free to remove binaries if both of the
 * following conditions are met:
 * </p>
 * <ul>
 * <li>If the binary is not references as a property value of the
 * format ":blobId:&lt;blobId&gt;" where &lt;blobId&gt; is the id returned by
 * {@link #write(InputStream in)}. This includes simple property values such as
 * {"bin": ":blobId:1234"} as well as array property values such as
 * {"array": [":blobId:1234", ":blobId:5678"]}.</li>
 * <li>If the binary was stored before the last retained revision (this is to
 * keep temporary binaries, and binaries that are not yet referenced).</li>
 * </ul>
 */
public interface MicroKernel {

    //---------------------------------------------------------< REVISION ops >

    /**
     * Return the id of the current head revision.
     *
     * @return the id of the head revision
     * @throws MicroKernelException if an error occurs
     */
    String getHeadRevision() throws MicroKernelException;

    /**
     * Returns a list of all currently available (historical) head revisions in
     * chronological order since a specific point. <i>Private</i> branch
     * revisions won't be included in the result.
     * <p/>
     * Format:
     * <pre>
     * [
     *   {
     *     "id" : "&lt;revisionId&gt;",
     *     "ts" : &lt;revisionTimestamp&gt;,
     *     "msg" : "&lt;commitMessage&gt;"
     *   },
     *   ...
     * ]
     * </pre>
     * The {@code path} parameter allows to filter the revisions by path, i.e.
     * only those revisions that affected the subtree rooted at {@code path}
     * will be included.
     * <p/>
     * The {@code maxEntries} parameter allows to limit the number of revisions
     * returned. if {@code maxEntries < 0} no limit will be applied. otherwise,
     * if the number of revisions satisfying the specified {@code since} and
     * {@code path} criteria exceeds {@code maxEntries}, only {@code maxEntries}
     * entries will be returned (in chronological order, starting with the oldest).
     *
     * @param since      timestamp (ms) of earliest revision to be returned
     * @param maxEntries maximum #entries to be returned;
     *                   if < 0, no limit will be applied.
     * @param path       optional path filter; if {@code null} or {@code ""} the
     *                   default ({@code "/"}) will be assumed, i.e. no filter
     *                   will be applied
     * @return a list of revisions in chronological order in JSON format.
     * @throws MicroKernelException if an error occurs
     */
    String /* jsonArray */ getRevisionHistory(long since, int maxEntries, String path)
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
     * <p/>
     * Note that commits on a <i>private</i> branch will be ignored.
     *
     * @param oldHeadRevisionId id of earlier head revision
     * @param timeout the maximum time to wait in milliseconds
     * @return the id of the head revision
     * @throws MicroKernelException if an error occurs
     * @throws InterruptedException if the thread was interrupted
     */
    String waitForCommit(String oldHeadRevisionId, long timeout)
            throws MicroKernelException, InterruptedException;

    /**
     * Returns a revision journal, starting with {@code fromRevisionId}
     * and ending with {@code toRevisionId} in chronological order.
     * <p/>
     * Format:
     * <pre>
     * [
     *   {
     *     "id" : "&lt;revisionId&gt;",
     *     "ts" : &lt;revisionTimestamp&gt;,
     *     "msg" : "&lt;commitMessage&gt;",
     *     "changes" : "&lt;JSON diff&gt;"
     *   },
     *   ...
     * ]
     * </pre>
     * If {@code fromRevisionId} and {@code toRevisionId} are not in chronological
     * order the returned journal will be empty (i.e. {@code []})
     * <p/>
     * The {@code path} parameter allows to filter the revisions by path, i.e.
     * only those revisions that affected the subtree rooted at {@code path}
     * will be included. The filter will also be applied to the JSON diff, i.e.
     * the diff will include only those changes that affected the subtree rooted
     * at {@code path}.
     * <p/>
     * A {@code MicroKernelException} is thrown if either {@code fromRevisionId}
     * or {@code toRevisionId} doesn't exist, if {@code fromRevisionId} denotes
     * a <i>private</i> branch revision <i>and</i> {@code toRevisionId} denotes
     * either a head revision or a revision on a different <i>private</i> branch,
     * or if another error occurs.
     * <p/>
     * If the journal includes <i>private</i> branch revisions, those entries
     * will include a {@code "branchRootId"} denoting the head revision the
     * <i>private</i> branch is based on.
     *
     * @param fromRevisionId id of first revision to be returned in journal
     * @param toRevisionId   id of last revision to be returned in journal,
     *                       if {@code null} the current head revision is assumed
     * @param path           optional path filter; if {@code null} or {@code ""}
     *                       the default ({@code "/"}) will be assumed, i.e. no
     *                       filter will be applied
     * @return a chronological list of revisions in JSON format
     * @throws MicroKernelException if any of the specified revisions doesn't exist or if another error occurs
     */
    String /* jsonArray */ getJournal(String fromRevisionId, String toRevisionId,
                                      String path)
            throws MicroKernelException;

    /**
     * Returns the JSON diff representation of the changes between the specified
     * revisions. The changes will be consolidated if the specified range
     * covers intermediary revisions. {@code fromRevisionId} and {@code toRevisionId}
     * don't need not be in a specific chronological order.
     * <p/>
     * The {@code path} parameter allows to filter the changes included in the
     * JSON diff, i.e. only those changes that affected the subtree rooted at
     * {@code path} will be included.
     * <p/>
     * The {@code depth} limit applies to the subtree rooted at {@code path}.
     * It allows to limit the depth of the diff, i.e. only changes up to the
     * specified depth will be included in full detail. changes at paths exceeding
     * the specified depth limit will be reported as {@code ^"/some/path" : {}},
     * indicating that there are unspecified changes below that path.
     * <table border="1">
     *   <tr>
     *     <th>{@code depth} value</th><th>scope of detailed diff</th>
     *    </tr>
     *    <tr>
     *      <td>-1</td><td>no limit will be applied</td>
     *    </tr>
     *    <tr>
     *      <td>0</td><td>changes affecting the properties and child node names of the node at {@code path}</td>
     *    </tr>
     *    <tr>
     *      <td>1</td><td>changes affecting the properties and child node names of the node at {@code path} and its direct descendants</td>
     *    </tr>
     *    <tr>
     *      <td>...</td><td>...</td>
     *    </tr>
     * </table>
     *
     * @param fromRevisionId a revision id, if {@code null} the current head revision is assumed
     * @param toRevisionId   another revision id, if {@code null} the current head revision is assumed
     * @param path           optional path filter; if {@code null} or {@code ""}
     *                       the default ({@code "/"}) will be assumed, i.e. no
     *                       filter will be applied
     * @param depth          depth  limit; if {@code -1} no limit will be applied
     * @return JSON diff representation of the changes
     * @throws MicroKernelException if any of the specified revisions doesn't exist or if another error occurs
     */
    String /* JSON diff */ diff(String fromRevisionId, String toRevisionId,
                                String path, int depth)
            throws MicroKernelException;

    //-------------------------------------------------------------< READ ops >

    /**
     * Determines whether the specified node exists.
     *
     * @param path       path denoting node
     * @param revisionId revision id, if {@code null} the current head revision is assumed
     * @return {@code true} if the specified node exists, otherwise {@code false}
     * @throws MicroKernelException if the specified revision does not exist or if another error occurs
     */
    boolean nodeExists(String path, String revisionId) throws MicroKernelException;

    /**
     * Returns the number of child nodes of the specified node.
     * <p/>
     * This is a convenience method since this information could gathered by
     * calling {@code getNodes(path, revisionId, 0, 0, 0, null)} and evaluating
     * the {@code :childNodeCount} property.
     *
     * @param path       path denoting node
     * @param revisionId revision id, if {@code null} the current head revision is assumed
     * @return the number of child nodes
     * @throws MicroKernelException if the specified node or revision does not exist or if another error occurs
     */
    long getChildNodeCount(String path, String revisionId) throws MicroKernelException;

    /**
     * Returns the node tree rooted at the specified parent node with the
     * specified depth, maximum child node maxChildNodes and offset. The depth of the
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
     * {@code :childNodeCount}) and their child node names
     * (i.e. empty child node objects)</td>
     * </tr>
     * <tr>
     * <td>depth = 2</td>
     * <td>[and so on...]</td>
     * </tr>
     * </table>
     * <p/>
     * Example (depth=0):
     * <pre>
     * {
     *   "someprop" : "someval",
     *   ":childNodeCount" : 2,
     *   "child1" : {},
     *   "child2" : {}
     * }
     * </pre>
     * Example (depth=1):
     * <pre>
     * {
     *   "someprop" : "someval",
     *   ":childNodeCount" : 2,
     *   "child1" : {
     *     "prop1" : 123,
     *     ":childNodeCount" : 2,
     *     "grandchild1" : {},
     *     "grandchild2" : {}
     *   },
     *   "child2" : {
     *     "prop1" : "bar",
     *     ":childNodeCount" : 0
     *   }
     * }
     * </pre>
     * Remarks:
     * <ul>
     * <li>If the property {@code :childNodeCount} equals 0, then the
     * node does not have any child nodes.
     * <li>If the value of {@code :childNodeCount} is larger than the number
     * of returned child nodes, then the node has more child nodes than those
     * included in the returned tree.</li>
     * </ul>
     * The {@code offset} parameter is only applied to the direct child nodes
     * of the root of the returned node tree. {@code maxChildNodes} however
     * is applied on all hierarchy levels.
     * <p/>
     * An {@code IllegalArgumentException} is thrown if both an {@code offset}
     * greater than zero and a {@code filter} on node names (see below) have been
     * specified.
     * <p/>
     * The order of the child nodes is stable for any given {@code revisionId},
     * i.e. calling {@code getNodes} repeatedly with the same {@code revisionId}
     * is guaranteed to return the child nodes in the same order, but the
     * specific order used is implementation-dependent and may change across
     * different revisions of the same node.
     * <p/>
     * The optional {@code filter} parameter allows to specify glob patterns for names of
     * nodes and/or properties to be included or excluded.
     * <p/>
     * Example:
     * <pre>
     * {
     *   "nodes": [ "foo*", "-foo1" ],
     *   "properties": [ "*", "-:childNodeCount" ]
     * }
     * </pre>
     * In the above example all child nodes with names starting with "foo" will
     * be included, except for nodes named "foo1"; similarly, all properties will
     * be included except for the ":childNodeCount" metadata property (see below).
     * <p/>
     * Glob Syntax:
     * <ul>
     * <li>a {@code nodes} or {@code properties} filter consists of one or more <i>globs</i>.</li>
     * <li>a <i>glob</i> prefixed by {@code -} (dash) is treated as an exclusion pattern;
     * all others are considered inclusion patterns.</li>
     * <li>a leading {@code -} (dash) must be escaped by prepending {@code \} (backslash)
     * if it should be interpreted as a literal.</li>
     * <li>{@code *} (asterisk) serves as a <i>wildcard</i>, i.e. it matches any
     * substring in the target name.</li>
     * <li>{@code *} (asterisk) occurrences within the glob to be interpreted as
     * literals must be escaped by prepending {@code \} (backslash).</li>
     * <li>a filter matches a target name if any of the inclusion patterns match but
     * none of the exclusion patterns.</li>
     * </ul>
     * If no filter is specified the implicit default filter is assumed:
     * {@code {"nodes":["*"],"properties":["*"]}}
     * <p/>
     * System-provided metadata properties:
     * <ul>
     *     <li>{@code :childNodeCount} provides the actual number of direct child nodes; this property
     *     is included by the implicit default filter. it can be excluded by specifying a filter such
     *     as {@code {properties:["*", "-:childNodeCount"]}}</li>
     *     <li>{@code :hash} provides a content-based identifier for the subtree
     *     rooted at the {@code :hash} property's parent node. {@code :hash} values
     *     are similar to fingerprints. they can be compared to quickly determine
     *     if two subtrees are identical. if the {@code :hash} values are different
     *     the respective subtrees are different with regard to structure and/or properties.
     *     if on the other hand the {@code :hash} values are identical the respective
     *     subtrees are identical with regard to structure and properties.
     *     {@code :hash} is <i>not</i> included by the implicit default filter.
     *     it can be included by specifying a filter such as {@code {properties:["*", ":hash"]}}.
     *     <p>Returning the {@code :hash} property is optional. Some implementations
     *     might only return it on specific nodes or might not support it at all.
     *     If however a {@code :hash} property is returned it has to obey the contract
     *     described above.</p>
     *     <p>Implementations that keep track of the child hash along with
     *     the child node name can return the {@code :hash} value also as
     *     a property of the child node objects, even if they'd otherwise
     *     be empty, for example due to a depth limit. If such child hashes
     *     are returned, the client can use them as an alternative to child
     *     paths when accessing those nodes.</li>
     *     <li>{@code :id} provides an implementation-specific identifier
     *     of a node. Identifiers are like content hashes as described above,
     *     except for the fact that two different identifiers may refer to
     *     identical subtrees. Also {@code :id} values may be returned for
     *     child nodes, in which case the client can use them for accessing
     *     those nodes.
     *     </li>
     * </ul>
     *
     * @param path          path denoting root of node tree to be retrieved,
     *                      or alternatively a previously returned
     *                      {@code :hash} or {@code :id} value; in the latter case
     *                      the {@code revisionId} parameter is ignored.
     * @param revisionId    revision id, if {@code null} the current head revision is assumed;
     *                      the {@code revisionId} parameter is ignored if {@code path}
     *                      is an identifier (i.e. a {@code :hash} or {@code :id} value).
     * @param depth         maximum depth of returned tree
     * @param offset        start position in the iteration order of child nodes (0 to start at the
     *                      beginning)
     * @param maxChildNodes maximum number of sibling child nodes to retrieve (-1 for all)
     * @param filter        optional filter on property and/or node names; if {@code null} or
     *                      {@code ""} the default filter will be assumed
     * @return node tree in JSON format or {@code null} if the specified node does not exist
     * @throws MicroKernelException if the specified revision does not exist or if another error occurs
     * @throws IllegalArgumentException if both an {@code offset > 0} and a {@code filter} on node names have been specified
     */
    String /* jsonTree */ getNodes(String path, String revisionId, int depth,
                                   long offset, int maxChildNodes, String filter)
            throws MicroKernelException;

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
     * @throws MicroKernelException if the specified revision doesn't exist or if another error occurs
     */
    String /* revisionId */ commit(String path, String jsonDiff,
                                   String revisionId, String message)
            throws MicroKernelException;

    /**
     * Creates a <i>private</i> branch revision off the specified <i>public</i>
     * trunk revision.
     * <p/>
     * A {@code MicroKernelException} is thrown if {@code trunkRevisionId} doesn't
     * exist, if it's not a <i>trunk</i> revision (i.e. it's not reachable
     * by traversing the revision history in reverse chronological order starting
     * from the current head revision) or if another error occurs.
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
     * @throws MicroKernelException if the specified blob does not exist or if another error occurs
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
     * @throws MicroKernelException if the specified blob does not exist or if another error occurs
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
