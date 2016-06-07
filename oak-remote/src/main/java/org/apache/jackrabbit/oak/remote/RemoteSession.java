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

package org.apache.jackrabbit.oak.remote;

import java.io.InputStream;
import java.util.List;
import java.util.Map;

/**
 * Collection of operations available on a remote repository once the user
 * correctly logged in.
 * <p>
 * Operations working on pure content, like reading a tree or committing
 * changes, requires a revision. A revision represents a snapshot of the
 * repository in a specified point in time. This concept enables repeatable
 * reads and consistent writes.
 * <p>
 * When binary data is involved, this interface exposes methods to read and
 * write arbitrary binary data from and to the repository. A binary data is
 * considered an immutable collection of bytes that can be referenced by an
 * identifier.
 */
public interface RemoteSession {

    /**
     * Read the latest revision in the repository.
     * <p>
     * This operation is always meant to succeed, because the repository will
     * always have an initial revision to return to the caller.
     *
     * @return The latest revision in the repository.
     */
    RemoteRevision readLastRevision();

    /**
     * Read a revision given a string representation of the revision itself.
     * <p>
     * This operation may fail for a number of reasons. In example, the string
     * passed to this method is not a valid revision, or this string represents
     * a revision that was valid in the past but it is no more valid.
     *
     * @param revision The string representation of the revision.
     * @return The revision represented by the string passed to this method, or
     * {@code null} if the string representation is invalid.
     */
    RemoteRevision readRevision(String revision);

    /**
     * Read a sub-tree from the repository at the given revision. Some filters
     * may be applied to the tree to avoid reading unwanted information.
     *
     * @param revision The revision representing the state of the repository to
     *                 read from.
     * @param path     The path of the root of the subtree to read.
     * @param filters  Filters to apply to the returned tree.
     * @return The tree requested by the given path, filtered according to the
     * provided filters. The method can return {@code null} if the root of the
     * tree is not found in the repository for the given revision.
     */
    RemoteTree readTree(RemoteRevision revision, String path, RemoteTreeFilters filters);

    /**
     * Create an operation to represent the addition of a new node in the
     * repository.
     *
     * @param path       Path of the new node to create.
     * @param properties Initial set of properties attached to the new node.
     * @return An operation representing the addition of a new node.
     */
    RemoteOperation createAddOperation(String path, Map<String, RemoteValue> properties);

    /**
     * Create an operation representing the removal of an existing node from the
     * repository.
     *
     * @param path Path of the node to remove.
     * @return An operation representing the removal of an existing node.
     */
    RemoteOperation createRemoveOperation(String path);

    /**
     * Create an operation representing the creation or modification of a
     * property of an existing node.
     *
     * @param path  Path of the node where the property is or will be attached
     *              to.
     * @param name  Name of the property to set.
     * @param value Value of the property.
     * @return An operation representing the creation or modification of a
     * property of an existing node.
     */
    RemoteOperation createSetOperation(String path, String name, RemoteValue value);

    /**
     * Create an operation to represent the removal of an existing property from
     * an existing node in the repository.
     *
     * @param path Path of the node where the property is attached to.
     * @param name Name of the property to remove.
     * @return An operation representing the removal of a property.
     */
    RemoteOperation createUnsetOperation(String path, String name);

    /**
     * Create an operation to represent the copy of a subtree into another
     * location into the repository.
     *
     * @param source Path of the root of the subtree to copy.
     * @param target Path where the subtree should be copied to.
     * @return An operation representing a copy of a subtree.
     */
    RemoteOperation createCopyOperation(String source, String target);

    /**
     * Create an operation to represent the move of a subtree into another
     * location into the repository.
     *
     * @param source Path of the root of the source subtree to move.
     * @param target Path where the subtree should be moved to.
     * @return An operation representing a move of a subtree.
     */
    RemoteOperation createMoveOperation(String source, String target);

    /**
     * Create an operation that represents the aggregation of multiple, simpler
     * operations. The aggregated operations are applied in the same sequence
     * provided by this method.
     *
     * @param operations Sequence of operations to aggregate.
     * @return An operation that, when executed, will execute the provided
     * operations in the provided sequence.
     */
    RemoteOperation createAggregateOperation(List<RemoteOperation> operations);

    /**
     * Commit some changes to the repository. The changes are represented by an
     * operation. The operation will be applied on the repository state
     * represented by the given revision.
     *
     * @param revision  Revision where the changes should be applied to.
     * @param operation Operation to change the state of the repository.
     * @return A new revision representing the new state of the repository where
     * the changes are applied.
     * @throws RemoteCommitException if the provided operation can't be
     *                               performed on the provided repository
     *                               state.
     */
    RemoteRevision commit(RemoteRevision revision, RemoteOperation operation) throws RemoteCommitException;

    /**
     * Read a binary ID given a string representation of the binary ID itself.
     * <p>
     * This operations may fail for a number of reasons. In example, the string
     * doesn't represent a valid binary ID, or the string represents a binary ID
     * that was valid in the past but is no more valid.
     *
     * @param binaryId String representation of the binary ID.
     * @return The binary ID read from the repository. This method may return
     * {@code null} if the string representation of the binary ID is not valid.
     */
    RemoteBinaryId readBinaryId(String binaryId);

    /**
     * Read a binary object from the repository according to the given filters.
     * <p>
     * In the case of a binary object, filters are really simple. At most, it is
     * possible to read just a portion of the binary object instead of reading
     * it in its entirety.
     *
     * @param binaryId Binary ID referring to the binary object to read.
     * @param filters  Filters to apply to the returned binary object.
     * @return A stream representing the filtered content of the binary object.
     */
    InputStream readBinary(RemoteBinaryId binaryId, RemoteBinaryFilters filters);

    /**
     * Read the length of a binary object from the repository.
     *
     * @param binaryId Binary ID referring to the binary object whose length
     *                 should be read.
     * @return The length of the binary object.
     */
    long readBinaryLength(RemoteBinaryId binaryId);

    /**
     * Write a binary object into the repository and return a binary ID
     * referencing to it.
     *
     * @param stream Stream representing the binary object to write.
     * @return Binary ID referencing the binary object written in the
     * repository.
     */
    RemoteBinaryId writeBinary(InputStream stream);

    /**
     * Performs a search in the content and returns a set of search results.
     *
     * @param revision The revision that should be used when searching the
     *                 content.
     * @param query    The query. It may contain placeholders that are to be
     *                 substituted with the actual parameters.
     * @param language The language the query is written in. It identifies the
     *                 syntax of the query.
     * @param offset   How many rows to skip when returning the results.
     * @param limit    How many results to return.
     * @return Search results.
     * @throws RemoteQueryParseException if the query can't be correctly
     *                                   parsed.
     */
    RemoteResults search(RemoteRevision revision, String query, String language, long offset, long limit) throws RemoteQueryParseException;

}
