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

package org.apache.jackrabbit.oak.remote.content;

import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.ContentSession;
import org.apache.jackrabbit.oak.api.PropertyValue;
import org.apache.jackrabbit.oak.api.Result;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.remote.RemoteBinaryFilters;
import org.apache.jackrabbit.oak.remote.RemoteBinaryId;
import org.apache.jackrabbit.oak.remote.RemoteCommitException;
import org.apache.jackrabbit.oak.remote.RemoteOperation;
import org.apache.jackrabbit.oak.remote.RemoteQueryParseException;
import org.apache.jackrabbit.oak.remote.RemoteResults;
import org.apache.jackrabbit.oak.remote.RemoteRevision;
import org.apache.jackrabbit.oak.remote.RemoteSession;
import org.apache.jackrabbit.oak.remote.RemoteTreeFilters;
import org.apache.jackrabbit.oak.remote.RemoteValue;

import java.io.IOException;
import java.io.InputStream;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.jackrabbit.oak.commons.PathUtils.denotesRoot;
import static org.apache.jackrabbit.oak.commons.PathUtils.isAbsolute;
import static org.apache.jackrabbit.oak.commons.PathUtils.isAncestor;

class ContentRemoteSession implements RemoteSession {

    private final ContentSession contentSession;

    private final ContentRemoteRevisions contentRemoteRevisions;

    private final ContentRemoteBinaries contentRemoteBinaries;

    public ContentRemoteSession(ContentSession contentSession, ContentRemoteRevisions contentRemoteRevisions, ContentRemoteBinaries contentRemoteBinaries) {
        this.contentSession = contentSession;
        this.contentRemoteRevisions = contentRemoteRevisions;
        this.contentRemoteBinaries = contentRemoteBinaries;
    }

    @Override
    public ContentRemoteRevision readLastRevision() {
        Root root = contentSession.getLatestRoot();
        String revisionId = contentRemoteRevisions.put(contentSession.getAuthInfo(), root);
        return new ContentRemoteRevision(revisionId, root);
    }

    @Override
    public ContentRemoteRevision readRevision(String revisionId) {
        Root root = contentRemoteRevisions.get(contentSession.getAuthInfo(), revisionId);

        if (root == null) {
            return null;
        }

        return new ContentRemoteRevision(revisionId, root);
    }

    @Override
    public ContentRemoteTree readTree(RemoteRevision revision, String path, RemoteTreeFilters filters) {
        ContentRemoteRevision contentRemoteRevision = null;

        if (revision instanceof ContentRemoteRevision) {
            contentRemoteRevision = (ContentRemoteRevision) revision;
        }

        if (contentRemoteRevision == null) {
            throw new IllegalArgumentException("revision not provided");
        }

        if (path == null) {
            throw new IllegalArgumentException("path not provided");
        }

        if (!isAbsolute(path)) {
            throw new IllegalArgumentException("invalid path");
        }

        if (filters == null) {
            throw new IllegalArgumentException("filters not provided");
        }

        Root root = contentRemoteRevision.getRoot();

        if (root == null) {
            throw new IllegalStateException("unable to locate the root");
        }

        Tree tree = root.getTree(path);

        if (tree.exists()) {
            return new ContentRemoteTree(tree, 0, filters, contentRemoteBinaries);
        }

        return null;
    }

    @Override
    public ContentRemoteOperation createAddOperation(String path, Map<String, RemoteValue> properties) {
        if (path == null) {
            throw new IllegalArgumentException("path not provided");
        }

        if (!isAbsolute(path)) {
            throw new IllegalArgumentException("invalid path");
        }

        if (denotesRoot(path)) {
            throw new IllegalArgumentException("adding root node");
        }

        if (properties == null) {
            throw new IllegalArgumentException("properties not provided");
        }

        List<ContentRemoteOperation> operations = new ArrayList<ContentRemoteOperation>();

        operations.add(new AddContentRemoteOperation(path));

        for (Map.Entry<String, RemoteValue> entry : properties.entrySet()) {
            operations.add(createSetOperation(path, entry.getKey(), entry.getValue()));
        }

        return new AggregateContentRemoteOperation(operations);
    }

    @Override
    public ContentRemoteOperation createRemoveOperation(String path) {
        if (path == null) {
            throw new IllegalArgumentException("path not provided");
        }

        if (!isAbsolute(path)) {
            throw new IllegalArgumentException("invalid path");
        }

        if (denotesRoot(path)) {
            throw new IllegalArgumentException("removing root node");
        }

        return new RemoveContentRemoteOperation(path);
    }

    @Override
    public ContentRemoteOperation createSetOperation(String path, String name, RemoteValue value) {
        if (path == null) {
            throw new IllegalArgumentException("path not provided");
        }

        if (!isAbsolute(path)) {
            throw new IllegalArgumentException("invalid path");
        }

        if (name == null) {
            throw new IllegalArgumentException("name not provided");
        }

        if (name.isEmpty()) {
            throw new IllegalArgumentException("name is empty");
        }

        if (value == null) {
            throw new IllegalArgumentException("value not provided");
        }

        return new SetContentRemoteOperation(contentRemoteBinaries, path, name, value);
    }

    @Override
    public ContentRemoteOperation createUnsetOperation(String path, String name) {
        if (path == null) {
            throw new IllegalArgumentException("path not provided");
        }

        if (!isAbsolute(path)) {
            throw new IllegalArgumentException("invalid path");
        }

        if (name == null) {
            throw new IllegalArgumentException("name not provided");
        }

        if (name.isEmpty()) {
            throw new IllegalArgumentException("name is empty");
        }

        return new UnsetContentRemoteOperation(path, name);
    }

    @Override
    public ContentRemoteOperation createCopyOperation(String source, String target) {
        if (source == null) {
            throw new IllegalArgumentException("source path not provided");
        }

        if (!isAbsolute(source)) {
            throw new IllegalArgumentException("invalid source path");
        }

        if (target == null) {
            throw new IllegalArgumentException("target path not provided");
        }

        if (!isAbsolute(target)) {
            throw new IllegalArgumentException("invalid target path");
        }

        if (source.equals(target)) {
            throw new IllegalArgumentException("same source and target path");
        }

        if (isAncestor(source, target)) {
            throw new IllegalArgumentException("source path is an ancestor of target path");
        }

        return new CopyContentRemoteOperation(source, target);
    }

    @Override
    public ContentRemoteOperation createMoveOperation(String source, String target) {
        if (source == null) {
            throw new IllegalArgumentException("source path not provided");
        }

        if (!isAbsolute(source)) {
            throw new IllegalArgumentException("invalid source path");
        }

        if (target == null) {
            throw new IllegalArgumentException("target path not provided");
        }

        if (!isAbsolute(target)) {
            throw new IllegalArgumentException("invalid target path");
        }

        if (source.equals(target)) {
            throw new IllegalArgumentException("same source and target path");
        }

        if (isAncestor(source, target)) {
            throw new IllegalArgumentException("source path is an ancestor of target path");
        }

        return new MoveContentRemoteOperation(source, target);
    }

    @Override
    public ContentRemoteOperation createAggregateOperation(final List<RemoteOperation> operations) {
        if (operations == null) {
            throw new IllegalArgumentException("operations not provided");
        }

        List<ContentRemoteOperation> contentRemoteOperations = new ArrayList<ContentRemoteOperation>();

        for (RemoteOperation operation : operations) {
            if (operation == null) {
                throw new IllegalArgumentException("operation not provided");
            }

            ContentRemoteOperation contentRemoteOperation = null;

            if (operation instanceof ContentRemoteOperation) {
                contentRemoteOperation = (ContentRemoteOperation) operation;
            }

            if (contentRemoteOperation == null) {
                throw new IllegalArgumentException("invalid operation");
            }

            contentRemoteOperations.add(contentRemoteOperation);
        }

        return new AggregateContentRemoteOperation(contentRemoteOperations);
    }

    @Override
    public ContentRemoteRevision commit(RemoteRevision revision, RemoteOperation operation) throws RemoteCommitException {
        ContentRemoteRevision contentRemoteRevision = null;

        if (revision instanceof ContentRemoteRevision) {
            contentRemoteRevision = (ContentRemoteRevision) revision;
        }

        if (contentRemoteRevision == null) {
            throw new IllegalArgumentException("invalid revision");
        }

        ContentRemoteOperation contentRemoteOperation = null;

        if (operation instanceof ContentRemoteOperation) {
            contentRemoteOperation = (ContentRemoteOperation) operation;
        }

        if (contentRemoteOperation == null) {
            throw new IllegalArgumentException("invalid operation");
        }

        Root root = contentRemoteRevision.getRoot();

        if (root == null) {
            throw new IllegalStateException("unable to locate the root");
        }

        contentRemoteOperation.apply(root);

        try {
            root.commit();
        } catch (CommitFailedException e) {
            throw new RemoteCommitException("unable to apply the changes", e);
        }

        return new ContentRemoteRevision(contentRemoteRevisions.put(contentSession.getAuthInfo(), root), root);
    }

    @Override
    public ContentRemoteBinaryId readBinaryId(String binaryId) {
        if (binaryId == null) {
            throw new IllegalArgumentException("binary id not provided");
        }

        if (binaryId.isEmpty()) {
            throw new IllegalArgumentException("invalid binary id");
        }

        Blob blob = contentRemoteBinaries.get(binaryId);

        if (blob == null) {
            return null;
        }

        return new ContentRemoteBinaryId(binaryId, blob);
    }

    @Override
    public InputStream readBinary(RemoteBinaryId binaryId, RemoteBinaryFilters filters) {
        ContentRemoteBinaryId contentRemoteBinaryId = null;

        if (binaryId instanceof ContentRemoteBinaryId) {
            contentRemoteBinaryId = (ContentRemoteBinaryId) binaryId;
        }

        if (contentRemoteBinaryId == null) {
            throw new IllegalArgumentException("invalid binary id");
        }

        if (filters == null) {
            throw new IllegalArgumentException("filters not provided");
        }

        return new ContentRemoteInputStream(contentRemoteBinaryId.asBlob().getNewStream(), filters);
    }

    @Override
    public long readBinaryLength(RemoteBinaryId binaryId) {
        ContentRemoteBinaryId contentRemoteBinaryId = null;

        if (binaryId instanceof ContentRemoteBinaryId) {
            contentRemoteBinaryId = (ContentRemoteBinaryId) binaryId;
        }

        if (contentRemoteBinaryId == null) {
            throw new IllegalArgumentException("invalid binary id");
        }

        return contentRemoteBinaryId.asBlob().length();
    }

    @Override
    public ContentRemoteBinaryId writeBinary(InputStream stream) {
        if (stream == null) {
            throw new IllegalArgumentException("stream not provided");
        }

        Blob blob;

        try {
            blob = contentSession.getLatestRoot().createBlob(stream);
        } catch (IOException e) {
            throw new RuntimeException("unable to write the binary object", e);
        }

        return new ContentRemoteBinaryId(contentRemoteBinaries.put(blob), blob);
    }

    @Override
    public RemoteResults search(RemoteRevision revision, String query, String language, long offset, long limit) throws RemoteQueryParseException {
        ContentRemoteRevision contentRemoteRevision = null;

        if (revision instanceof ContentRemoteRevision) {
            contentRemoteRevision = (ContentRemoteRevision) revision;
        }

        if (contentRemoteRevision == null) {
            throw new IllegalArgumentException("invalid revision");
        }

        Root root = contentRemoteRevision.getRoot();

        if (query == null) {
            throw new IllegalArgumentException("query not provided");
        }

        if (language == null) {
            throw new IllegalArgumentException("language not provided");
        }

        if (!root.getQueryEngine().getSupportedQueryLanguages().contains(language)) {
            throw new IllegalArgumentException("language not supported");
        }

        if (offset < 0) {
            throw new IllegalArgumentException("invalid offset");
        }

        if (limit < 0) {
            throw new IllegalArgumentException("invalid limit");
        }

        Result results;

        try {
            results = root.getQueryEngine().executeQuery(query, language, limit, offset, new HashMap<String, PropertyValue>(), new HashMap<String, String>());
        } catch (ParseException e) {
            throw new RemoteQueryParseException("invalid query", e);
        }

        return new ContentRemoteResults(contentRemoteBinaries, results);
    }

}
