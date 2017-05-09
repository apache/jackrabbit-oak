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

import org.apache.jackrabbit.oak.api.AuthInfo;
import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.ContentSession;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.remote.RemoteBinaryFilters;
import org.apache.jackrabbit.oak.remote.RemoteBinaryId;
import org.apache.jackrabbit.oak.remote.RemoteCommitException;
import org.apache.jackrabbit.oak.remote.RemoteOperation;
import org.apache.jackrabbit.oak.remote.RemoteRevision;
import org.apache.jackrabbit.oak.remote.RemoteTreeFilters;
import org.apache.jackrabbit.oak.remote.RemoteValue;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ContentRemoteSessionTest {

    private ContentRemoteSession createSession() {
        return createSession(mock(ContentSession.class));
    }

    private ContentRemoteSession createSession(ContentRemoteBinaries binaries) {
        return new ContentRemoteSession(mock(ContentSession.class), mock(ContentRemoteRevisions.class), binaries);
    }

    private ContentRemoteSession createSession(ContentSession session, ContentRemoteBinaries binaries) {
        return new ContentRemoteSession(session, mock(ContentRemoteRevisions.class), binaries);
    }

    private ContentRemoteSession createSession(ContentSession session, ContentRemoteRevisions revisions) {
        return new ContentRemoteSession(session, revisions, mock(ContentRemoteBinaries.class));
    }

    private ContentRemoteSession createSession(ContentSession session) {
        return new ContentRemoteSession(session, mock(ContentRemoteRevisions.class), mock(ContentRemoteBinaries.class));
    }

    @Test
    public void testReadLastRevision() {
        assertNotNull(createSession().readLastRevision());
    }

    @Test
    public void testReadLastRevisionAsString() {
        Root root = mock(Root.class);

        AuthInfo authInfo = mock(AuthInfo.class);

        ContentSession session = mock(ContentSession.class);
        doReturn(authInfo).when(session).getAuthInfo();
        doReturn(root).when(session).getLatestRoot();

        ContentRemoteRevisions revisions = mock(ContentRemoteRevisions.class);
        doReturn("id").when(revisions).put(authInfo, root);

        assertEquals("id", createSession(session, revisions).readLastRevision().asString());
    }

    @Test
    public void testReadRevision() {
        Root root = mock(Root.class);

        AuthInfo authInfo = mock(AuthInfo.class);

        ContentSession session = mock(ContentSession.class);
        doReturn(authInfo).when(session).getAuthInfo();

        ContentRemoteRevisions revisions = mock(ContentRemoteRevisions.class);
        doReturn(root).when(revisions).get(authInfo, "id");

        assertNotNull(createSession(session, revisions).readRevision("id"));
    }

    @Test
    public void testReadRevisionAsString() {
        Root root = mock(Root.class);

        AuthInfo authInfo = mock(AuthInfo.class);

        ContentSession session = mock(ContentSession.class);
        doReturn(authInfo).when(session).getAuthInfo();

        ContentRemoteRevisions revisions = mock(ContentRemoteRevisions.class);
        doReturn(root).when(revisions).get(authInfo, "id");

        assertEquals("id", createSession(session, revisions).readRevision("id").asString());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testReadTreeWithNullRevision() throws Exception {
        createSession().readTree(null, "/", new RemoteTreeFilters());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testReadTreeWithInvalidRevision() throws Exception {
        createSession().readTree(mock(RemoteRevision.class), "/", new RemoteTreeFilters());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testReadTreeWithNullPath() throws Exception {
        ContentRemoteRevision revision = mock(ContentRemoteRevision.class);
        createSession().readTree(revision, null, new RemoteTreeFilters());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testReadTreeWithInvalidPath() throws Exception {
        ContentRemoteRevision revision = mock(ContentRemoteRevision.class);
        createSession().readTree(revision, "invalid", new RemoteTreeFilters());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testReadTreeWithNullFilters() throws Exception {
        ContentRemoteRevision revision = mock(ContentRemoteRevision.class);
        createSession().readTree(revision, "/", null);
    }

    @Test
    public void testReadNonExistingTree() throws Exception {
        Tree tree = mock(Tree.class);
        when(tree.exists()).thenReturn(false);

        Root root = mock(Root.class);
        when(root.getTree(anyString())).thenReturn(tree);

        ContentRemoteRevision revision = mock(ContentRemoteRevision.class);
        doReturn(root).when(revision).getRoot();

        assertNull(createSession().readTree(revision, "/", new RemoteTreeFilters()));
    }

    @Test
    public void testReadExistingTree() throws Exception {
        Tree tree = mock(Tree.class);
        when(tree.exists()).thenReturn(true);

        Root root = mock(Root.class);
        when(root.getTree(anyString())).thenReturn(tree);

        ContentRemoteRevision revision = mock(ContentRemoteRevision.class);
        doReturn(root).when(revision).getRoot();

        assertNotNull(createSession().readTree(revision, "/", new RemoteTreeFilters()));
    }

    @Test
    public void testCreateAddOperation() {
        assertNotNull(createSession().createAddOperation("/test", new HashMap<String, RemoteValue>()));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testCreateAddOperationWithNullPath() {
        createSession().createAddOperation(null, new HashMap<String, RemoteValue>());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testCreateAddOperationWithInvalidPath() {
        createSession().createAddOperation("invalid", new HashMap<String, RemoteValue>());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testCreateAddOperationWithRootPath() {
        createSession().createAddOperation("/", new HashMap<String, RemoteValue>());
    }

    @Test(expected = IllegalArgumentException.class)
    public void createAddOperationWithNullProperties() {
        createSession().createAddOperation("/test", null);
    }

    @Test
    public void testCreateRemoveOperation() {
        assertNotNull(createSession().createRemoveOperation("/test"));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testCreateRemoveOperationWithNullPath() {
        createSession().createRemoveOperation(null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testCreateRemoveOperationWithInvalidPath() {
        createSession().createRemoveOperation("invalid");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testCreateRemoveOperationWithRootPath() {
        createSession().createRemoveOperation("/");
    }

    @Test
    public void testCreateSetOperation() {
        assertNotNull(createSession().createSetOperation("/test", "name", RemoteValue.toText("value")));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testCreateSetOperationWithNullPath() {
        createSession().createSetOperation(null, "name", RemoteValue.toText("value"));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testCreateSetOperationWithInvalidPath() {
        createSession().createSetOperation("invalid", "name", RemoteValue.toText("value"));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testCreateSetOperationWithNullName() {
        createSession().createSetOperation("/test", null, RemoteValue.toText("value"));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testCreateSetOperationWithEmptyName() {
        createSession().createSetOperation("/test", "", RemoteValue.toText("value"));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testCreateSetOperationWithNullValue() {
        createSession().createSetOperation("/test", "name", null);
    }

    @Test
    public void testCreateUnsetOperation() {
        assertNotNull(createSession().createUnsetOperation("/test", "name"));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testCreateUnsetOperationWithNullPath() {
        createSession().createUnsetOperation(null, "name");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testCreateUnsetOperationWithInvalidPath() {
        createSession().createUnsetOperation("invalid", "name");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testCreateUnsetOperationWithNullName() {
        createSession().createUnsetOperation("/test", null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testCreateUnsetOperationWithEmptyName() {
        createSession().createUnsetOperation("/test", "");
    }

    @Test
    public void createCopyOperation() {
        assertNotNull(createSession().createCopyOperation("/source", "/target"));
    }

    @Test(expected = IllegalArgumentException.class)
    public void createCopyOperationWithNullSourcePath() {
        createSession().createCopyOperation(null, "/target");
    }

    @Test(expected = IllegalArgumentException.class)
    public void createCopyOperationWithInvalidSourcePath() {
        createSession().createCopyOperation("invalid", "/target");
    }

    @Test(expected = IllegalArgumentException.class)
    public void createCopyOperationWithNullTargetPath() {
        createSession().createCopyOperation("/source", null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void createCopyOperationWithInvalidTargetPath() {
        createSession().createCopyOperation("/source", "invalid");
    }

    @Test(expected = IllegalArgumentException.class)
    public void createCopyOperationWithSameSourceAndTargetPath() {
        createSession().createCopyOperation("/same", "/same");
    }

    @Test(expected = IllegalArgumentException.class)
    public void createCopyOperationWithSourceAncestorOfTarget() {
        createSession().createCopyOperation("/source", "/source/target");
    }

    @Test
    public void createMoveOperation() {
        assertNotNull(createSession().createMoveOperation("/source", "/target"));
    }

    @Test(expected = IllegalArgumentException.class)
    public void createMoveOperationWithNullSourcePath() {
        createSession().createMoveOperation(null, "/target");
    }

    @Test(expected = IllegalArgumentException.class)
    public void createMoveOperationWithInvalidSourcePath() {
        createSession().createMoveOperation("invalid", "/target");
    }

    @Test(expected = IllegalArgumentException.class)
    public void createMoveOperationWithNullTargetPath() {
        createSession().createMoveOperation("/source", null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void createMoveOperationWithInvalidTargetPath() {
        createSession().createMoveOperation("/source", "invalid");
    }

    @Test(expected = IllegalArgumentException.class)
    public void createMoveOperationWithSameSourceAndTargetPath() {
        createSession().createMoveOperation("/same", "/same");
    }

    @Test(expected = IllegalArgumentException.class)
    public void createMoveOperationWithSourceAncestorOfTarget() {
        createSession().createMoveOperation("/source", "/source/target");
    }

    @Test
    public void createAggregateOperation() {
        assertNotNull(createSession().createAggregateOperation(new ArrayList<RemoteOperation>()));
    }

    @Test(expected = IllegalArgumentException.class)
    public void createAggregateOperationWithNullList() {
        createSession().createAggregateOperation(null);
    }

    @Test
    public void testCommit() throws Exception {
        Root root = mock(Root.class);

        ContentRemoteOperation operation = mock(ContentRemoteOperation.class);

        ContentRemoteRevision revision = mock(ContentRemoteRevision.class);
        doReturn(root).when(revision).getRoot();

        assertNotNull(createSession().commit(revision, operation));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testCommitWithNullRevision() throws Exception {
        createSession().commit(null, mock(ContentRemoteOperation.class));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testCommitWithInvalidRevision() throws Exception {
        createSession().commit(mock(RemoteRevision.class), mock(ContentRemoteOperation.class));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testCommitWithNullOperation() throws Exception {
        createSession().commit(mock(ContentRemoteRevision.class), null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testCommitWithInvalidOperation() throws Exception {
        createSession().commit(mock(ContentRemoteRevision.class), mock(RemoteOperation.class));
    }

    @Test(expected = RemoteCommitException.class)
    public void testCommitWithOperationThrowingException() throws Exception {
        Root root = mock(Root.class);

        ContentRemoteOperation operation = mock(ContentRemoteOperation.class);
        doThrow(RemoteCommitException.class).when(operation).apply(root);

        ContentRemoteRevision revision = mock(ContentRemoteRevision.class);
        doReturn(root).when(revision).getRoot();

        createSession().commit(revision, operation);
    }

    @Test(expected = RemoteCommitException.class)
    public void testCommitWithConflictingCommit() throws Exception {
        Root root = mock(Root.class);
        doThrow(CommitFailedException.class).when(root).commit();

        ContentRemoteRevision revision = mock(ContentRemoteRevision.class);
        doReturn(root).when(revision).getRoot();

        createSession().commit(revision, mock(ContentRemoteOperation.class));
    }

    @Test
    public void testReadBinaryId() {
        Blob blob = mock(Blob.class);

        ContentRemoteBinaries binaries = mock(ContentRemoteBinaries.class);
        doReturn(blob).when(binaries).get("id");

        ContentRemoteSession remoteSession = createSession(binaries);
        assertNotNull(remoteSession.readBinaryId("id"));
    }

    @Test
    public void testReadBinaryIdAsString() {
        Blob blob = mock(Blob.class);

        ContentRemoteBinaries binaries = mock(ContentRemoteBinaries.class);
        doReturn(blob).when(binaries).get("id");

        ContentRemoteSession remoteSession = createSession(binaries);
        assertEquals("id", remoteSession.readBinaryId("id").asString());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testReadBinaryIdWithNullReference() {
        createSession().readBinaryId(null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testReadBinaryIdWithEmptyReference() {
        createSession().readBinaryId("");
    }

    @Test
    public void testReadBinaryIdWithInvalidReference() {
        Root root = mock(Root.class);
        doReturn(null).when(root).getBlob(anyString());

        ContentSession session = mock(ContentSession.class);
        doReturn(root).when(session).getLatestRoot();

        ContentRemoteSession remoteSession = createSession(session);
        assertNull(remoteSession.readBinaryId("id"));
    }

    @Test
    public void testReadBinary() {
        Blob blob = mock(Blob.class);

        ContentRemoteBinaryId binaryId = mock(ContentRemoteBinaryId.class);
        doReturn(blob).when(binaryId).asBlob();

        assertNotNull(createSession().readBinary(binaryId, new RemoteBinaryFilters()));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testReadBinaryWithNullId() {
        createSession().readBinary(null, new RemoteBinaryFilters());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testReadBinaryWithInvalidId() {
        createSession().readBinary(mock(RemoteBinaryId.class), new RemoteBinaryFilters());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testReadBinaryWithNullFilters() {
        createSession().readBinary(mock(ContentRemoteBinaryId.class), null);
    }

    @Test
    public void testWriteBinary() throws Exception {
        Blob blob = mock(Blob.class);

        InputStream stream = mock(InputStream.class);

        Root root = mock(Root.class);
        doReturn(blob).when(root).createBlob(stream);

        ContentSession session = mock(ContentSession.class);
        doReturn(root).when(session).getLatestRoot();

        ContentRemoteBinaries binaries = mock(ContentRemoteBinaries.class);
        doReturn("id").when(binaries).put(blob);

        ContentRemoteSession remoteSession = createSession(session, binaries);
        assertEquals("id", remoteSession.writeBinary(stream).asString());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testWriteBinaryWithNullStream() throws Exception {
        createSession().writeBinary(null);
    }

    @Test(expected = RuntimeException.class)
    public void testWriteBinaryFailure() throws Exception {
        InputStream stream = mock(InputStream.class);

        Root root = mock(Root.class);
        doThrow(IOException.class).when(root).createBlob(stream);

        ContentSession session = mock(ContentSession.class);
        doReturn(root).when(session).getLatestRoot();

        ContentRemoteSession remoteSession = createSession(session);
        remoteSession.writeBinary(stream);
    }

    @Test
    public void testReadBinaryLength() throws Exception {
        Blob blob = mock(Blob.class);
        doReturn(42L).when(blob).length();

        ContentRemoteBinaryId binaryId = mock(ContentRemoteBinaryId.class);
        doReturn(blob).when(binaryId).asBlob();

        assertEquals(42L, createSession().readBinaryLength(binaryId));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testReadBinaryLengthWithNullBinaryId() throws Exception {
        createSession().readBinaryLength(null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testReadBinaryLengthWithInvalidBinaryId() throws Exception {
        createSession().readBinaryLength(mock(RemoteBinaryId.class));
    }

}
