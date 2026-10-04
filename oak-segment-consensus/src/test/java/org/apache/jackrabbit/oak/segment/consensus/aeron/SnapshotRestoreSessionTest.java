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
package org.apache.jackrabbit.oak.segment.consensus.aeron;

import io.aeron.ExclusivePublication;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.UnsafeBuffer;
import org.apache.jackrabbit.oak.segment.file.FileStore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class SnapshotRestoreSessionTest {

    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();

    @Test
    public void writesRestoredFilesAndReturnsSnapshotState() throws Exception {
        File storeDir = tempFolder.newFolder("snapshot-restore");
        SnapshotRestoreSession session = new SnapshotRestoreSession(storeDir);

        session.onFragment(snapshotBuffer("{\"type\":\"metadata\",\"head\":\"head-1\",\"ethereumEpoch\":42,\"timestamp\":1234}"), 0, encodedLength("{\"type\":\"metadata\",\"head\":\"head-1\",\"ethereumEpoch\":42,\"timestamp\":1234}"));
        session.onFragment(snapshotBuffer("{\"type\":\"file_header\",\"fileType\":\"journal\",\"fileName\":\"journal.log\",\"fileSize\":11}"), 0, encodedLength("{\"type\":\"file_header\",\"fileType\":\"journal\",\"fileName\":\"journal.log\",\"fileSize\":11}"));
        session.onFragment(snapshotBuffer("hello ".getBytes(StandardCharsets.UTF_8)), 0, encodedLength("hello ".getBytes(StandardCharsets.UTF_8)));
        session.onFragment(snapshotBuffer("world".getBytes(StandardCharsets.UTF_8)), 0, encodedLength("world".getBytes(StandardCharsets.UTF_8)));

        SnapshotService.SnapshotState state = session.complete();

        assertNotNull(state);
        assertEquals("head-1", state.head);
        assertEquals(42, state.epoch);
        assertEquals(1234L, state.timestamp);
        assertEquals(1, state.fileCount);
        assertEquals(
            "hello world",
            Files.readString(new File(storeDir, "journal.log").toPath(), StandardCharsets.UTF_8)
        );
    }

    @Test
    public void supportsLegacyEpochMetadataField() throws Exception {
        File storeDir = tempFolder.newFolder("snapshot-legacy-epoch");
        SnapshotRestoreSession session = new SnapshotRestoreSession(storeDir);

        session.onFragment(snapshotBuffer("{\"type\":\"metadata\",\"head\":\"head-2\",\"epoch\":7,\"timestamp\":99}"), 0, encodedLength("{\"type\":\"metadata\",\"head\":\"head-2\",\"epoch\":7,\"timestamp\":99}"));

        SnapshotService.SnapshotState state = session.complete();

        assertNotNull(state);
        assertEquals("head-2", state.head);
        assertEquals(7, state.epoch);
        assertEquals(99L, state.timestamp);
        assertEquals(0, state.fileCount);
    }

    @Test
    public void leadershipTermRoundTripsThroughSnapshotMetadata() throws Exception {
        assertEquals(5L, roundTripLeadershipTerm(5L));
        assertEquals("snapshots without the field restore as unknown", -1L, roundTripLeadershipTerm(-1L));
    }

    private long roundTripLeadershipTerm(long leadershipTermId) throws Exception {
        FileStore fileStore = mock(FileStore.class, RETURNS_DEEP_STUBS);
        when(fileStore.getHead().getRecordId().toString()).thenReturn("head-3");
        SnapshotService service = new SnapshotService(fileStore, tempFolder.newFolder().getAbsolutePath());
        ExclusivePublication publication = mock(ExclusivePublication.class);
        SnapshotRestoreSession session = new SnapshotRestoreSession(tempFolder.newFolder());
        when(publication.offer(any(DirectBuffer.class), anyInt(), anyInt())).thenAnswer(invocation -> {
            session.onFragment(invocation.getArgument(0), invocation.getArgument(1), invocation.getArgument(2));
            return 1L;
        });

        service.createSnapshot(publication, mock(IdleStrategy.class), 3, leadershipTermId);

        SnapshotService.SnapshotState state = session.complete();
        assertEquals("head-3", state.head);
        assertEquals(3, state.epoch);
        return state.leadershipTermId;
    }

    @Test
    public void ignoresNonSnapshotFragmentsAndReturnsNullWithoutMetadata() throws Exception {
        File storeDir = tempFolder.newFolder("snapshot-no-metadata");
        SnapshotRestoreSession session = new SnapshotRestoreSession(storeDir);

        session.onFragment(bufferForTemplate(SimpleMessageHeader.TEMPLATE_ID_WRITE_PROPOSAL, "ignored".getBytes(StandardCharsets.UTF_8)), 0, encodedLength("ignored".getBytes(StandardCharsets.UTF_8)));
        session.onFragment(snapshotBuffer("orphan".getBytes(StandardCharsets.UTF_8)), 0, encodedLength("orphan".getBytes(StandardCharsets.UTF_8)));

        assertNull(session.complete());
    }

    private static UnsafeBuffer snapshotBuffer(String payload) {
        return snapshotBuffer(payload.getBytes(StandardCharsets.UTF_8));
    }

    private static UnsafeBuffer snapshotBuffer(byte[] payload) {
        return bufferForTemplate(SimpleMessageHeader.TEMPLATE_ID_SNAPSHOT, payload);
    }

    private static UnsafeBuffer bufferForTemplate(int templateId, byte[] payload) {
        UnsafeBuffer buffer = new UnsafeBuffer(new byte[encodedLength(payload)]);
        SimpleMessageHeader.encode(buffer, 0, payload.length, templateId);
        buffer.putBytes(SimpleMessageHeader.ENCODED_LENGTH, payload);
        return buffer;
    }

    private static int encodedLength(String payload) {
        return encodedLength(payload.getBytes(StandardCharsets.UTF_8));
    }

    private static int encodedLength(byte[] payload) {
        return SimpleMessageHeader.ENCODED_LENGTH + payload.length;
    }
}
