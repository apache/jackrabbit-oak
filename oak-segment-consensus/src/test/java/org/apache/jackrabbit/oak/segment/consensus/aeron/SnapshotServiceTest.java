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
import io.aeron.FragmentAssembler;
import io.aeron.Image;
import io.aeron.Publication;
import io.aeron.cluster.client.ClusterException;
import io.aeron.logbuffer.Header;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.AgentTerminationException;
import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.UnsafeBuffer;
import org.apache.jackrabbit.oak.segment.consensus.service.AppliedLogPosition;
import org.junit.Test;

import java.nio.charset.StandardCharsets;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * The service snapshot carries only the applied-log watermark, the term and diagnostics; never store files.
 */
public class SnapshotServiceTest {

    private final SnapshotService service = new SnapshotService();

    @Test
    public void metadataRoundTripsThroughThePublicationAndTheSnapshotImage() {
        SnapshotService.SnapshotState written =
            new SnapshotService.SnapshotState(new AppliedLogPosition(4096L, 2, 3L), 4L, 17, "head-1");
        ExclusivePublication publication = mock(ExclusivePublication.class);
        byte[][] offered = new byte[1][];
        when(publication.offer(any(DirectBuffer.class), anyInt(), anyInt())).thenAnswer(invocation -> {
            DirectBuffer buffer = invocation.getArgument(0);
            offered[0] = new byte[(int) invocation.getArgument(2)];
            buffer.getBytes((int) invocation.getArgument(1), offered[0]);
            return 128L;
        });

        service.createSnapshot(publication, mock(IdleStrategy.class), written);
        SnapshotService.SnapshotState read = service.restoreSnapshot(imageOf(offered[0]), mock(IdleStrategy.class));

        assertEquals(written.applied, read.applied);
        assertEquals(4L, read.leadershipTermId);
        assertEquals(17, read.epoch);
        assertEquals("head-1", read.head);
        verify(publication, times(1)).offer(any(DirectBuffer.class), anyInt(), anyInt());
    }

    @Test
    public void backPressureIsRetriedOnTheIdleStrategy() {
        ExclusivePublication publication = mock(ExclusivePublication.class);
        when(publication.offer(any(DirectBuffer.class), anyInt(), anyInt()))
            .thenReturn(Publication.BACK_PRESSURED, Publication.ADMIN_ACTION, 64L);
        IdleStrategy idle = mock(IdleStrategy.class);

        service.createSnapshot(publication, idle, state());

        verify(idle, times(2)).idle();
    }

    @Test
    public void closedOrFullPublicationFailsTheSnapshot() {
        for (long result : new long[] {Publication.CLOSED, Publication.MAX_POSITION_EXCEEDED, Publication.NOT_CONNECTED}) {
            ExclusivePublication publication = mock(ExclusivePublication.class);
            when(publication.offer(any(DirectBuffer.class), anyInt(), anyInt())).thenReturn(result);
            try {
                service.createSnapshot(publication, mock(IdleStrategy.class), state());
                fail("Expected failure for offer result " + result);
            } catch (ClusterException expected) {
                // the snapshot is not acknowledged as taken
            }
        }
    }

    @Test(expected = AgentTerminationException.class)
    public void agentTerminationFromTheIdleStrategyIsRethrown() {
        ExclusivePublication publication = mock(ExclusivePublication.class);
        when(publication.offer(any(DirectBuffer.class), anyInt(), anyInt())).thenReturn(Publication.BACK_PRESSURED);
        IdleStrategy idle = mock(IdleStrategy.class);
        doThrow(new AgentTerminationException("interrupted")).when(idle).idle();

        service.createSnapshot(publication, idle, state());
    }

    @Test
    public void snapshotWithoutWatermarkMetadataIsRejected() {
        String legacy = "{\"type\":\"metadata\",\"head\":\"h\",\"ethereumEpoch\":1,\"timestamp\":2}";
        try {
            service.restoreSnapshot(imageOf(frame(legacy)), mock(IdleStrategy.class));
            fail("Expected the store-streaming snapshot format to be rejected");
        } catch (ClusterException e) {
            assertTrue(e.getMessage(), e.getMessage().contains("unsupported"));
        }
        try {
            service.restoreSnapshot(imageOf(null), mock(IdleStrategy.class));
            fail("Expected an empty snapshot to be rejected");
        } catch (ClusterException e) {
            assertTrue(e.getMessage(), e.getMessage().contains("no"));
        }
    }

    private static SnapshotService.SnapshotState state() {
        return new SnapshotService.SnapshotState(new AppliedLogPosition(64L, 0, 0L), 0L, -1, "head");
    }

    private static byte[] frame(String json) {
        byte[] payload = json.getBytes(StandardCharsets.UTF_8);
        UnsafeBuffer buffer = new UnsafeBuffer(new byte[SimpleMessageHeader.ENCODED_LENGTH + payload.length]);
        SimpleMessageHeader.encode(buffer, 0, payload.length, SimpleMessageHeader.TEMPLATE_ID_SNAPSHOT);
        buffer.putBytes(SimpleMessageHeader.ENCODED_LENGTH, payload);
        return buffer.byteArray();
    }

    /** An image that delivers {@code frame} (if any) as one unfragmented message, then ends. */
    private static Image imageOf(byte[] frame) {
        Image image = mock(Image.class);
        boolean[] delivered = {frame == null};
        when(image.isEndOfStream()).thenAnswer(invocation -> delivered[0]);
        when(image.poll(any(), anyInt())).thenAnswer(invocation -> {
            FragmentAssembler assembler = invocation.getArgument(0);
            Header header = mock(Header.class);
            when(header.flags()).thenReturn((byte) 0xC0);
            assembler.onFragment(new UnsafeBuffer(frame), 0, frame.length, header);
            delivered[0] = true;
            return 1;
        });
        return image;
    }
}
