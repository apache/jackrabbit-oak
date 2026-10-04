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

import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import io.aeron.cluster.service.ClientSession;
import io.aeron.cluster.service.Cluster;
import io.aeron.logbuffer.Header;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.AgentTerminationException;
import org.agrona.concurrent.UnsafeBuffer;
import org.apache.jackrabbit.oak.segment.consensus.service.AppliedLogPosition;
import org.junit.Before;
import org.junit.Test;

import static io.aeron.cluster.service.Cluster.Role.LEADER;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class AeronIngressHandlerTest {

    private static final long LOG_POSITION = 4096L;

    private AeronMessageCodec codec;
    private MessageDispatcher dispatcher;
    private ClientSession session;
    private DirectBuffer buffer;
    private Header header;
    private Cluster cluster;
    private AeronIngressHandler handler;

    @Before
    public void setUp() {
        codec = mock(AeronMessageCodec.class);
        dispatcher = mock(MessageDispatcher.class);
        session = mock(ClientSession.class);
        buffer = mock(DirectBuffer.class);
        header = mock(Header.class);
        cluster = mock(Cluster.class);
        handler = new AeronIngressHandler(codec, dispatcher);

        when(session.id()).thenReturn(7L);
        when(header.position()).thenReturn(LOG_POSITION);
        when(cluster.role()).thenReturn(LEADER);
        when(codec.headerLength()).thenReturn(SimpleMessageHeader.ENCODED_LENGTH);
    }

    @Test
    public void handleMessageRejectsShortMessagesBeforeDecode() {
        boolean result = handler.handleMessage(
            session,
            123L,
            buffer,
            0,
            SimpleMessageHeader.ENCODED_LENGTH - 1,
            header,
            cluster
        );

        assertFalse(result);
        verify(codec, never()).decodeHeader(buffer, 0);
        verify(dispatcher, never()).dispatch(eq(123L), anyLong(), eq(buffer), eq(0), eq(SimpleMessageHeader.ENCODED_LENGTH - 1));
    }

    @Test
    public void handleMessageRunsCallbacksForGenesisMessages() {
        AtomicInteger heartbeats = new AtomicInteger();
        AtomicReference<String> genesis = new AtomicReference<>();
        String payload = "{\"command\":\"CREATE_GENESIS\",\"timestamp\":42,\"genesisValidator\":\"http://leader:8090\"}";
        buffer = bufferWithPayload(payload);
        handler.setHeartbeatCallback(heartbeats::incrementAndGet);
        handler.setGenesisCallback((json, logPosition) -> genesis.set(json));
        when(codec.decodeHeader(buffer, 0))
            .thenReturn(new SimpleMessageHeader.HeaderInfo(payload.getBytes(StandardCharsets.UTF_8).length,
                SimpleMessageHeader.TEMPLATE_ID_GENESIS_PROPOSAL, 1, 1));

        boolean result = handler.handleMessage(
            session,
            123L,
            buffer,
            0,
            SimpleMessageHeader.ENCODED_LENGTH + payload.getBytes(StandardCharsets.UTF_8).length,
            header,
            cluster
        );

        assertTrue(result);
        assertEquals(1, heartbeats.get());
        assertEquals(payload, genesis.get());
        verify(dispatcher, never()).dispatch(eq(123L), anyLong(), eq(buffer), eq(0),
            eq(SimpleMessageHeader.ENCODED_LENGTH + payload.getBytes(StandardCharsets.UTF_8).length));
    }

    @Test
    public void genesisRecordsItsLogPositionAndIsSkippedWhenTheStoreAlreadyHasIt() {
        String payload = "{\"command\":\"CREATE_GENESIS\",\"timestamp\":42,\"genesisValidator\":\"http://leader:8090\"}";
        buffer = bufferWithPayload(payload);
        int length = SimpleMessageHeader.ENCODED_LENGTH + payload.getBytes(StandardCharsets.UTF_8).length;
        when(codec.decodeHeader(buffer, 0)).thenReturn(new SimpleMessageHeader.HeaderInfo(
            payload.getBytes(StandardCharsets.UTF_8).length, SimpleMessageHeader.TEMPLATE_ID_GENESIS_PROPOSAL, 1, 1));
        MessageDispatcher realDispatcher = new MessageDispatcher();
        realDispatcher.setTermProvider(() -> 3L);
        List<AppliedLogPosition> applied = new ArrayList<>();
        AeronIngressHandler realHandler = new AeronIngressHandler(codec, realDispatcher);
        realHandler.setGenesisCallback((json, logPosition) -> applied.add(logPosition));

        assertTrue(realHandler.handleMessage(session, 123L, buffer, 0, length, header, cluster));
        realDispatcher.setReplayFloor(applied.get(0));
        assertTrue(realHandler.handleMessage(session, 123L, buffer, 0, length, header, cluster));

        assertEquals(Collections.singletonList(new AppliedLogPosition(LOG_POSITION, 0, 3L)), applied);
    }

    @Test
    public void handleMessageAcceptsGenesisAndSnapshotWithoutDispatcher() {
        String payload = "{\"command\":\"CREATE_GENESIS\",\"timestamp\":7,\"genesisValidator\":\"http://leader:8090\"}";
        buffer = bufferWithPayload(payload);
        when(codec.decodeHeader(buffer, 0))
            .thenReturn(new SimpleMessageHeader.HeaderInfo(payload.getBytes(StandardCharsets.UTF_8).length,
                SimpleMessageHeader.TEMPLATE_ID_GENESIS_PROPOSAL, 1, 1))
            .thenReturn(new SimpleMessageHeader.HeaderInfo(0, SimpleMessageHeader.TEMPLATE_ID_SNAPSHOT, 1, 1));

        assertTrue(handler.handleMessage(session, 123L, buffer, 0,
            SimpleMessageHeader.ENCODED_LENGTH + payload.getBytes(StandardCharsets.UTF_8).length, header, cluster));
        assertTrue(handler.handleMessage(session, 124L, buffer, 0,
            SimpleMessageHeader.ENCODED_LENGTH + payload.getBytes(StandardCharsets.UTF_8).length, header, cluster));

        verify(dispatcher, never()).dispatch(eq(123L), anyLong(), eq(buffer), eq(0),
            eq(SimpleMessageHeader.ENCODED_LENGTH + payload.getBytes(StandardCharsets.UTF_8).length));
        verify(dispatcher, never()).dispatch(eq(124L), anyLong(), eq(buffer), eq(0),
            eq(SimpleMessageHeader.ENCODED_LENGTH + payload.getBytes(StandardCharsets.UTF_8).length));
    }

    @Test
    public void handleMessageDelegatesToDispatcherForRegularMessages() {
        when(codec.decodeHeader(buffer, 0))
            .thenReturn(new SimpleMessageHeader.HeaderInfo(0, 999, 1, 1));
        when(dispatcher.dispatch(123L, LOG_POSITION, buffer, 0, 16)).thenReturn(true);

        boolean result = handler.handleMessage(session, 123L, buffer, 0, 16, header, cluster);

        assertTrue(result);
        verify(dispatcher).dispatch(123L, LOG_POSITION, buffer, 0, 16);
    }

    @Test
    public void handleMessageAllowsNullClusterForNonClusterBoundTests() {
        when(codec.decodeHeader(buffer, 0))
            .thenReturn(new SimpleMessageHeader.HeaderInfo(0, 999, 1, 1));
        when(dispatcher.dispatch(123L, LOG_POSITION, buffer, 0, 16)).thenReturn(true);

        boolean result = handler.handleMessage(session, 123L, buffer, 0, 16, header, null);

        assertTrue(result);
        verify(dispatcher).dispatch(123L, LOG_POSITION, buffer, 0, 16);
    }

    @Test
    public void handleMessageRateLimitsRepeatedDispatchFailures() throws Exception {
        when(codec.decodeHeader(buffer, 0))
            .thenReturn(new SimpleMessageHeader.HeaderInfo(0, 999, 1, 1));
        when(dispatcher.dispatch(123L, LOG_POSITION, buffer, 0, 16)).thenReturn(false);
        AtomicLong lastLogTime = atomicLongField(handler, "lastDispatchFailLogMs");
        AtomicInteger suppressed = atomicIntField(handler, "dispatchFailSuppressed");

        assertFalse(handler.handleMessage(session, 123L, buffer, 0, 16, header, cluster));
        assertTrue(lastLogTime.get() > 0);
        assertEquals(0, suppressed.get());

        assertFalse(handler.handleMessage(session, 123L, buffer, 0, 16, header, cluster));
        assertEquals(1, suppressed.get());

        lastLogTime.set(0L);
        suppressed.set(2);

        assertFalse(handler.handleMessage(session, 123L, buffer, 0, 16, header, cluster));
        assertEquals(0, suppressed.get());
    }

    @Test
    public void handleMessagePropagatesAgentTermination() {
        AgentTerminationException termination = new AgentTerminationException("unexpected Aeron close");
        when(codec.decodeHeader(buffer, 0)).thenReturn(new SimpleMessageHeader.HeaderInfo(
            8, SimpleMessageHeader.TEMPLATE_ID_WRITE_PROPOSAL, 1, 1));
        when(dispatcher.dispatch(anyLong(), anyLong(), eq(buffer), eq(0), eq(16))).thenThrow(termination);

        try {
            handler.handleMessage(session, 123L, buffer, 0, 16, header, cluster);
            fail("AgentTerminationException was swallowed");
        } catch (AgentTerminationException e) {
            assertSame(termination, e);
        }
    }

    @Test
    public void handleMessagePropagatesDecodingOrDispatchFailures() {
        RuntimeException failure = new RuntimeException("boom");
        when(codec.decodeHeader(buffer, 0)).thenThrow(failure);

        try {
            handler.handleMessage(session, 123L, buffer, 0, 16, header, cluster);
            fail("the failure was swallowed and the entry skipped");
        } catch (RuntimeException e) {
            assertSame(failure, e);
        }
    }

    private static AtomicLong atomicLongField(Object target, String name) throws Exception {
        Field field = target.getClass().getDeclaredField(name);
        field.setAccessible(true);
        return (AtomicLong) field.get(target);
    }

    private static AtomicInteger atomicIntField(Object target, String name) throws Exception {
        Field field = target.getClass().getDeclaredField(name);
        field.setAccessible(true);
        return (AtomicInteger) field.get(target);
    }

    private static DirectBuffer bufferWithPayload(String payload) {
        byte[] payloadBytes = payload.getBytes(StandardCharsets.UTF_8);
        byte[] bytes = new byte[SimpleMessageHeader.ENCODED_LENGTH + payloadBytes.length];
        System.arraycopy(payloadBytes, 0, bytes, SimpleMessageHeader.ENCODED_LENGTH, payloadBytes.length);
        return new UnsafeBuffer(bytes);
    }
}
