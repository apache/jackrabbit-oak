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
package org.apache.jackrabbit.oak.segment.consensus.server;

import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import org.apache.jackrabbit.oak.segment.consensus.aeron.AeronConsensusEngine;
import org.apache.jackrabbit.oak.segment.consensus.gc.GCProposalManager;
import org.apache.jackrabbit.oak.segment.consensus.service.MutationAuditMetadata;
import org.apache.jackrabbit.oak.segment.http.server.SegmentHttpServer;
import org.apache.jackrabbit.oak.segment.http.server.ServerContext;
import org.apache.jackrabbit.oak.segment.http.server.handlers.ConsensusApiHandler;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

public class AeronClusterCallbackBinderTest {

    @Test
    public void bindRoutesReplicatedWriteAndDeleteToConsensusHandler() {
        AeronConsensusEngine engine = mock(AeronConsensusEngine.class);
        SegmentHttpServer httpServer = mock(SegmentHttpServer.class);
        ConsensusApiHandler handler = mock(ConsensusApiHandler.class);
        ServerContext context = mock(ServerContext.class);

        when(httpServer.getConsensusApiHandler()).thenReturn(handler);
        when(httpServer.getContext()).thenReturn(context);

        new AeronClusterCallbackBinder().bind(engine, httpServer);

        ArgumentCaptor<AeronConsensusEngine.WriteApplicationCallback> writeCaptor =
            ArgumentCaptor.forClass(AeronConsensusEngine.WriteApplicationCallback.class);
        verify(engine).setWriteApplicationCallback(writeCaptor.capture());

        AeronConsensusEngine.WriteApplicationCallback callback = writeCaptor.getValue();
        MutationAuditMetadata writeAuditMetadata = MutationAuditMetadata.write(
            "tx-1", "corr-1", "proposal-1", "0xeth", 42L, 84L, null
        );
        MutationAuditMetadata deleteAuditMetadata = MutationAuditMetadata.delete(
            "tx-2", "corr-2", "proposal-2", null, null, null, 83L
        );
        callback.applyReplicatedWrite(
            "wallet", "/content", "text/plain", "body", "sig", "intent", "blob", "image/png", "cid",
            writeAuditMetadata
        );
        callback.applyReplicatedDelete("wallet", "/content", "sig", deleteAuditMetadata);

        ArgumentCaptor<MutationAuditMetadata> writeAuditCaptor = ArgumentCaptor.forClass(MutationAuditMetadata.class);
        ArgumentCaptor<MutationAuditMetadata> deleteAuditCaptor = ArgumentCaptor.forClass(MutationAuditMetadata.class);
        verify(handler).applyReplicatedWriteWithAuditMetadata(
            org.mockito.Mockito.eq("wallet"),
            org.mockito.Mockito.eq("/content"),
            org.mockito.Mockito.eq("text/plain"),
            org.mockito.Mockito.eq("body"),
            org.mockito.Mockito.eq("sig"),
            org.mockito.Mockito.eq("intent"),
            org.mockito.Mockito.eq("blob"),
            org.mockito.Mockito.eq("image/png"),
            org.mockito.Mockito.eq("cid"),
            writeAuditCaptor.capture()
        );
        verify(handler).applyReplicatedDeleteWithAuditMetadata(
            org.mockito.Mockito.eq("wallet"),
            org.mockito.Mockito.eq("/content"),
            org.mockito.Mockito.eq("sig"),
            deleteAuditCaptor.capture()
        );

        assertEquals(MutationAuditMetadata.Operation.WRITE, writeAuditCaptor.getValue().getOperation());
        assertEquals("proposal-1", writeAuditCaptor.getValue().getProposalId());
        assertEquals("tx-1", writeAuditCaptor.getValue().getTransactionId());
        assertEquals(Long.valueOf(84L), writeAuditCaptor.getValue().getEthereumObservedEpoch());
        assertEquals(MutationAuditMetadata.Operation.DELETE, deleteAuditCaptor.getValue().getOperation());
        assertEquals("proposal-2", deleteAuditCaptor.getValue().getProposalId());
        assertEquals("corr-2", deleteAuditCaptor.getValue().getCorrelationId());
        assertEquals(Long.valueOf(83L), deleteAuditCaptor.getValue().getEthereumFinalizedEpoch());
    }

    @Test
    public void bindRoutesReplicatedGCOperationsToManager() throws Exception {
        AeronConsensusEngine engine = mock(AeronConsensusEngine.class);
        SegmentHttpServer httpServer = mock(SegmentHttpServer.class);
        ServerContext context = mock(ServerContext.class);
        GCProposalManager manager = mock(GCProposalManager.class);

        context.gcProposalManager = manager;
        when(httpServer.getContext()).thenReturn(context);
        when(httpServer.getConsensusApiHandler()).thenReturn(mock(ConsensusApiHandler.class));

        new AeronClusterCallbackBinder().bind(engine, httpServer);

        ArgumentCaptor<AeronConsensusEngine.GCApplicationCallback> gcCaptor =
            ArgumentCaptor.forClass(AeronConsensusEngine.GCApplicationCallback.class);
        verify(engine).setGCCallback(gcCaptor.capture());

        AeronConsensusEngine.GCApplicationCallback callback = gcCaptor.getValue();
        callback.applyGCProposal("proposal-1", "wallet", "r42", 64L, "2.50", 1_000L);
        callback.applyGCVote("proposal-1", 2, true, null, 2_000L);
        callback.applyGCExecute("proposal-1", 3);

        verify(manager).applyReplicatedProposal("proposal-1", "wallet", "r42", 64L, "2.50", 1_000L);
        verify(manager).voteOnProposal("proposal-1", 2, true, "", 2_000L);
        verify(manager).applyReplicatedExecute("proposal-1", 3);
    }

    @Test
    public void bindSkipsGCOperationsWhenManagerMissing() {
        AeronConsensusEngine engine = mock(AeronConsensusEngine.class);
        SegmentHttpServer httpServer = mock(SegmentHttpServer.class);
        ServerContext context = mock(ServerContext.class);

        context.gcProposalManager = null;
        when(httpServer.getContext()).thenReturn(context);
        when(httpServer.getConsensusApiHandler()).thenReturn(mock(ConsensusApiHandler.class));

        ListAppender<ILoggingEvent> appender = TestLogAppenderSupport.attach(AeronClusterCallbackBinder.class);
        try {
            new AeronClusterCallbackBinder().bind(engine, httpServer);

            ArgumentCaptor<AeronConsensusEngine.GCApplicationCallback> gcCaptor =
                ArgumentCaptor.forClass(AeronConsensusEngine.GCApplicationCallback.class);
            verify(engine).setGCCallback(gcCaptor.capture());

            AeronConsensusEngine.GCApplicationCallback callback = gcCaptor.getValue();
            callback.applyGCProposal("proposal-1", "wallet", "r42", 64L, "2.50", 1_000L);
            callback.applyGCVote("proposal-1", 2, true, "ok", 2_000L);
            callback.applyGCExecute("proposal-1", 3);

            assertTrue(TestLogAppenderSupport.contains(appender, "GC proposal manager not initialized"));
        } finally {
            TestLogAppenderSupport.detach(AeronClusterCallbackBinder.class, appender);
        }
    }
}
