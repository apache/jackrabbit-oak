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
package org.apache.jackrabbit.oak.segment.http.server.handlers;

import org.apache.jackrabbit.oak.segment.consensus.aeron.AeronConsensusEngine;
import org.apache.jackrabbit.oak.segment.file.FileStore;
import org.apache.jackrabbit.oak.segment.http.server.ServerContext;
import org.apache.jackrabbit.oak.segment.http.server.util.JsonOutputUtil;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.junit.Test;

import jakarta.servlet.http.HttpServletResponse;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.file.Paths;
import java.util.Arrays;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ConsensusStatusHandlerTest {

    @Test
    public void testHandleGetConsensusStatusReturnsStandaloneStateWithoutEngine() throws Exception {
        StringWriter body = new StringWriter();
        HttpServletResponse response = responseWithBody(body);

        new ConsensusStatusHandler(newContext(null)).handleGetConsensusStatus(response);

        verify(response).setStatus(HttpServletResponse.SC_OK);
        assertTrue(body.toString().contains("\"contractVersion\":\"consensus.status.v1\""));
        assertTrue(body.toString().contains("\"consensusType\":\"none\""));
        assertTrue(body.toString().contains("\"currentRole\":\"STANDALONE\""));
    }

    @Test
    public void testHandleGetConsensusStatusReturnsAeronStateAndOmitsNullLeader() throws Exception {
        StringWriter body = new StringWriter();
        HttpServletResponse response = responseWithBody(body);
        AeronConsensusEngine engine = mock(AeronConsensusEngine.class);
        when(engine.getCurrentRole()).thenReturn(org.apache.jackrabbit.oak.segment.consensus.leader.ValidatorRole.FOLLOWER);
        when(engine.isLeader()).thenReturn(false);
        when(engine.getCurrentLeader()).thenReturn(null);
        when(engine.getCurrentEpoch()).thenReturn(12);
        when(engine.getCurrentTerm()).thenReturn(8);
        when(engine.getReachableValidatorCount()).thenReturn(3);
        when(engine.getAllFollowers()).thenReturn(Arrays.asList("http://validator-2:8090"));
        when(engine.getCurrentEthereumEpoch()).thenReturn(1024);

        new ConsensusStatusHandler(newContext(engine)).handleGetConsensusStatus(response);

        String json = body.toString();
        assertTrue(json.contains("\"contractVersion\":\"consensus.status.v1\""));
        assertTrue(json.contains("\"consensusType\":\"aeron-cluster\""));
        assertTrue(json.contains("\"currentRole\":\"FOLLOWER\""));
        assertTrue(json.contains("\"isLeader\":false"));
        assertTrue(json.contains("\"currentEpoch\":12"));
        assertFalse(json.contains("\"currentLeader\""));
    }

    @Test
    public void testHandleGetConsensusLeaderReturnsCanonicalLeaderPayload() throws Exception {
        StringWriter body = new StringWriter();
        HttpServletResponse response = responseWithBody(body);
        AeronConsensusEngine engine = mock(AeronConsensusEngine.class);
        when(engine.getCurrentRole()).thenReturn(org.apache.jackrabbit.oak.segment.consensus.leader.ValidatorRole.FOLLOWER);
        when(engine.isLeader()).thenReturn(false);
        when(engine.getCurrentLeader()).thenReturn("http://validator-1:8090");
        when(engine.getCurrentTerm()).thenReturn(9);

        new ConsensusStatusHandler(newContext(engine)).handleGetConsensusLeader(response);

        String json = body.toString();
        assertTrue(json.contains("\"contractVersion\":\"consensus.leader.v1\""));
        assertTrue(json.contains("\"consensusType\":\"aeron-cluster\""));
        assertTrue(json.contains("\"currentLeader\":\"http://validator-1:8090\""));
        assertTrue(json.contains("\"leaderKnown\":true"));
        assertTrue(json.contains("\"currentTerm\":9"));
    }

    @Test
    public void testLocalOnlyLeaderQueryAnswersFromLocalKnowledgeWithoutDiscovery() throws Exception {
        StringWriter body = new StringWriter();
        HttpServletResponse response = responseWithBody(body);
        AeronConsensusEngine engine = mock(AeronConsensusEngine.class);
        when(engine.getCurrentRole()).thenReturn(org.apache.jackrabbit.oak.segment.consensus.leader.ValidatorRole.FOLLOWER);
        when(engine.getCurrentLeaderHint()).thenReturn("http://validator-2:8090");

        new ConsensusStatusHandler(newContext(engine)).handleGetConsensusLeader(true, response);

        verify(engine, never()).getCurrentLeader();
        String json = body.toString();
        assertTrue(json.contains("\"contractVersion\":\"consensus.leader.v1\""));
        assertTrue(json.contains("\"currentLeader\":\"http://validator-2:8090\""));
        assertTrue(json.contains("\"leaderKnown\":true"));
    }

    private static ServerContext newContext(AeronConsensusEngine engine) {
        ServerContext context = new ServerContext(
            mock(FileStore.class),
            mock(NodeStore.class),
            Paths.get("/tmp/store"),
            "http://localhost:8090"
        );
        context.aeronConsensusEngine = engine;
        return context;
    }

    private static HttpServletResponse responseWithBody(StringWriter body) throws Exception {
        HttpServletResponse response = mock(HttpServletResponse.class);
        when(response.getWriter()).thenReturn(new PrintWriter(body));
        return response;
    }
}
