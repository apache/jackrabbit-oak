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

package org.apache.jackrabbit.oak.segment.standby.server;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.UUID;

import io.netty.channel.embedded.EmbeddedChannel;
import org.apache.jackrabbit.oak.segment.standby.codec.GetSegmentRequest;
import org.apache.jackrabbit.oak.segment.standby.codec.GetSegmentResponse;
import org.junit.Test;

public class GetSegmentRequestHandlerTest {

    @Test
    public void successfulReadsShouldGenerateResponses() throws Exception {
        byte[] data = new byte[] {3, 4, 5};

        StandbySegmentReader reader = mock(StandbySegmentReader.class);
        when(reader.readSegment("segmentId")).thenReturn(data);

        EmbeddedChannel channel = new EmbeddedChannel(new GetSegmentRequestHandler(reader));
        channel.writeInbound(new GetSegmentRequest("clientId", "segmentId"));
        GetSegmentResponse response = (GetSegmentResponse) channel.readOutbound();
        assertEquals("clientId", response.getClientId());
        assertEquals("segmentId", response.getSegmentId());
        assertArrayEquals(data, response.getSegmentData());
    }

    @Test
    public void unsuccessfulReadsShouldBeDiscarded() throws Exception {
        UUID uuid = new UUID(1, 2);

        StandbySegmentReader reader = mock(StandbySegmentReader.class);
        when(reader.readSegment(uuid.toString())).thenReturn(null);

        EmbeddedChannel channel = new EmbeddedChannel(new GetSegmentRequestHandler(reader));
        channel.writeInbound(new GetSegmentRequest("clientId", uuid.toString()));
        assertNull(channel.readOutbound());
    }

    @Test
    public void unrecognizedMessagesShouldBeIgnored() throws Exception {
        StandbySegmentReader reader = mock(StandbySegmentReader.class);
        EmbeddedChannel channel = new EmbeddedChannel(new GetSegmentRequestHandler(reader));
        channel.writeInbound("unrecognized");
        assertEquals("unrecognized", channel.readInbound());
    }

}
