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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.netty.channel.embedded.EmbeddedChannel;
import org.apache.jackrabbit.oak.segment.standby.codec.GetHeadRequest;
import org.apache.jackrabbit.oak.segment.standby.codec.GetHeadResponse;
import org.junit.Test;

public class GetHeadRequestHandlerTest {

    @Test
    public void successfulReadsShouldGenerateResponses() throws Exception {
        StandbyHeadReader reader = mock(StandbyHeadReader.class);
        when(reader.readHeadRecordId()).thenReturn("recordId");

        EmbeddedChannel channel = new EmbeddedChannel(new GetHeadRequestHandler(reader));
        channel.writeInbound(new GetHeadRequest("clientId"));
        GetHeadResponse response = (GetHeadResponse) channel.readOutbound();
        assertEquals("recordId", response.getHeadRecordId());
        assertEquals("clientId", response.getClientId());
    }

    @Test
    public void unsuccessfulReadsShouldBeDiscarded() throws Exception {
        StandbyHeadReader reader = mock(StandbyHeadReader.class);
        when(reader.readHeadRecordId()).thenReturn(null);

        EmbeddedChannel channel = new EmbeddedChannel(new GetHeadRequestHandler(reader));
        channel.writeInbound(new GetHeadRequest("clientId"));
        assertNull(channel.readOutbound());
    }

    @Test
    public void unrecognizedMessagesShouldBeIgnored() throws Exception {
        EmbeddedChannel channel = new EmbeddedChannel(new GetHeadRequestHandler(mock(StandbyHeadReader.class)));
        channel.writeInbound("unrecognized");
        assertEquals("unrecognized", channel.readInbound());
    }

}
