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

package org.apache.jackrabbit.oak.segment.standby.codec;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.Test;

public class RequestDecoderTest {

    @Test
    public void shouldDecodeValidGetBlobRequests() throws Exception {
        EmbeddedChannel channel = new EmbeddedChannel(new RequestDecoder());
        channel.writeInbound(Messages.newGetBlobRequest("clientId", "blobId", false));
        GetBlobRequest request = (GetBlobRequest) channel.readInbound();
        assertEquals("clientId", request.getClientId());
        assertEquals("blobId", request.getBlobId());
    }

    @Test
    public void shouldDecodeValidGetHeadRequests() throws Exception {
        EmbeddedChannel channel = new EmbeddedChannel(new RequestDecoder());
        channel.writeInbound(Messages.newGetHeadRequest("clientId", false));
        GetHeadRequest request = (GetHeadRequest) channel.readInbound();
        assertEquals("clientId", request.getClientId());
    }

    @Test
    public void shouldDecodeValidGetSegmentRequests() throws Exception {
        EmbeddedChannel channel = new EmbeddedChannel(new RequestDecoder());
        channel.writeInbound(Messages.newGetSegmentRequest("clientId", "segmentId", false));
        GetSegmentRequest request = (GetSegmentRequest) channel.readInbound();
        assertEquals("clientId", request.getClientId());
        assertEquals("segmentId", request.getSegmentId());
    }

    @Test
    public void shouldDecodeValidGetReferencesRequests() throws Exception {
        EmbeddedChannel channel = new EmbeddedChannel(new RequestDecoder());
        channel.writeInbound(Messages.newGetReferencesRequest("clientId", "segmentId", false));
        GetReferencesRequest request = (GetReferencesRequest) channel.readInbound();
        assertEquals("clientId", request.getClientId());
        assertEquals("segmentId", request.getSegmentId());
    }

    @Test
    public void shouldDropInvalidMessages() throws Exception {
        EmbeddedChannel channel = new EmbeddedChannel(new RequestDecoder());
        channel.writeInbound("Standby-CMD@clientId:z");
        assertNull(channel.readInbound());
    }

    @Test
    public void shouldIgnoreInvalidMessages() throws Exception {
        EmbeddedChannel channel = new EmbeddedChannel(new RequestDecoder());
        channel.writeInbound("wrong");
        assertEquals("wrong", channel.readInbound());
    }

}
