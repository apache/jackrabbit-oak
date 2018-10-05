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

import java.io.ByteArrayInputStream;
import java.io.InputStream;

import io.netty.channel.embedded.EmbeddedChannel;
import org.apache.commons.io.IOUtils;
import org.apache.jackrabbit.oak.segment.standby.codec.GetBlobRequest;
import org.apache.jackrabbit.oak.segment.standby.codec.GetBlobResponse;
import org.junit.Test;

public class GetBlobRequestHandlerTest {

    @Test
    public void successfulReadsShouldGenerateResponses() throws Exception {
        byte[] blobData = new byte[] {99, 114, 97, 112};

        StandbyBlobReader reader = mock(StandbyBlobReader.class);
        when(reader.readBlob("blobId")).thenReturn(new ByteArrayInputStream(blobData));
        when(reader.getBlobLength("blobId")).thenReturn(4L);

        EmbeddedChannel channel = new EmbeddedChannel(new GetBlobRequestHandler(reader));
        channel.writeInbound(new GetBlobRequest("clientId", "blobId"));
        GetBlobResponse response = (GetBlobResponse) channel.readOutbound();
        assertEquals("clientId", response.getClientId());
        assertEquals("blobId", response.getBlobId());
        assertEquals(blobData.length, response.getLength());
        try (InputStream is = response.getInputStream()) {
            byte[] receivedData = IOUtils.toByteArray(is);
            assertArrayEquals(blobData, receivedData);
        }
    }

    @Test
    public void unsuccessfulReadsShouldBeDiscarded() throws Exception {
        StandbyBlobReader reader = mock(StandbyBlobReader.class);
        when(reader.readBlob("blobId")).thenReturn(null);

        EmbeddedChannel channel = new EmbeddedChannel(new GetBlobRequestHandler(reader));
        channel.writeInbound(new GetBlobRequest("clientId", "blobId"));
        assertNull(channel.readOutbound());
    }

    @Test
    public void unrecognizedMessagesShouldBeIgnored() throws Exception {
        StandbyBlobReader reader = mock(StandbyBlobReader.class);
        EmbeddedChannel channel = new EmbeddedChannel(new GetBlobRequestHandler(reader));
        channel.writeInbound("unrecognized");
        assertEquals("unrecognized", channel.readInbound());
    }

}
