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

import static com.google.common.collect.Iterables.elementsEqual;
import static com.google.common.collect.Lists.newArrayList;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static org.apache.jackrabbit.oak.segment.standby.StandbyTestUtils.hash;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.UUID;

import com.google.common.base.Charsets;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.Test;

public class ResponseDecoderTest {

    @Test
    public void unrecognizedMessagesShouldBeDropped() throws Exception {
        ByteBuf buf = Unpooled.buffer();
        buf.writeInt(1);
        buf.writeByte(-1);

        EmbeddedChannel channel = new EmbeddedChannel(new ResponseDecoder());
        channel.writeInbound(buf);
        assertNull(channel.readInbound());
    }

    @Test
    public void shouldDecodeValidGetBlobResponses() throws Exception {
        byte[] blobData = new byte[] {1, 2, 3};

        String blobId = "blobId";
        byte[] blobIdBytes = blobId.getBytes(Charsets.UTF_8);

        ByteBuf buf = Unpooled.buffer();
        buf.writeInt(1 + 4 + blobIdBytes.length + 8 + blobData.length);
        buf.writeByte(Messages.HEADER_BLOB);
        buf.writeInt(blobIdBytes.length);
        buf.writeBytes(blobIdBytes);
        buf.writeLong(hash(blobData));
        buf.writeBytes(blobData);

        EmbeddedChannel channel = new EmbeddedChannel(new ResponseDecoder());
        channel.writeInbound(buf);
        GetBlobResponse response = (GetBlobResponse) channel.readInbound();
        assertEquals("blobId", response.getBlobId());
        assertArrayEquals(blobData, response.getBlobData());
    }

    @Test
    public void shouldDropInvalidGetBlobResponses() throws Exception {
        byte[] blobData = new byte[] {1, 2, 3};

        String blobId = "blobId";
        byte[] blobIdBytes = blobId.getBytes(Charsets.UTF_8);

        ByteBuf buf = Unpooled.buffer();
        buf.writeInt(1 + 4 + blobIdBytes.length + 8 + blobData.length);
        buf.writeByte(Messages.HEADER_BLOB);
        buf.writeInt(blobIdBytes.length);
        buf.writeBytes(blobIdBytes);
        buf.writeLong(hash(blobData) + 1);
        buf.writeBytes(blobData);

        EmbeddedChannel channel = new EmbeddedChannel(new ResponseDecoder());
        channel.writeInbound(buf);
        assertNull(channel.readInbound());
    }

    @Test
    public void shouldDecodeValidGetHeadResponses() throws Exception {
        String recordId = "recordId";
        byte[] recordIdBytes = recordId.getBytes(Charsets.UTF_8);

        ByteBuf in = Unpooled.buffer();
        in.writeInt(recordIdBytes.length + 1);
        in.writeByte(Messages.HEADER_RECORD);
        in.writeBytes(recordIdBytes);

        EmbeddedChannel channel = new EmbeddedChannel(new ResponseDecoder());
        channel.writeInbound(in);
        GetHeadResponse response = (GetHeadResponse) channel.readInbound();
        assertEquals(recordId, response.getHeadRecordId());
    }

    @Test
    public void shouldDecodeValidGetSegmentResponses() throws Exception {
        UUID uuid = new UUID(1, 2);
        byte[] data = new byte[] {3, 4, 5};

        ByteBuf buf = Unpooled.buffer();
        buf.writeInt(data.length + 25);
        buf.writeByte(Messages.HEADER_SEGMENT);
        buf.writeLong(uuid.getMostSignificantBits());
        buf.writeLong(uuid.getLeastSignificantBits());
        buf.writeLong(hash(data));
        buf.writeBytes(data);

        EmbeddedChannel channel = new EmbeddedChannel(new ResponseDecoder());
        channel.writeInbound(buf);
        GetSegmentResponse response = (GetSegmentResponse) channel.readInbound();
        assertEquals(uuid, UUID.fromString(response.getSegmentId()));
        assertArrayEquals(data, response.getSegmentData());
    }

    @Test
    public void shouldDecodeValidGetReferencesResponses() throws Exception {
        byte[] data = "a:b,c".getBytes(Charsets.UTF_8);

        ByteBuf buf = Unpooled.buffer();
        buf.writeInt(data.length + 1);
        buf.writeByte(Messages.HEADER_REFERENCES);
        buf.writeBytes(data);

        EmbeddedChannel channel = new EmbeddedChannel(new ResponseDecoder());
        channel.writeInbound(buf);
        GetReferencesResponse response = (GetReferencesResponse) channel.readInbound();
        assertEquals("a", response.getSegmentId());
        assertTrue(elementsEqual(asList("b", "c"), response.getReferences()));
    }

    @Test
    public void shouldDecodeValidSingleElementGetReferencesResponses() throws Exception {
        byte[] data = "a:b".getBytes(Charsets.UTF_8);

        ByteBuf buf = Unpooled.buffer();
        buf.writeInt(data.length + 1);
        buf.writeByte(Messages.HEADER_REFERENCES);
        buf.writeBytes(data);

        EmbeddedChannel channel = new EmbeddedChannel(new ResponseDecoder());
        channel.writeInbound(buf);
        GetReferencesResponse response = (GetReferencesResponse) channel.readInbound();
        assertEquals("a", response.getSegmentId());
        assertTrue(elementsEqual(newArrayList("b"), response.getReferences()));
    }

    @Test
    public void shouldDecodeValidZeroElementsGetReferencesResponses() throws Exception {
        byte[] data = "a:".getBytes(Charsets.UTF_8);

        ByteBuf buf = Unpooled.buffer();
        buf.writeInt(data.length + 1);
        buf.writeByte(Messages.HEADER_REFERENCES);
        buf.writeBytes(data);

        EmbeddedChannel channel = new EmbeddedChannel(new ResponseDecoder());
        channel.writeInbound(buf);
        GetReferencesResponse response = (GetReferencesResponse) channel.readInbound();
        assertEquals("a", response.getSegmentId());
        assertTrue(elementsEqual(emptyList(), response.getReferences()));
    }

    @Test
    public void shouldDropInvalidGetSegmentResponses() throws Exception {
        UUID uuid = new UUID(1, 2);
        byte[] data = new byte[] {3, 4, 5};

        ByteBuf buf = Unpooled.buffer();
        buf.writeInt(data.length + 25);
        buf.writeByte(Messages.HEADER_SEGMENT);
        buf.writeLong(uuid.getMostSignificantBits());
        buf.writeLong(uuid.getLeastSignificantBits());
        buf.writeLong(hash(data) + 1);
        buf.writeBytes(data);

        EmbeddedChannel channel = new EmbeddedChannel(new ResponseDecoder());
        channel.writeInbound(buf);
        assertNull(channel.readInbound());
    }

}
