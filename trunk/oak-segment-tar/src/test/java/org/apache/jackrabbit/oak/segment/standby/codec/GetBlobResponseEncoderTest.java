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

import static org.apache.jackrabbit.oak.segment.standby.StandbyTestUtils.createBlobChunkBuffer;
import static org.apache.jackrabbit.oak.segment.standby.StandbyTestUtils.createMask;
import static org.junit.Assert.assertEquals;

import java.io.ByteArrayInputStream;

import io.netty.buffer.ByteBuf;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.stream.ChunkedWriteHandler;
import org.junit.Test;

public class GetBlobResponseEncoderTest {

    @Test
    public void shouldEncodeOneChunkResponse() throws Exception {
        byte[] blobData = new byte[] {1, 2, 3};

        String blobId = "blobId";
        byte mask = createMask(1, 1);

        EmbeddedChannel channel = new EmbeddedChannel(new ChunkedWriteHandler(), new GetBlobResponseEncoder(3));
        channel.writeOutbound(new GetBlobResponse("clientId", blobId,new ByteArrayInputStream(blobData), blobData.length));
        ByteBuf buffer = (ByteBuf) channel.readOutbound();
        ByteBuf expected = createBlobChunkBuffer(Messages.HEADER_BLOB, 3L, blobId, blobData, mask);
        
        assertEquals(expected, buffer);
    }

    @Test
    public void shouldEncodeTwoChunksResponse() throws Exception {
        byte[] blobData = new byte[] {1, 2, 3, 4};
        byte[] firstChunkData = new byte[] {1, 2};
        byte[] secondChunkbData = new byte[] {3, 4};

        String blobId = "blobId";

        EmbeddedChannel channel = new EmbeddedChannel(new ChunkedWriteHandler(), new GetBlobResponseEncoder(2));
        channel.writeOutbound(new GetBlobResponse("clientId", blobId,new ByteArrayInputStream(blobData), blobData.length));
        
        ByteBuf firstBuffer = (ByteBuf) channel.readOutbound();
        byte firstMask = createMask(1, 2);
        ByteBuf firstExpected = createBlobChunkBuffer(Messages.HEADER_BLOB, 4L, blobId, firstChunkData, firstMask);

        assertEquals(firstExpected, firstBuffer);
        
        ByteBuf secondBuffer = (ByteBuf) channel.readOutbound();
        byte secondMask = createMask(2, 2);
        ByteBuf secondExpected = createBlobChunkBuffer(Messages.HEADER_BLOB, 4L, blobId, secondChunkbData, secondMask);

        assertEquals(secondExpected, secondBuffer);
    }
}
