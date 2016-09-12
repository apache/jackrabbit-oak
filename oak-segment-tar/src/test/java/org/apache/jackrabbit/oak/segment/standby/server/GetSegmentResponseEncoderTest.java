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

import static org.apache.jackrabbit.oak.segment.standby.server.ServerTestUtils.hash;
import static org.apache.jackrabbit.oak.segment.standby.server.ServerTestUtils.mockSegment;
import static org.junit.Assert.assertEquals;

import java.util.UUID;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import org.apache.jackrabbit.oak.segment.Segment;
import org.apache.jackrabbit.oak.segment.standby.codec.Messages;
import org.junit.Test;

public class GetSegmentResponseEncoderTest {

    @Test
    public void encodeResponse() throws Exception {
        UUID uuid = new UUID(1, 2);
        byte[] data = new byte[] {3, 4, 5};
        Segment segment = mockSegment(uuid, data);

        EmbeddedChannel channel = new EmbeddedChannel(new GetSegmentResponseEncoder());
        channel.writeOutbound(new GetSegmentResponse("clientId", segment));
        ByteBuf buffer = (ByteBuf) channel.readOutbound();

        ByteBuf expected = Unpooled.buffer();
        expected.writeInt(data.length + 25);
        expected.writeByte(Messages.HEADER_SEGMENT);
        expected.writeLong(uuid.getMostSignificantBits());
        expected.writeLong(uuid.getLeastSignificantBits());
        expected.writeLong(hash(data));
        expected.writeBytes(data);

        assertEquals(expected, buffer);
    }

}
