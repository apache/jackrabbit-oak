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

import static org.apache.jackrabbit.oak.segment.standby.StandbyTestUtils.hash;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.ByteArrayInputStream;

import com.google.common.base.Charsets;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import org.apache.jackrabbit.oak.api.Blob;
import org.junit.Test;

public class GetBlobResponseEncoderTest {

    @Test
    public void encodeResponse() throws Exception {
        byte[] data = new byte[] {1, 2, 3};

        String contentIdentity = "contentIdentity";
        byte[] contentIdentityBytes = contentIdentity.getBytes(Charsets.UTF_8);

        Blob blob = mock(Blob.class);
        when(blob.getNewStream()).thenReturn(new ByteArrayInputStream(data));
        when(blob.getContentIdentity()).thenReturn(contentIdentity);

        EmbeddedChannel channel = new EmbeddedChannel(new GetBlobResponseEncoder());
        channel.writeOutbound(new GetBlobResponse("clientId", blob));
        ByteBuf buffer = (ByteBuf) channel.readOutbound();

        ByteBuf expected = Unpooled.buffer();
        expected.writeInt(3);
        expected.writeByte(Messages.HEADER_BLOB);
        expected.writeInt(contentIdentityBytes.length);
        expected.writeBytes(contentIdentityBytes);
        expected.writeLong(hash(data));
        expected.writeBytes(data);

        assertEquals(expected, buffer);
    }

}
