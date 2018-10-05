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

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;

import com.google.common.base.Charsets;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.Test;

public class GetReferencesResponseEncoderTest {

    @Test
    public void encodeResponse() throws Exception {
        EmbeddedChannel channel = new EmbeddedChannel(new GetReferencesResponseEncoder());
        channel.writeOutbound(new GetReferencesResponse("clientId", "a", asList("b", "c")));
        ByteBuf buffer = (ByteBuf) channel.readOutbound();

        String body = "a:b,c";
        byte[] data = body.getBytes(Charsets.UTF_8);

        ByteBuf expected = Unpooled.buffer();
        expected.writeInt(data.length + 1);
        expected.writeByte(Messages.HEADER_REFERENCES);
        expected.writeBytes(data);

        assertEquals(expected, buffer);

    }
}
