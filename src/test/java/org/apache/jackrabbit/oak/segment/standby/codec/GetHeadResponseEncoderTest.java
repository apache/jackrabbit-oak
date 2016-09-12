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

import static org.apache.jackrabbit.oak.segment.standby.StandbyTestUtils.mockRecordId;
import static org.junit.Assert.assertEquals;

import com.google.common.base.Charsets;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import org.apache.jackrabbit.oak.segment.RecordId;
import org.junit.Test;

public class GetHeadResponseEncoderTest {

    @Test
    public void encodeResponse() throws Exception {
        RecordId recordId = mockRecordId(1, 2, 8);

        EmbeddedChannel channel = new EmbeddedChannel(new GetHeadResponseEncoder());
        channel.writeOutbound(new GetHeadResponse("clientId", recordId));
        ByteBuf buffer = (ByteBuf) channel.readOutbound();

        ByteBuf expected = Unpooled.buffer();
        expected.writeInt(recordId.toString().getBytes(Charsets.UTF_8).length + 1);
        expected.writeByte(Messages.HEADER_RECORD);
        expected.writeBytes(recordId.toString().getBytes(Charsets.UTF_8));
        assertEquals(expected, buffer);
    }

}
