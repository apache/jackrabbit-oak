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

import static org.apache.jackrabbit.oak.segment.standby.codec.Messages.extractClientFrom;
import static org.apache.jackrabbit.oak.segment.standby.codec.Messages.extractMessageFrom;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import org.junit.Test;

public class MessagesTest {

    @Test
    public void extractMessageFromPayloadWithoutMagic() throws Exception {
        assertNull(extractMessageFrom("wrong"));
    }

    @Test
    public void extractMessageFromEmptyPayload() throws Exception {
        assertNull(extractMessageFrom("Standby-CMD@"));
    }

    @Test
    public void extractMessageFromPayloadWithClient() throws Exception {
        assertEquals("message",  extractMessageFrom("Standby-CMD@client:message"));
    }

    @Test
    public void extractMessageFromPayloadWithoutClient() throws Exception {
        assertEquals("message",  extractMessageFrom("Standby-CMD@:message"));
    }

    @Test
    public void extractClientFromPayloadWithoutMagic() throws Exception {
        assertNull(extractClientFrom("wrong"));
    }

    @Test
    public void extractClientFromEmptyPayload() throws Exception {
        assertNull(extractClientFrom("Standby-CMD@"));
    }

    @Test
    public void extractClientFromPayloadWithClient() throws Exception {
        assertEquals("client",  extractClientFrom("Standby-CMD@client:message"));
    }

    @Test
    public void extractClientFromPayloadWithoutClient() throws Exception {
        assertEquals("", extractClientFrom("Standby-CMD@:message"));
    }

}
