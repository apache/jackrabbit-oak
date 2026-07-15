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
package org.apache.jackrabbit.oak.spi.security.authentication.external.impl.jmx;

import org.apache.jackrabbit.oak.spi.security.authentication.external.ExternalIdentityRef;
import org.apache.jackrabbit.oak.spi.security.authentication.external.SyncResult;
import org.apache.jackrabbit.oak.spi.security.authentication.external.basic.DefaultSyncResultImpl;
import org.apache.jackrabbit.oak.spi.security.authentication.external.basic.DefaultSyncedIdentity;
import org.apache.jackrabbit.util.Text;
import org.jetbrains.annotations.NotNull;
import org.junit.Test;

import java.util.Collections;

import static org.junit.Assert.assertEquals;

public class ResultMessagesTest {

    private final ResultMessages messages = new ResultMessages();

    private static void assertResultMessages(@NotNull ResultMessages resultMessages, @NotNull String expectedUid, @NotNull String expectedOperation) {
        for (String msg : resultMessages.getMessages()) {
            String op = msg.substring(msg.indexOf(":") + 2, msg.indexOf("\","));

            int index = msg.indexOf("uid:\"") + 5;
            String uid = msg.substring(index, msg.indexOf("\",", index));

            assertEquals(expectedUid, uid);
            assertEquals(expectedOperation, op);
        }
    }

    private static String extractOp(@NotNull SyncResult.Status status) {
        String st = status.toString().toLowerCase();
        if (st.indexOf('_') == -1) {
            return st.substring(0, 3);
        } else {
            StringBuilder s = new StringBuilder();
            for (String seg : Text.explode(st, '_')) {
                s.append(seg.charAt(0));
            }
            return s.toString();
        }
    }

    @Test
    public void testAppendResultWithNullSyncedIdentity() {
        SyncResult result = new DefaultSyncResultImpl(null, SyncResult.Status.NOP);
        messages.append(Collections.singletonList(result));

        assertResultMessages(messages, "", "nop");
    }

    @Test
    public void testSyncStatusReflectedInMessage() {
        for (SyncResult.Status status : SyncResult.Status.values()) {
            SyncResult result = new DefaultSyncResultImpl(null, status);
            ResultMessages msgs = new ResultMessages();
            msgs.append(Collections.singletonList(result));
            assertResultMessages(msgs, "", extractOp(status));
        }
    }

    @Test
    public void testUidReflectedInMessage() {
        SyncResult result = new DefaultSyncResultImpl(new DefaultSyncedIdentity("id", new ExternalIdentityRef("id", "name"), false, 0), SyncResult.Status.ENABLE);
        ResultMessages msgs = new ResultMessages();
        msgs.append(Collections.singletonList(result));
        assertResultMessages(msgs, "id", extractOp(SyncResult.Status.ENABLE));
    }
}