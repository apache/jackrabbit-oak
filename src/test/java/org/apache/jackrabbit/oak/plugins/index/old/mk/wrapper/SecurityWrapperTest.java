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
package org.apache.jackrabbit.oak.plugins.index.old.mk.wrapper;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import junit.framework.Assert;
import org.apache.jackrabbit.mk.api.MicroKernel;
import org.apache.jackrabbit.mk.api.MicroKernelException;
import org.apache.jackrabbit.mk.json.JsopReader;
import org.apache.jackrabbit.mk.json.JsopTokenizer;
import org.apache.jackrabbit.oak.plugins.index.old.mk.simple.SimpleKernelImpl;
import org.junit.Before;
import org.junit.Test;

/**
 * Test the security wrapper.
 */
public class SecurityWrapperTest {

    // TODO: Remove SimpleKernelImpl-specific assumptions from the test
    private final MicroKernel mk =
            new SimpleKernelImpl("mem:SecurityWrapperTest");

    private String head;
    private MicroKernel mkAdmin;
    private MicroKernel mkGuest;

    @Before
    public void setUp() throws Exception {
        head = mk.getHeadRevision();
        head = mk.commit("/", "+ \":user\": { \":rights\":\"admin\" }", head, "");
        head = mk.commit("/", "+ \":user/guest\": {\"password\": \"guest\", \"rights\":\"read\" }", head, "");
        head = mk.commit("/", "+ \":user/sa\": {\"password\": \"abc\", \"rights\":\"admin\" }", head, "");
        mkAdmin = new SecurityWrapper(mk, "sa", "abc");
        mkGuest = new SecurityWrapper(mk, "guest", "guest");
    }

    @Test
    public void wrongPassword() {
        try {
            new SecurityWrapper(mk, "sa", "xyz");
            fail();
        } catch (Throwable e) {
            // expected (wrong password)
        }
    }

    @Test
    public void commit() {
        head = mkAdmin.commit("/", "+ \"test\": { \"data\": \"Hello\" }", head, null);
        head = mkAdmin.commit("/", "- \"test\"", head, null);
        try {
            head = mkGuest.commit("/", "+ \"test\": { \"data\": \"Hello\" }", head, null);
            fail();
        } catch (MicroKernelException e) {
            // expected
        }
    }

    @Test
    public void getJournal() {
        String fromRevision = mkAdmin.getHeadRevision();
        String toRevision = mkAdmin.commit("/", "+ \"test\": { \"data\": \"Hello\" }", head, "");
        toRevision = mkAdmin.commit("/", "^ \"test/data\": \"Hallo\"", toRevision, "");
        toRevision = mkAdmin.commit("/", "^ \"test/data\": null", toRevision, "");
        String j2 = mkGuest.getJournal(fromRevision, toRevision, null);
        assertEquals("", filterJournal(j2));
        toRevision = mkAdmin.commit("/", "^ \":rights\": \"read\"", fromRevision, "");
        String j3 = mkGuest.getJournal(fromRevision, toRevision, null);
        assertEquals(
                "+\"/test\":{\"data\":\"Hello\"}\n" +
                "^\"/test/data\":\"Hallo\"\n" +
                "^\"/test/data\":null\n",
                filterJournal(j3));
        String journal = mkAdmin.getJournal(fromRevision, toRevision, null);
        assertEquals(
                "+\"/test\":{\"data\":\"Hello\"}\n" +
                "^\"/test/data\":\"Hallo\"\n" +
                "^\"/test/data\":null\n" +
                "+\"/:rights\":\"read\"",
                filterJournal(journal));
    }

    @Test
    public void getNodes() {
        head = mk.getHeadRevision();
        assertTrue(mkAdmin.nodeExists("/:user", head));
        assertFalse(mkGuest.nodeExists("/:user", head));
        assertNull(mkGuest.getNodes("/:user", head, 1, 0, -1, null));
        head = mkAdmin.commit("/", "^ \":rights\": \"read\"", head, "");
        head = mkAdmin.commit("/", "+ \"test\": { \"data\": \"Hello\" }", head, "");
        assertTrue(mkAdmin.nodeExists("/", head));
        assertNull(mkGuest.getNodes("/unknown", head, 1, 0, -1, null));
        assertNull(mkGuest.getNodes("/unknown/node", head, 1, 0, -1, null));
        assertTrue(mkGuest.nodeExists("/", head));
        assertNull(mkGuest.getNodes("/unknown", head, 1, 0, -1, null));
        assertEquals("{\":rights\":\"read\",\":childNodeCount\":2,\":user\":{\":rights\":\"admin\",\":childNodeCount\":2,\"guest\":{},\"sa\":{}},\"test\":{\"data\":\"Hello\",\":childNodeCount\":0}}", mkAdmin.getNodes("/", head, 1, 0, -1, null));
        assertEquals("{\":childNodeCount\":1,\"test\":{\"data\":\"Hello\",\":childNodeCount\":0}}", mkGuest.getNodes("/", head, 1, 0, -1, null));
    }

    private static String filterJournal(String journal) {
        JsopTokenizer t = new JsopTokenizer(journal);
        StringBuilder buff = new StringBuilder();
        t.read('[');
        boolean isNew = false;
        do {
            t.read('{');
            Assert.assertEquals("id", t.readString());
            t.read(':');
            t.readString();
            t.read(',');
            Assert.assertEquals("ts", t.readString());
            t.read(':');
            t.read(JsopReader.NUMBER);
            t.read(',');
            Assert.assertEquals("msg", t.readString());
            t.read(':');
            t.read();
            t.read(',');
            Assert.assertEquals("changes", t.readString());
            t.read(':');
            String changes = t.readString().trim();
            if (isNew) {
                if (buff.length() > 0) {
                    buff.append('\n');
                }
                buff.append(changes);
            }
            // the first revision isn't new, all others are
            isNew = true;
            t.read('}');
        } while (t.matches(','));
        return buff.toString();
    }

}
