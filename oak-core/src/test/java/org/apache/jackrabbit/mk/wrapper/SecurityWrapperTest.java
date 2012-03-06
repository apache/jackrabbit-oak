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
package org.apache.jackrabbit.mk.wrapper;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import junit.framework.Assert;
import org.apache.jackrabbit.mk.MicroKernelFactory;
import org.apache.jackrabbit.mk.MultiMkTestBase;
import org.apache.jackrabbit.mk.api.MicroKernel;
import org.apache.jackrabbit.mk.api.MicroKernelException;
import org.apache.jackrabbit.mk.json.JsopTokenizer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/**
 * Test the security wrapper.
 */
@RunWith(Parameterized.class)
public class SecurityWrapperTest extends MultiMkTestBase {

    private String head;
    private MicroKernel mkAdmin;
    private MicroKernel mkGuest;

    public SecurityWrapperTest(String url) {
        super(url);
    }

    @Before
    public void setUp() throws Exception {
        super.setUp();
        if (!isSimpleKernel(mk)) {
            return;
        }
        head = mk.getHeadRevision();
        head = mk.commit("/", "+ \":user\": { \":rights\":\"admin\" }", head, "");
        head = mk.commit("/", "+ \":user/guest\": {\"password\": \"guest\", \"rights\":\"read\" }", head, "");
        head = mk.commit("/", "+ \":user/sa\": {\"password\": \"abc\", \"rights\":\"admin\" }", head, "");
        mkAdmin = MicroKernelFactory.getInstance("sec:sa@abc:" + url);
        mkGuest = MicroKernelFactory.getInstance("sec:guest@guest:" + url);
    }

    @After
    public void tearDown() throws InterruptedException {
        try {
            if (mkAdmin != null) {
                mkAdmin.dispose();
            }
            if (mkGuest != null) {
                mkGuest.dispose();
            }
            super.tearDown();
        } catch (Throwable e) {
            e.printStackTrace();
        }
    }

    @Test
    public void wrongPassword() {
        try {
            MicroKernelFactory.getInstance("sec:sa@xyz:" + url);
            fail();
        } catch (Throwable e) {
            // expected (wrong password)
        }
    }

    @Test
    public void commit() {
        if (!isSimpleKernel(mk)) {
            return;
        }
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
        if (!isSimpleKernel(mk)) {
            return;
        }
        String fromRevision = mkAdmin.getHeadRevision();
        String toRevision = mkAdmin.commit("/", "+ \"test\": { \"data\": \"Hello\" }", head, "");
        toRevision = mkAdmin.commit("/", "^ \"test/data\": \"Hallo\"", toRevision, "");
        toRevision = mkAdmin.commit("/", "^ \"test/data\": null", toRevision, "");
        String j2 = mkGuest.getJournal(fromRevision, toRevision);
        assertEquals("", filterJournal(j2));
        toRevision = mkAdmin.commit("/", "^ \":rights\": \"read\"", fromRevision, "");
        String j3 = mkGuest.getJournal(fromRevision, toRevision);
        assertEquals(
                "+\"/test\":{\"data\":\"Hello\"}\n" +
                "^\"/test/data\":\"Hallo\"\n" +
                "^\"/test/data\":null\n",
                filterJournal(j3));
        String journal = mkAdmin.getJournal(fromRevision, toRevision);
        assertEquals(
                "+\"/test\":{\"data\":\"Hello\"}\n" +
                "^\"/test/data\":\"Hallo\"\n" +
                "^\"/test/data\":null\n" +
                "+\"/:rights\":\"read\"",
                filterJournal(journal));
    }

    @Test
    public void getNodes() {
        if (!isSimpleKernel(mk)) {
            return;
        }
        head = mk.getHeadRevision();
        assertTrue(mkAdmin.nodeExists("/:user", head));
        assertFalse(mkGuest.nodeExists("/:user", head));
        head = mkAdmin.commit("/", "^ \":rights\": \"read\"", head, "");
        head = mkAdmin.commit("/", "+ \"test\": { \"data\": \"Hello\" }", head, "");
        assertTrue(mkAdmin.nodeExists("/", head));
        assertTrue(mkGuest.nodeExists("/", head));
        assertEquals("{\":rights\":\"read\",\":childNodeCount\":2,\":user\":{\":rights\":\"admin\",\":childNodeCount\":2,\"guest\":{},\"sa\":{}},\"test\":{\"data\":\"Hello\",\":childNodeCount\":0}}", mkAdmin.getNodes("/", head));
        assertEquals("{\":childNodeCount\":1,\"test\":{\"data\":\"Hello\",\":childNodeCount\":0}}", mkGuest.getNodes("/", head));
    }

    private String filterJournal(String journal) {
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
            t.read(JsopTokenizer.NUMBER);
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
