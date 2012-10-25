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
package org.apache.jackrabbit.oak.plugins.index.old.mk;

import static org.junit.Assert.fail;
import junit.framework.Assert;
import org.apache.jackrabbit.mk.json.JsopReader;
import org.apache.jackrabbit.mk.json.JsopTokenizer;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/**
 * Test moving nodes.
 */
@RunWith(Parameterized.class)
public class MoveNodeIT extends MultiMkTestBase {

    private String head;
    private String journalRevision;

    public MoveNodeIT(String url) {
        super(url);
    }

    @Before
    @Override
    public void setUp() throws Exception {
        super.setUp();
        head = mk.getHeadRevision();
        commit("/", "+ \"test\": {\"a\":{}, \"b\":{}, \"c\":{}}");
        commit("/", "+ \"test2\": {}");
        getJournal();
    }

    @Test
    public void moveTryOverwriteExisting() {
        // move /test/b to /test2
        commit("/", "> \"test/b\": \"/test2/b\"");

        try {
            // try to move /test/a to /test2/b
            commit("/", "> \"test/a\": \"/test2/b\"");
            fail();
        } catch (Exception e) {
            // expected
        }
    }

    @Test
    public void moveTryBecomeDescendantOfSelf() {
        // move /test to /test/a/test

        try {
            // try to move /test to /test/a/test
            commit("/", "> \"test\": \"/test/a/test\"");
            fail();
        } catch (Exception e) {
            // expected
        }
    }

    private void commit(String root, String diff) {
        head = mk.commit(root, diff, head, null);
    }

    private String getJournal() {
        if (journalRevision == null) {
            String revs = mk.getRevisionHistory(0, 1, null);
            JsopTokenizer t = new JsopTokenizer(revs);
            t.read('[');
            do {
                t.read('{');
                Assert.assertEquals("id", t.readString());
                t.read(':');
                journalRevision = t.readString();
                t.read(',');
                Assert.assertEquals("ts", t.readString());
                t.read(':');
                t.read(JsopReader.NUMBER);
                t.read(',');
                Assert.assertEquals("msg", t.readString());
                t.read(':');
                t.read();
                t.read('}');
            } while (t.matches(','));
        }
        String head = mk.getHeadRevision();
        String journal = mk.getJournal(journalRevision, head, null);
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
        journalRevision = head;
        return buff.toString();
    }

}