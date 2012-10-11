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
package org.apache.jackrabbit.oak.util;

import org.apache.jackrabbit.oak.AbstractOakTest;
import org.apache.jackrabbit.oak.api.ContentRepository;
import org.apache.jackrabbit.oak.api.ContentSession;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.query.JsopUtil;
import org.junit.Before;
import org.junit.Test;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertFalse;
import static junit.framework.Assert.assertTrue;
import static org.apache.jackrabbit.oak.api.Type.STRING;

public class JsopUtilTest extends AbstractOakTest {

    protected ContentSession session;
    protected Root root;

    @Override
    @Before
    public void before() throws Exception {
        super.before();
        session = createAdminSession();
        root = session.getLatestRoot();
    }

    @Override
    protected ContentRepository createRepository() {
        return createEmptyRepository();
    }

    @Test
    public void test() throws Exception {
        Tree t = root.getTree("/");
        assertFalse(t.hasChild("test"));

        String add = "/ + \"test\": { \"a\": { \"id\": \"123\" }, \"b\": {} }";
        JsopUtil.apply(root, add);
        root.commit();

        t = root.getTree("/");
        assertTrue(t.hasChild("test"));

        t = t.getChild("test");
        assertEquals(2, t.getChildrenCount());
        assertTrue(t.hasChild("a"));
        assertTrue(t.hasChild("b"));

        assertEquals(0, t.getChild("b").getChildrenCount());

        t = t.getChild("a");
        assertEquals(0, t.getChildrenCount());
        assertTrue(t.hasProperty("id"));
        assertEquals("123", t.getProperty("id").getValue(STRING));

        String rm = "/ - \"test\"";
        JsopUtil.apply(root, rm);
        root.commit();

        t = root.getTree("/");
        assertFalse(t.hasChild("test"));
    }

}
