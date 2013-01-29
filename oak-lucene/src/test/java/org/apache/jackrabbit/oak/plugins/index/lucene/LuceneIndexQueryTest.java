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
package org.apache.jackrabbit.oak.plugins.index.lucene;

import static junit.framework.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Iterator;

import org.apache.jackrabbit.oak.Oak;
import org.apache.jackrabbit.oak.api.ContentRepository;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.plugins.nodetype.write.InitialContent;
import org.apache.jackrabbit.oak.query.AbstractQueryTest;
import org.apache.jackrabbit.oak.query.JsopUtil;
import org.junit.Ignore;
import org.junit.Test;

/**
 * Tests the query engine using the default index implementation: the
 * {@link LuceneIndexProvider}
 */
public class LuceneIndexQueryTest extends AbstractQueryTest {

    @Override
    protected void createTestIndexNode() throws Exception {
        Tree index = root.getTree("/");
        createTestIndexNode(index, LuceneIndexConstants.TYPE_LUCENE);
        root.commit();
    }

    @Override
    protected ContentRepository createRepository() {
        return new Oak().with(new InitialContent())
            .with(new LowCostLuceneIndexProvider())
            .with(new LuceneIndexHookProvider())
            .createContentRepository();
    }

    @Test
    public void sql2() throws Exception {
        test("sql2.txt");
    }

    @Test
    @Ignore("OAK-420")
    public void sql2Measure() throws Exception {
        test("sql2_measure.txt");
    }

    @Test
    public void descendantTest() throws Exception {
        JsopUtil.apply(root, "/ + \"test\": { \"a\": {}, \"b\": {} }");
        root.commit();

        Iterator<String> result = executeQuery(
                "select * from [nt:base] where isdescendantnode('/test')",
                "JCR-SQL2").iterator();
        assertTrue(result.hasNext());
        assertEquals("/test/a", result.next());
        assertEquals("/test/b", result.next());
        assertFalse(result.hasNext());
    }

    @Test
    public void descendantTest2() throws Exception {
        JsopUtil.apply(
                root,
                "/ + \"test\": { \"a\": { \"name\": [\"Hello\", \"World\" ] }, \"b\": { \"name\" : \"Hello\" }}");
        root.commit();

        Iterator<String> result = executeQuery(
                "select * from [nt:base] where isdescendantnode('/test') and name='World'",
                "JCR-SQL2").iterator();
        assertTrue(result.hasNext());
        assertEquals("/test/a", result.next());
        assertFalse(result.hasNext());
    }

    @Test
    public void ischildnodeTest() throws Exception {
        JsopUtil.apply(
                root,
                "/ + \"parents\": { \"p0\": {\"id\": \"0\"}, \"p1\": {\"id\": \"1\"}, \"p2\": {\"id\": \"2\"}}");
        JsopUtil.apply(
                root,
                "/ + \"children\": { \"c1\": {\"p\": \"1\"}, \"c2\": {\"p\": \"1\"}, \"c3\": {\"p\": \"2\"}, \"c4\": {\"p\": \"3\"}}");
        root.commit();

        Iterator<String> result = executeQuery(
                "select * from [nt:base] as p inner join [nt:base] as p2 on ischildnode(p2, p) where p.[jcr:path] = '/'",
                "JCR-SQL2").iterator();
        assertTrue(result.hasNext());
        assertEquals("/, /children", result.next());
        assertEquals("/, /jcr:system", result.next());
        assertEquals("/, /oak:index", result.next());
        assertEquals("/, /parents", result.next());
        assertFalse(result.hasNext());
    }
}
