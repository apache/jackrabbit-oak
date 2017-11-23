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
package org.apache.jackrabbit.oak.query;

import static org.apache.jackrabbit.oak.InitialContent.INITIAL_CONTENT;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import java.text.ParseException;
import java.util.Iterator;

import org.apache.jackrabbit.oak.api.Result;
import org.apache.jackrabbit.oak.api.ResultRow;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.core.ImmutableRoot;
import org.junit.Test;

public class NativeQueryTest {

    private final ImmutableRoot ROOT = new ImmutableRoot(INITIAL_CONTENT);
    private final QueryEngineImpl QUERY_ENGINE = (QueryEngineImpl)ROOT.getQueryEngine();

    private final SQL2Parser p = SQL2ParserTest.createTestSQL2Parser();

    
    @Test
    public void dontTraverseForSuggest() throws Exception {
        String sql = "select [rep:suggest()] from [nt:base] where suggest('test')";
        assertDontTraverseFor(sql);
    }

    @Test
    public void dontTraverseForSpellcheck() throws Exception {
        String sql = "select [rep:spellcheck()] from [nt:base] where spellcheck('test')";
        assertDontTraverseFor(sql);
    }

    @Test
    public void dontTraverseForNative() throws Exception {
        String sql = "select [jcr:path] from [nt:base] where native('solr', 'name:(Hello OR World)')";
        assertDontTraverseFor(sql);
    }

    @Test
    public void dontTraverseForSimilar() throws Exception {
        String sql = "select [rep:similar()] from [nt:base] where similar(., '/test/a')";
        assertDontTraverseFor(sql);
    }

    private void assertDontTraverseFor(String sql) throws ParseException {
        QueryImpl query = (QueryImpl)p.parse(sql);
        query.setExecutionContext(QUERY_ENGINE.getExecutionContext());
        Result result = query.executeQuery();
        Iterator<? extends ResultRow> it = result.getRows().iterator();
        assertFalse("Zero results expected", it.hasNext());

        query = (QueryImpl)p.parse("measure " + sql);
        query.setExecutionContext(QUERY_ENGINE.getExecutionContext());
        result = query.executeQuery();
        it = result.getRows().iterator();
        while(it.hasNext()) {
            ResultRow row = it.next();
            String selector = row.getValue("selector").getValue(Type.STRING);
            if ("nt:base".equals(selector)) {
                long scanCount = row.getValue("scanCount").getValue(Type.LONG);
                // we expect that no was scanned that's it
                // - no traversal of the whole respository
                assertEquals("Repository's scan count doesn't match", 0, scanCount);
            }
        }
    }
}