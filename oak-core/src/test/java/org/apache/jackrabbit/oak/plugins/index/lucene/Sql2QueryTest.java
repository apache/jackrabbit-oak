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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Iterator;

import org.apache.jackrabbit.oak.api.ResultRow;
import org.apache.jackrabbit.oak.api.Tree;
import org.junit.Test;

public class Sql2QueryTest extends AbstractLuceneQueryTest {

    @Test
    public void simpleSql2() throws Exception {

        Tree test = root.getTree("/").addChild("test");
        test.addChild("a").setProperty("name", vf.createValue("hello"));
        test.addChild("b").setProperty("name", vf.createValue("nothello"));
        root.commit();

        String sql = "select * from [nt:base] where name = 'hello'";

        Iterator<? extends ResultRow> result;
        result = executeQuery(sql).getRows().iterator();
        assertTrue(result.hasNext());
        assertEquals("/test/a", result.next().getPath());
        assertFalse(result.hasNext());
    }

}
