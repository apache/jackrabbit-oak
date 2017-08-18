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
package org.apache.jackrabbit.oak.plugins.index.property;

import static org.junit.Assert.assertTrue;

import javax.jcr.query.Query;

import org.apache.jackrabbit.oak.InitialContent;
import org.apache.jackrabbit.oak.Oak;
import org.apache.jackrabbit.oak.api.ContentRepository;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.plugins.index.nodetype.NodeTypeIndexProvider;
import org.apache.jackrabbit.oak.query.AbstractQueryTest;
import org.apache.jackrabbit.oak.spi.security.OpenSecurityProvider;
import org.apache.jackrabbit.oak.spi.whiteboard.Whiteboard;
import org.junit.Test;

public class OptionIndexTagTests extends AbstractQueryTest {
    
    Whiteboard wb;
    
    @Override
    protected ContentRepository createRepository() {
        Oak oak = new Oak().with(new InitialContent())
            .with(new OpenSecurityProvider())
            .with(new NodeTypeIndexProvider())
            .with(new PropertyIndexProvider())
            .with(new PropertyIndexEditorProvider());
        return oak.createContentRepository();
    }

    @Test
    public void optionIndexTag() throws Exception {
        // disable the counter index, so that traversal is normally not used
        // (only used if there is no index)
        Tree index = root.getTree("/oak:index/counter");
        index.remove();
        index = root.getTree("/oak:index/uuid");
        index.setProperty("tags", "x");
        root.commit();
        String statement, result;
        statement = "explain select * from [mix:versionable] where [jcr:uuid] = 1 option(index tag x)";
        result = executeQuery(statement, Query.JCR_SQL2, false, false).toString();
        assertTrue(result, result.indexOf("/* property uuid") >= 0);
        statement = "explain select * from [mix:versionable] where [jcr:uuid] = 1 option(index tag x, index name uuid)";
        result = executeQuery(statement, Query.JCR_SQL2, false, false).toString();
        assertTrue(result, result.indexOf("/* property uuid") >= 0);
        statement = "explain select * from [mix:versionable] where [jcr:uuid] = 1 option(index tag y, index name uuid)";
        result = executeQuery(statement, Query.JCR_SQL2, false, false).toString();
        assertTrue(result, result.indexOf("/* property uuid") >= 0);
        statement = "explain select * from [mix:versionable] where [jcr:uuid] = 1 option(index tag x, index name nodetype)";
        result = executeQuery(statement, Query.JCR_SQL2, false, false).toString();
        assertTrue(result, result.indexOf("/* property uuid") >= 0);
        
        statement = "explain select * from [mix:versionable] where [jcr:uuid] = 1 option(index tag y, index name nodetype)";
        result = executeQuery(statement, Query.JCR_SQL2, false, false).toString();
        assertTrue(result, result.indexOf("/* nodeType ") >= 0);
        statement = "explain select * from [mix:versionable] where [jcr:uuid] = 1 option(index name nodetype)";
        result = executeQuery(statement, Query.JCR_SQL2, false, false).toString();
        assertTrue(result, result.indexOf("/* nodeType ") >= 0);
        
        statement = "explain select * from [mix:versionable] where [jcr:uuid] = 1 option(index tag y)";
        result = executeQuery(statement, Query.JCR_SQL2, false, false).toString();
        assertTrue(result, result.indexOf("/* traverse ") >= 0);
    }

    @Test
    public void optionIndexName() throws Exception {
        // disable the counter index, so that traversal is never used
        Tree index = root.getTree("/oak:index/counter");
        index.remove();
        root.commit();
        String statement, result;
        statement = "explain select * from [mix:versionable] where [jcr:uuid] = 1 option(index name uuid)";
        result = executeQuery(statement, Query.JCR_SQL2, false, false).toString();
        assertTrue(result, result.indexOf("/* property uuid") >= 0);
        statement = "explain select * from [mix:versionable] where [jcr:uuid] = 1 option(index name nodetype)";
        result = executeQuery(statement, Query.JCR_SQL2, false, false).toString();
        assertTrue(result, result.indexOf("/* nodeType ") >= 0);
    }

}
