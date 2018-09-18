/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.jackrabbit.oak.spi.query;

import static junit.framework.Assert.assertEquals;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import javax.jcr.query.Query;

import org.apache.jackrabbit.oak.Oak;
import org.apache.jackrabbit.oak.api.ContentRepository;
import org.apache.jackrabbit.oak.api.ContentSession;
import org.apache.jackrabbit.oak.api.QueryEngine;
import org.apache.jackrabbit.oak.api.Result;
import org.apache.jackrabbit.oak.api.ResultRow;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.InitialContent;
import org.apache.jackrabbit.oak.spi.security.OpenSecurityProvider;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * {@code QueryTest} contains query related tests.
 */
public class QueryTest {

    private ContentRepository repository;

    @Before
    public void setUp() {
        repository = new Oak().with(new OpenSecurityProvider()).with(new InitialContent()).createContentRepository();
    }

    @After
    public void tearDown() {
        repository = null;
    }

    @Test
    public void queryOnStableRevision() throws Exception {
        ContentSession s = repository.login(null, null);
        Root r = s.getLatestRoot();
        Tree t = r.getTree("/").addChild("test");
        t.addChild("node1").setProperty("jcr:primaryType", "nt:base");
        t.addChild("node2").setProperty("jcr:primaryType", "nt:base");
        t.addChild("node3").setProperty("jcr:primaryType", "nt:base");
        r.commit();

        ContentSession s2 = repository.login(null, null);
        Root r2 = s2.getLatestRoot();

        r.getTree("/test").getChild("node2").remove();
        r.commit();

        Result result = r2.getQueryEngine().executeQuery(
                "test//element(*, nt:base)", Query.XPATH,
                QueryEngine.NO_BINDINGS, QueryEngine.NO_MAPPINGS);
        Set<String> paths = new HashSet<String>();
        for (ResultRow rr : result.getRows()) {
            paths.add(rr.getPath());
        }
        assertEquals(new HashSet<String>(Arrays.asList("/test/node1", "/test/node2", "/test/node3")), paths);
    }

}
