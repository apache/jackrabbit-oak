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

package org.apache.jackrabbit.oak.query;

import com.google.common.collect.ImmutableList;
import org.apache.jackrabbit.oak.InitialContent;
import org.apache.jackrabbit.oak.Oak;
import org.apache.jackrabbit.oak.api.ContentRepository;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.spi.security.OpenSecurityProvider;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

public class PropertyInexistenceTest extends AbstractQueryTest {
    @Override
    protected ContentRepository createRepository() {
        return new Oak()
                .with(new OpenSecurityProvider())
                .with(new InitialContent())
                .createContentRepository();
    }

    private String initVal = null;

    @Before
    public void setup() {
        initVal = System.getProperty("oak.useOldInexistenceCheck");
    }

    @After
    public void tearDown() {
        if (initVal == null) {
            System.clearProperty("oak.useOldInexistenceCheck");
        } else {
            System.setProperty("oak.useOldInexistenceCheck", initVal);
        }
    }

    @Test
    public void inexistence() throws Exception {
        Tree rootTree = root.getTree("/").addChild("a");

        rootTree.addChild("x").addChild("y");
        root.commit();

        String query1 = "SELECT * FROM [nt:base] WHERE ISDESCENDANTNODE('/a') AND [z] IS NULL";
        List<String> expected1 = ImmutableList.of("/a/x", "/a/x/y");

        String query2 = "SELECT * FROM [nt:base] WHERE ISDESCENDANTNODE('/a/x') AND [z] IS NULL";
        List<String> expected2 = ImmutableList.of("/a/x/y");

        assertQuery(query1, expected1);
        assertQuery(query2, expected2);

        // old behavior remains same as new for non-relative constraints
        System.setProperty("oak.useOldInexistenceCheck", "true");

        assertQuery(query1, expected1);
        assertQuery(query2, expected2);
    }

    @Test
    public void relativeInexistence() throws Exception {
        Tree rootTree = root.getTree("/").addChild("a");

        rootTree.addChild("x").addChild("y");
        rootTree.addChild("x1");
        root.commit();

        String query1 = "SELECT * FROM [nt:base] WHERE ISDESCENDANTNODE('/a') AND [y/z] IS NULL";
        List<String> expected1 = ImmutableList.of("/a/x", "/a/x/y", "/a/x1");
        List<String> expectedOld1 = ImmutableList.of("/a/x");

        String query2 = "SELECT * FROM [nt:base] WHERE ISDESCENDANTNODE('/a/x') AND [y/z] IS NULL";
        List<String> expected2 = ImmutableList.of("/a/x/y");
        List<String> expectedOld2 = ImmutableList.of();

        assertQuery(query1, expected1);
        assertQuery(query2, expected2);

        // old behavior for relative constraints differs from new
        System.setProperty("oak.useOldInexistenceCheck", "true");

        assertQuery(query1, expectedOld1);
        assertQuery(query2, expectedOld2);


        rootTree.addChild("x2").addChild("z").setProperty("y", "bar");
        rootTree.addChild("x2").addChild("z1").addChild("y").setProperty("z", "bar");
        root.commit();

        System.setProperty("oak.useOldInexistenceCheck", "false");

        String query3 = "SELECT * FROM [nt:base] WHERE ISDESCENDANTNODE('/a/x2') AND [y/z] IS NULL";
        List<String> expected3 = ImmutableList.of("/a/x2/z", "/a/x2/z1/y");
        List<String> expectedOld3 = ImmutableList.of();

        assertQuery(query3, expected3);

        System.setProperty("oak.useOldInexistenceCheck", "true");
        assertQuery(query3, expectedOld3);
    }

    @Test
    public void deeperRelativeInexistence() throws Exception {
        Tree rootTree = root.getTree("/").addChild("a");

        rootTree.addChild("x");
        rootTree.addChild("x1").addChild("w");
        rootTree.addChild("x2").addChild("w").addChild("y");
        root.commit();

        String query = "SELECT * FROM [nt:base] WHERE ISDESCENDANTNODE('/a') AND [w/y/z] IS NULL";
        List<String> expected = ImmutableList.of("/a/x", "/a/x1", "/a/x1/w", "/a/x2", "/a/x2/w", "/a/x2/w/y");
        List<String> expectedOld = ImmutableList.of("/a/x2");

        assertQuery(query, expected);

        System.setProperty("oak.useOldInexistenceCheck", "true");
        assertQuery(query, expectedOld);
    }
}
