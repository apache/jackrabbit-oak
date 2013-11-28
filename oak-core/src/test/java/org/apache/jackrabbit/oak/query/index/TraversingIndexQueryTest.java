/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0 Unless required by applicable law
 * or agreed to in writing, software distributed under the License is
 * distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language
 * governing permissions and limitations under the License.
 */
package org.apache.jackrabbit.oak.query.index;

import org.apache.jackrabbit.oak.Oak;
import org.apache.jackrabbit.oak.api.ContentRepository;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.nodetype.write.InitialContent;
import org.apache.jackrabbit.oak.query.AbstractQueryTest;
import org.apache.jackrabbit.oak.spi.security.OpenSecurityProvider;
import org.junit.Test;

import com.google.common.collect.ImmutableList;

/**
 * Tests the query engine using the default index implementation: the
 * {@link TraversingIndex}
 */
public class TraversingIndexQueryTest extends AbstractQueryTest {

    @Override
    protected ContentRepository createRepository() {
        return new Oak()
            .with(new OpenSecurityProvider())
            .with(new InitialContent())
            .createContentRepository();
    }

    @Test
    public void sql1() throws Exception {
        test("sql1.txt");
    }

    @Test
    public void sql2() throws Exception {
        test("sql2.txt");
    }

    @Test
    public void testFullTextTerm() throws Exception {
        //OAK-1024 allow '/' in a full-text query 
        Tree node = root.getTree("/").addChild("content");
        node.setProperty("jcr:mimeType", "text/plain");
        root.commit();
        assertQuery("//*[jcr:contains(., 'text/plain')]", "xpath",
                ImmutableList.of("/content"));
    }

    @Test
    public void testFullTextTermName() throws Exception {
        Tree c = root.getTree("/").addChild("content");
        c.addChild("testFullTextTermNameSimple");
        c.addChild("testFullTextTermNameFile.txt");
        root.commit();
        assertQuery("//*[jcr:contains(., 'testFullTextTermNameSimple')]",
                "xpath",
                ImmutableList.of("/content/testFullTextTermNameSimple"));
        assertQuery("//*[jcr:contains(., 'testFullTextTermNameFile.txt')]",
                "xpath",
                ImmutableList.of("/content/testFullTextTermNameFile.txt"));
    }

    @Test
    public void testMultiNotEqual() throws Exception {
        Tree c = root.getTree("/").addChild("content");

        c.addChild("one").setProperty("prop", "value");
        c.addChild("two").setProperty("prop",
                ImmutableList.of("aaa", "value", "bbb"), Type.STRINGS);
        c.addChild("three").setProperty("prop",
                ImmutableList.of("aaa", "bbb", "ccc"), Type.STRINGS);
        root.commit();

        assertQuery("//*[@prop != 'value']", "xpath",
                ImmutableList.of("/content/two", "/content/three"));
    }

    @Test
    public void testMultiAndEquals() throws Exception {
        Tree c = root.getTree("/").addChild("content");

        c.addChild("one").setProperty("prop", "aaa");
        c.addChild("two").setProperty("prop",
                ImmutableList.of("aaa", "bbb", "ccc"), Type.STRINGS);
        c.addChild("three").setProperty("prop", ImmutableList.of("aaa", "bbb"),
                Type.STRINGS);
        root.commit();

        assertQuery("//*[(@prop = 'aaa' and @prop = 'bbb' and @prop = 'ccc')]",
                "xpath", ImmutableList.of("/content/two"));
    }

    @Test
    public void testMultiAndLike() throws Exception {
        Tree c = root.getTree("/").addChild("content");

        c.addChild("one").setProperty("prop", "aaaBoom");
        c.addChild("two").setProperty("prop",
                ImmutableList.of("aaaBoom", "bbbBoom", "cccBoom"), Type.STRINGS);
        c.addChild("three").setProperty("prop", ImmutableList.of("aaaBoom", "bbbBoom"),
                Type.STRINGS);
        root.commit();

        assertQuery("//*[(jcr:like(@prop, 'aaa%') and jcr:like(@prop, 'bbb%') and jcr:like(@prop, 'ccc%'))]",
                "xpath", ImmutableList.of("/content/two"));
    }

    @Test
    public void testSubPropertyMultiAndEquals() throws Exception {
        Tree c = root.getTree("/").addChild("content");

        c.addChild("one").addChild("child").setProperty("prop", "aaa");
        c.addChild("two")
                .addChild("child")
                .setProperty("prop", ImmutableList.of("aaa", "bbb", "ccc"),
                        Type.STRINGS);
        c.addChild("three")
                .addChild("child")
                .setProperty("prop", ImmutableList.of("aaa", "bbb"),
                        Type.STRINGS);
        root.commit();

        assertQuery(
                "//*[(child/@prop = 'aaa' and child/@prop = 'bbb' and child/@prop = 'ccc')]",
                "xpath", ImmutableList.of("/content/two"));
    }

    @Test
    public void testSubPropertyMultiAndLike() throws Exception {
        Tree c = root.getTree("/").addChild("content");

        c.addChild("one").addChild("child").setProperty("prop", "aaaBoom");
        c.addChild("two")
                .addChild("child")
                .setProperty("prop", ImmutableList.of("aaaBoom", "bbbBoom", "cccBoom"),
                        Type.STRINGS);
        c.addChild("three")
                .addChild("child")
                .setProperty("prop", ImmutableList.of("aaaBoom", "bbbBoom"),
                        Type.STRINGS);
        root.commit();

        assertQuery(
                "//*[(jcr:like(child/@prop, 'aaa%') and jcr:like(child/@prop, 'bbb%') and jcr:like(child/@prop, 'ccc%'))]",
                "xpath", ImmutableList.of("/content/two"));
    }
}
