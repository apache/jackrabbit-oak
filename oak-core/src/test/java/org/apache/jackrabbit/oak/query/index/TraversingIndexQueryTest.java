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

import static org.apache.jackrabbit.JcrConstants.JCR_PRIMARYTYPE;

import java.util.ArrayList;
import java.util.Arrays;

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

    @Test
    public void testOak1301() throws Exception {
        Tree t1 = root.getTree("/").addChild("home").addChild("users")
                .addChild("testing").addChild("socialgraph_test_user_4");
        t1.setProperty("jcr:primaryType", "rep:User");
        t1.setProperty("rep:authorizableId", "socialgraph_test_user_4");

        Tree s = t1.addChild("social");
        s.setProperty("jcr:primaryType", "sling:Folder");

        Tree r = s.addChild("relationships");
        r.setProperty("jcr:primaryType", "sling:Folder");

        Tree f = r.addChild("friend");
        f.setProperty("jcr:primaryType", "sling:Folder");

        Tree sg = f.addChild("socialgraph_test_group");
        sg.setProperty("jcr:primaryType", "nt:unstructured");
        sg.setProperty("id", "socialgraph_test_group");

        Tree t2 = root.getTree("/").addChild("home").addChild("groups")
                .addChild("testing").addChild("socialgraph_test_group");
        root.commit();

        // select [jcr:path], [jcr:score], * from [nt:base] as a where [id/*] =
        // 'socialgraph_test_group' and isdescendantnode(a, '/home') /* xpath:
        // /jcr:root/home//*[id='socialgraph_test_group'] */
        assertQuery(
                "/jcr:root/home//*[@id='socialgraph_test_group']",
                "xpath",
                ImmutableList
                        .of("/home/users/testing/socialgraph_test_user_4/social/relationships/friend/socialgraph_test_group"));

        // sql2 select c.[jcr:path] as [jcr:path], c.[jcr:score] as [jcr:score],
        // c.* from [nt:base] as a inner join [nt:base] as b on ischildnode(b,
        // a) inner join [nt:base] as c on isdescendantnode(c, b) where name(a)
        // = 'social' and isdescendantnode(a, '/home') and name(b) =
        // 'relationships' and c.[id/*] = 'socialgraph_test_group' /* xpath:
        // /jcr:root/home//social/relationships//*[id='socialgraph_test_group']
        // */
        assertQuery(
                "/jcr:root/home//social/relationships//*[@id='socialgraph_test_group']",
                "xpath",
                ImmutableList
                        .of("/home/users/testing/socialgraph_test_user_4/social/relationships/friend/socialgraph_test_group"));

    }

    @Test
    public void testRelativeProperties() throws Exception {
        root.getTree("/").addChild("content").addChild("node1")
                .setProperty("prop", 128);
        root.commit();

        assertQuery("//*[(@prop > 1)]", "xpath",
                ImmutableList.of("/content/node1"));
        assertQuery("//*[(@prop > 2)]", "xpath",
                ImmutableList.of("/content/node1"));
        assertQuery("//*[(@prop > 20)]", "xpath",
                ImmutableList.of("/content/node1"));
        assertQuery("//*[(@prop > 100)]", "xpath",
                ImmutableList.of("/content/node1"));
        assertQuery("//*[(@prop > 200)]", "xpath", new ArrayList<String>());
        assertQuery("//*[(@prop > 1000)]", "xpath", new ArrayList<String>());

        assertQuery("//*[(*/@prop > 1)]", "xpath", ImmutableList.of("/content"));
        assertQuery("//*[(*/@prop > 2)]", "xpath", ImmutableList.of("/content"));
        assertQuery("//*[(*/@prop > 20)]", "xpath",
                ImmutableList.of("/content"));
        assertQuery("//*[(*/@prop > 100)]", "xpath",
                ImmutableList.of("/content"));
        assertQuery("//*[(*/@prop > 200)]", "xpath", new ArrayList<String>());
        assertQuery("//*[(*/@prop > 1000)]", "xpath", new ArrayList<String>());
    }

    @Test
    public void testRelativeProperties2() throws Exception {
        Tree t = root.getTree("/").addChild("content").addChild("nodes");
        Tree a = t.addChild("a");
        a.setProperty(JCR_PRIMARYTYPE, "nt:unstructured", Type.NAME);
        Tree b = a.addChild("b");
        b.setProperty(JCR_PRIMARYTYPE, "nt:unstructured", Type.NAME);
        Tree c = b.addChild("c");
        c.setProperty(JCR_PRIMARYTYPE, "nt:unstructured", Type.NAME);

        Tree d1 = c.addChild("d1");
        d1.setProperty("prop", 10);
        d1.setProperty(JCR_PRIMARYTYPE, "nt:unstructured", Type.NAME);
        Tree d2 = c.addChild("d2");
        d2.setProperty("prop", 20);
        d2.setProperty(JCR_PRIMARYTYPE, "nt:unstructured", Type.NAME);
        Tree d3 = c.addChild("d3");
        d3.setProperty("prop", 30);
        d3.setProperty(JCR_PRIMARYTYPE, "nt:unstructured", Type.NAME);
        root.commit();

        assertQuery("/jcr:root/content/nodes//*[(*/*/*/@prop >= 9)]", "xpath",
                ImmutableList.of("/content/nodes/a", "/content/nodes/a/b",
                        "/content/nodes/a/b/c"));

        assertQuery(
                "/jcr:root/content/nodes//element(*, nt:unstructured)[(*/*/*/@prop >= 9)]",
                "xpath", ImmutableList.of("/content/nodes/a",
                        "/content/nodes/a/b", "/content/nodes/a/b/c"));

        assertQuery("/jcr:root/content/nodes//*[(*/*/*/@prop >= 10)]", "xpath",
                ImmutableList.of("/content/nodes/a", "/content/nodes/a/b",
                        "/content/nodes/a/b/c"));
        assertQuery(
                "/jcr:root/content/nodes//element(*, nt:unstructured)[(*/*/*/@prop >= 10)]",
                "xpath", ImmutableList.of("/content/nodes/a",
                        "/content/nodes/a/b", "/content/nodes/a/b/c"));

        assertQuery("/jcr:root/content/nodes//*[(*/*/*/@prop >= 15)]", "xpath",
                ImmutableList.of("/content/nodes/a", "/content/nodes/a/b",
                        "/content/nodes/a/b/c"));
        assertQuery(
                "/jcr:root/content/nodes//element(*, nt:unstructured)[(*/*/*/@prop >= 15)]",
                "xpath", ImmutableList.of("/content/nodes/a",
                        "/content/nodes/a/b", "/content/nodes/a/b/c"));

        assertQuery("/jcr:root/content/nodes//*[(*/*/*/@prop >= 20)]", "xpath",
                ImmutableList.of("/content/nodes/a", "/content/nodes/a/b",
                        "/content/nodes/a/b/c"));
        assertQuery(
                "/jcr:root/content/nodes//element(*, nt:unstructured)[(*/*/*/@prop >= 20)]",
                "xpath", ImmutableList.of("/content/nodes/a",
                        "/content/nodes/a/b", "/content/nodes/a/b/c"));

        assertQuery("/jcr:root/content/nodes//*[(*/*/*/@prop >= 30)]", "xpath",
                ImmutableList.of("/content/nodes/a", "/content/nodes/a/b",
                        "/content/nodes/a/b/c"));
        assertQuery(
                "/jcr:root/content/nodes//element(*, nt:unstructured)[(*/*/*/@prop >= 30)]",
                "xpath", ImmutableList.of("/content/nodes/a",
                        "/content/nodes/a/b", "/content/nodes/a/b/c"));
    }

    @Test
    public void testMultipleRelativeProperties() throws Exception {
        Tree content = root.getTree("/").addChild("content");

        content.addChild("node1").setProperty("a", 128);
        content.addChild("node2").setProperty("a", "abc");
        content.addChild("node3").setProperty("a", "1280");

        content.addChild("node1").setProperty("b", 128);
        content.addChild("node2").setProperty("b", 1024);
        content.addChild("node3").setProperty("b", 2048);

        content.addChild("node1").setProperty("c", 10.3);
        content.addChild("node2").setProperty("c", -10.3);
        content.addChild("node3").setProperty("c", 9.8);

        content.addChild("node1").setProperty("d", Arrays.asList("x", "y"), Type.STRINGS);
        content.addChild("node2").setProperty("d", 10);
        content.addChild("node3").setProperty("d", Arrays.asList(1L, 2L), Type.LONGS);

        root.commit();

        assertQuery("//*[*/@a > 2]", "xpath", Arrays.asList("/content"));
        assertQuery("//*[*/@a > '1']", "xpath", Arrays.asList("/content"));
        assertQuery("//*[*/@a > 'abd']", "xpath", Arrays.<String>asList());
        assertQuery("//*[*/@a = 'abc']", "xpath", Arrays.asList("/content"));
        // this may be unexpected: it is evaluated as
        // ['128', 'abc', '1280'] >= 'abc'
        assertQuery("//*[*/@a >= 'abc']", "xpath", Arrays.asList("/content"));

        assertQuery("//*[*/@b > 2]", "xpath", Arrays.asList("/content"));
        assertQuery("//*[*/@b > 2048]", "xpath", Arrays.<String>asList());
        assertQuery("//*[*/@b > '1']", "xpath", Arrays.asList("/content"));
        assertQuery("//*[*/@b = 128]", "xpath", Arrays.asList("/content"));

        assertQuery("//*[*/@c > 10]", "xpath", Arrays.asList("/content"));
        assertQuery("//*[*/@c > 11]", "xpath", Arrays.<String>asList());
        assertQuery("//*[*/@c > '1']", "xpath", Arrays.asList("/content"));
        assertQuery("//*[*/@c = 9.8]", "xpath", Arrays.asList("/content"));

        assertQuery("//*[*/@d > 10]", "xpath", Arrays.asList("/content"));
        assertQuery("//*[*/@d > 11]", "xpath", Arrays.asList("/content"));
        assertQuery("//*[*/@d > '1']", "xpath", Arrays.asList("/content"));
        assertQuery("//*[*/@d = 10]", "xpath", Arrays.asList("/content"));
        // this may be unexpected: it is evaluated as
        // ['x', 'y', '10', '1', '2'] < '3'
        assertQuery("//*[*/@d < 3]", "xpath", Arrays.asList("/content"));
    }

}
