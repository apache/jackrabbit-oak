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
package org.apache.jackrabbit.oak.plugins.index;

import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.index.search.FulltextIndexConstants;
import org.apache.jackrabbit.oak.query.AbstractQueryTest;
import org.junit.Test;

import java.util.Calendar;
import java.util.List;

import static org.apache.jackrabbit.JcrConstants.JCR_CONTENT;
import static org.apache.jackrabbit.JcrConstants.JCR_DATA;
import static org.apache.jackrabbit.JcrConstants.JCR_PRIMARYTYPE;
import static org.apache.jackrabbit.JcrConstants.NT_FILE;
import static org.apache.jackrabbit.JcrConstants.NT_FOLDER;
import static org.apache.jackrabbit.oak.plugins.memory.BinaryPropertyState.binaryProperty;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public abstract class IndexAggregationCommonTest extends AbstractQueryTest {
    protected IndexOptions indexOptions;
    protected TestRepository repositoryOptionsUtil;

    @Override
    protected void createTestIndexNode() throws Exception {
        Tree index = root.getTree("/");
        Tree indexDefn = createTestIndexNode(index, indexOptions.getIndexType());
        indexDefn.setProperty("includedPaths", List.of("/content"), Type.STRINGS);
        TestUtil.useV2(indexDefn);
        //Aggregates
        TestUtil.newNodeAggregator(indexDefn)
                .newRuleWithName(NT_FILE, List.of(JCR_CONTENT, JCR_CONTENT + "/*"))
                .newRuleWithName(NT_FOLDER, List.of("myFile", "subfolder/subsubfolder/file"));

        //Include all properties
        Tree props = TestUtil.newRulePropTree(indexDefn, "nt:base");
        TestUtil.enableForFullText(props, FulltextIndexConstants.REGEX_ALL_PROPS, true);

        root.commit();
    }

    /**
     * simple index aggregation from jcr:content to nt:file
     */
    @Test
    public void testNtFileAggregate() throws Exception {

        String sqlBase = "SELECT * FROM [nt:file] as f WHERE";
        String sqlCat = sqlBase + " CONTAINS (f.*, 'cat')";
        String sqlDog = sqlBase + " CONTAINS (f.*, 'dog')";

        Tree content = root.getTree("/").addChild("content");
        Tree file = content.addChild("myFile");
        file.setProperty(JCR_PRIMARYTYPE, NT_FILE, Type.NAME);
        Tree resource = file.addChild(JCR_CONTENT);
        resource.setProperty(JCR_PRIMARYTYPE, "nt:resource", Type.NAME);
        resource.setProperty("jcr:lastModified", Calendar.getInstance());
        resource.setProperty("jcr:encoding", "UTF-8");
        resource.setProperty("jcr:mimeType", "text/plain");
        resource.setProperty(binaryProperty(JCR_DATA,
                "the quick brown fox jumps over the lazy dog."));
        root.commit();

        assertEventually(() -> assertQuery(sqlDog, List.of("/content/myFile")));

        // update jcr:data
        root.getTree("/content")
                .getChild("myFile")
                .getChild(JCR_CONTENT)
                .setProperty(
                        binaryProperty(JCR_DATA,
                                "the quick brown fox jumps over the lazy cat."));
        root.commit();

        assertEventually(() -> {
            assertQuery(sqlDog, List.of());
            assertQuery(sqlCat, List.of("/content/myFile"));
        });

        // replace jcr:content with unstructured
        root.getTree("/content").getChild("myFile").getChild(JCR_CONTENT).remove();

        Tree unstrContent = root.getTree("/content").getChild("myFile")
                .addChild(JCR_CONTENT);
        unstrContent.setProperty(JCR_PRIMARYTYPE, JcrConstants.NT_UNSTRUCTURED,
                Type.NAME);

        Tree foo = unstrContent.addChild("foo");
        foo.setProperty(JCR_PRIMARYTYPE, JcrConstants.NT_UNSTRUCTURED,
                Type.NAME);
        foo.setProperty("text", "the quick brown fox jumps over the lazy dog.");
        root.commit();

        assertEventually(() -> {
            assertQuery(sqlDog, List.of("/content/myFile"));
            assertQuery(sqlCat, List.of());
        });

        // remove foo
        root.getTree("/content").getChild("myFile").getChild(JCR_CONTENT)
                .getChild("foo").remove();

        root.commit();

        assertEventually(() -> {
            assertQuery(sqlDog, List.of());
            assertQuery(sqlCat, List.of());
        });

        // replace jcr:content again with resource
        root.getTree("/content").getChild("myFile").getChild(JCR_CONTENT).remove();

        resource = root.getTree("/content").getChild("myFile").addChild(JCR_CONTENT);
        resource.setProperty(JCR_PRIMARYTYPE, "nt:resource", Type.NAME);
        resource.setProperty("jcr:lastModified", Calendar.getInstance());
        resource.setProperty("jcr:encoding", "UTF-8");
        resource.setProperty("jcr:mimeType", "text/plain");
        resource.setProperty(binaryProperty(JCR_DATA,
                "the quick brown fox jumps over the lazy cat."));
        root.commit();

        assertEventually(() -> {
            assertQuery(sqlDog, List.of());
            assertQuery(sqlCat, List.of("/content/myFile"));
        });
    }

    @Test
    public void testChildNodeWithOr() throws Exception {
        Tree content = root.getTree("/").addChild("content");
        Tree file = content.addChild("myFile");
        file.setProperty(JCR_PRIMARYTYPE, NT_FILE, Type.NAME);
        Tree resource = file.addChild(JCR_CONTENT);
        resource.setProperty(JCR_PRIMARYTYPE, "nt:resource", Type.NAME);
        resource.setProperty("jcr:lastModified", Calendar.getInstance());
        resource.setProperty("jcr:encoding", "UTF-8");
        resource.setProperty("jcr:mimeType", "text/plain");
        resource.setProperty(binaryProperty(JCR_DATA,
                "the quick brown fox jumps over the lazy dog."));

        resource.setProperty("jcr:title", "title");
        resource.setProperty("jcr:description", "description");

        root.commit();

        String matchContentSimple = "//element(*, nt:file)[(jcr:contains(jcr:content, 'dog'))]";
        assertEventually(() -> {
            assertQuery(matchContentSimple, "xpath", List.of("/content/myFile"));

            String matchContent = " //element(*, nt:file)[(jcr:contains(jcr:content, 'dog') or jcr:contains(jcr:content/@jcr:title, 'invalid') or jcr:contains(jcr:content/@jcr:description, 'invalid'))]";
            assertQuery(matchContent, "xpath", List.of("/content/myFile"));

            String matchTitle = " //element(*, nt:file)[(jcr:contains(jcr:content, 'invalid') or jcr:contains(jcr:content/@jcr:title, 'title') or jcr:contains(jcr:content/@jcr:description, 'invalid'))]";
            assertQuery(matchTitle, "xpath", List.of("/content/myFile"));

            String matchDesc = " //element(*, nt:file)[(jcr:contains(jcr:content, 'invalid') or jcr:contains(jcr:content/@jcr:title, 'invalid') or jcr:contains(jcr:content/@jcr:description, 'description'))]";
            assertQuery(matchDesc, "xpath", List.of("/content/myFile"));

            String matchNone = " //element(*, nt:file)[(jcr:contains(jcr:content, 'invalid') or jcr:contains(jcr:content/@jcr:title, 'invalid') or jcr:contains(jcr:content/@jcr:description, 'invalid'))]";
            assertQuery(matchNone, "xpath", List.of());
        });
    }

    @Test
    public void testChildNodeWithOrComposite() throws Exception {

        Tree content = root.getTree("/").addChild("content");
        Tree folder = content.addChild("myFolder");
        folder.setProperty(JCR_PRIMARYTYPE, NT_FOLDER, Type.NAME);
        Tree file = folder.addChild("myFile");
        file.setProperty(JCR_PRIMARYTYPE, NT_FILE, Type.NAME);
        file.setProperty("jcr:title", "title");
        file.setProperty("jcr:description", "description");

        Tree resource = file.addChild(JCR_CONTENT);
        resource.setProperty(JCR_PRIMARYTYPE, "nt:resource", Type.NAME);
        resource.setProperty("jcr:lastModified", Calendar.getInstance());
        resource.setProperty("jcr:encoding", "UTF-8");
        resource.setProperty("jcr:mimeType", "text/plain");
        resource.setProperty(binaryProperty(JCR_DATA,
                "the quick brown fox jumps over the lazy dog."));

        root.commit();

        assertEventually(() -> {
            String matchContentSimple = "//element(*, nt:folder)[(jcr:contains(myFile, 'dog'))]";
            assertQuery(matchContentSimple, "xpath", List.of("/content/myFolder"));

            String matchContent = " //element(*, nt:folder)[(jcr:contains(myFile, 'dog') or jcr:contains(myFile/@jcr:title, 'invalid') or jcr:contains(myFile/@jcr:description, 'invalid'))]";
            assertQuery(matchContent, "xpath", List.of("/content/myFolder"));

            String matchTitle = " //element(*, nt:folder)[(jcr:contains(myFile, 'invalid') or jcr:contains(myFile/@jcr:title, 'title') or jcr:contains(myFile/@jcr:description, 'invalid'))]";
            assertQuery(matchTitle, "xpath", List.of("/content/myFolder"));

            String matchDesc = " //element(*, nt:folder)[(jcr:contains(myFile, 'invalid') or jcr:contains(myFile/@jcr:title, 'invalid') or jcr:contains(myFile/@jcr:description, 'description'))]";
            assertQuery(matchDesc, "xpath", List.of("/content/myFolder"));

            String matchNone = " //element(*, nt:folder)[(jcr:contains(myFile, 'invalid') or jcr:contains(myFile/@jcr:title, 'invalid') or jcr:contains(myFile/@jcr:description, 'invalid'))]";
            assertQuery(matchNone, "xpath", List.of());

            String matchOnlyTitleOr = " //element(*, nt:folder)[(jcr:contains(myFile/@jcr:title, 'title') or jcr:contains(myFile/@jcr:title, 'unknown') )]";
            assertQuery(matchOnlyTitleOr, "xpath", List.of("/content/myFolder"));
        });
    }

    @Test
    public void testNodeTypes() throws Exception {
        Tree content = root.getTree("/").addChild("content");
        Tree folder = content.addChild("myFolder");
        folder.setProperty(JCR_PRIMARYTYPE, NT_FOLDER, Type.NAME);
        Tree file = folder.addChild("myFile");
        file.setProperty(JCR_PRIMARYTYPE, NT_FILE, Type.NAME);
        file.setProperty("jcr:title", "title");
        file.setProperty("jcr:description", "description");

        Tree resource = file.addChild(JCR_CONTENT);
        resource.setProperty(JCR_PRIMARYTYPE, "nt:resource", Type.NAME);
        resource.setProperty("jcr:lastModified", Calendar.getInstance());
        resource.setProperty("jcr:encoding", "UTF-8");
        resource.setProperty("jcr:mimeType", "text/plain");
        resource.setProperty(binaryProperty(JCR_DATA,
                "the quick brown fox jumps over the lazy dog."));

        root.commit();

        assertEventually(() -> {
            String matchContentSimple = "//*[( jcr:contains(., 'dog') and @jcr:primaryType = 'nt:file' )]";
            assertQuery(matchContentSimple, "xpath", List.of("/content/myFolder/myFile"));

            String matchContentDouble = "//*[( jcr:contains(., 'dog') and (@jcr:primaryType = 'nt:file' or @jcr:primaryType = 'nt:folder') )]";
            assertQuery(matchContentDouble, "xpath", List.of("/content/myFolder", "/content/myFolder/myFile"));
        });
    }

    @Test
    public void testNodeTypesDeep() throws Exception {
        Tree content = root.getTree("/").addChild("content");
        Tree folder = content.addChild("myFolder");
        folder.setProperty(JCR_PRIMARYTYPE, NT_FOLDER, Type.NAME);

        Tree folder2 = folder.addChild("subfolder");
        folder2.setProperty(JCR_PRIMARYTYPE, "nt:unstructured", Type.NAME);

        Tree folder3 = folder2.addChild("subsubfolder");
        folder3.setProperty(JCR_PRIMARYTYPE, "nt:unstructured", Type.NAME);
        file(folder3, "file");

        root.commit();

        String xpath = "//element(*, nt:folder)[jcr:contains(., 'dog')]";
        assertEventually(() -> assertQuery(xpath, "xpath", List.of("/content/myFolder")));
    }

    private static void file(Tree parent, String name) {
        Tree file = parent.addChild(name);
        file.setProperty(JCR_PRIMARYTYPE, NT_FILE, Type.NAME);

        Tree resource = file.addChild(JCR_CONTENT);
        resource.setProperty(JCR_PRIMARYTYPE, "nt:resource", Type.NAME);
        resource.setProperty("jcr:lastModified", Calendar.getInstance());
        resource.setProperty("jcr:encoding", "UTF-8");
        resource.setProperty("jcr:mimeType", "text/plain");
        resource.setProperty(binaryProperty(JCR_DATA,
                "the quick brown fox jumps over the lazy dog."));
    }

    @Test
    public void testChildNodeProperty() throws Exception {
        Tree content = root.getTree("/").addChild("content");
        Tree file = content.addChild("myFile");
        file.setProperty(JCR_PRIMARYTYPE, NT_FILE, Type.NAME);
        Tree resource = file.addChild(JCR_CONTENT);
        resource.setProperty(JCR_PRIMARYTYPE, "nt:resource", Type.NAME);
        resource.setProperty("jcr:lastModified", Calendar.getInstance());
        resource.setProperty("jcr:encoding", "UTF-8");
        resource.setProperty("jcr:mimeType", "text/plain");
        resource.setProperty(binaryProperty(JCR_DATA,
                "the quick brown fox jumps over the lazy dog."));

        resource.setProperty("jcr:title", "title");
        resource.setProperty("jcr:description", "description");

        root.commit();

        assertEventually(() -> {
            String matchChildSimple = "//*[( jcr:contains(@jcr:title, 'title') )]";
            assertQuery(matchChildSimple, "xpath", List.of("/content/myFile/jcr:content"));

            String matchChildWithStar = "//*[( jcr:contains(., 'dog') and jcr:contains(@jcr:title, 'title') )]";
            assertQuery(matchChildWithStar, "xpath", List.of("/content/myFile/jcr:content"));
        });
    }

    @Test
    public void testChildNodeProperty2() throws Exception {
        Tree content = root.getTree("/").addChild("content");
        Tree file = content.addChild("myFile");
        file.setProperty(JCR_PRIMARYTYPE, NT_FILE, Type.NAME);
        Tree resource = file.addChild(JCR_CONTENT);
        resource.setProperty(JCR_PRIMARYTYPE, "nt:resource", Type.NAME);
        resource.setProperty(binaryProperty(JCR_DATA,
                "the quick brown fox jumps over the lazy dog."));
        resource.setProperty("jcr:title", "title");
        resource.setProperty("jcr:description", "description");

        Tree file2 = content.addChild("myFile2");
        file2.setProperty(JCR_PRIMARYTYPE, NT_FILE, Type.NAME);
        Tree resource2 = file2.addChild(JCR_CONTENT);
        resource2.setProperty(JCR_PRIMARYTYPE, "nt:resource", Type.NAME);
        resource2.setProperty(binaryProperty(JCR_DATA,
                "the quick brown fox jumps over the lazy dog."));
        resource2.setProperty("jcr:title", "other");
        resource.setProperty("jcr:description", "title");

        root.commit();

        String matchChildSimple = "//*[( jcr:contains(jcr:content/@jcr:title, 'title') )]";
        assertEventually(() -> assertQuery(matchChildSimple, "xpath", List.of("/content/myFile")));
    }

    @Test
    public void testPreventDoubleAggregation() throws Exception {
        Tree content = root.getTree("/").addChild("content");
        Tree file = content.addChild("myFile");
        file.setProperty(JCR_PRIMARYTYPE, NT_FILE, Type.NAME);
        file.setProperty("jcr:title", "fox");

        Tree resource = file.addChild(JCR_CONTENT);
        resource.setProperty(JCR_PRIMARYTYPE, "nt:resource", Type.NAME);
        resource.setProperty("jcr:lastModified", Calendar.getInstance());
        resource.setProperty("jcr:encoding", "UTF-8");
        resource.setProperty("jcr:mimeType", "text/plain");
        resource.setProperty(binaryProperty(JCR_DATA,
                "the quick brown fox jumps over the lazy dog."));
        root.commit();

        String matchChildSimple = "//element(*, nt:file)[( jcr:contains(., 'fox') )]";
        assertEventually(() -> assertQuery(matchChildSimple, "xpath", List.of("/content/myFile")));
    }

    @Test
    public void testDifferentNodes() throws Exception {

        Tree content = root.getTree("/").addChild("content");
        Tree folder = content.addChild("myFolder");
        folder.setProperty(JCR_PRIMARYTYPE, NT_FOLDER, Type.NAME);
        Tree file = folder.addChild("myFile");
        file.setProperty(JCR_PRIMARYTYPE, NT_FILE, Type.NAME);
        file.setProperty("jcr:title", "title");
        file.setProperty("jcr:description", "description");

        Tree resource = file.addChild(JCR_CONTENT);
        resource.setProperty(JCR_PRIMARYTYPE, "nt:resource", Type.NAME);
        resource.setProperty("jcr:lastModified", Calendar.getInstance());
        resource.setProperty("jcr:encoding", "UTF-8");
        resource.setProperty("jcr:mimeType", "text/plain");
        resource.setProperty(binaryProperty(JCR_DATA,
                "the quick brown fox jumps over the lazy dog."));

        root.commit();

        assertEventually(() -> {
            assertQuery(
                    "//element(*, nt:file)[jcr:contains(., 'dog')]",
                    "xpath", List.of("/content/myFolder/myFile"));

            assertQuery(
                    "//element(*, nt:file)[jcr:contains(., 'title')]",
                    "xpath", List.of("/content/myFolder/myFile"));

            assertQuery(
                    "//element(*, nt:file)[jcr:contains(., 'dog') and jcr:contains(., 'title')]",
                    "xpath", List.of("/content/myFolder/myFile"));

            // double aggregation dupes
            assertQuery(
                    "//*[(jcr:contains(., 'dog') or jcr:contains(jcr:content, 'dog') )]",
                    "xpath", List.of("/content/myFolder", "/content/myFolder/myFile", "/content/myFolder/myFile/jcr:content"));
        });
    }

    @Test
    public void oak3371AggregateV2() throws CommitFailedException {
        oak3371();
    }

    @Test
    public void oak3371AggregateV1() throws CommitFailedException {

        Tree indexdef = root.getTree("/oak:index/" + TEST_INDEX_NAME);
        assertNotNull(indexdef);
        assertTrue(indexdef.exists());
        indexdef.setProperty(FulltextIndexConstants.COMPAT_MODE, 1L);
        indexdef.setProperty(IndexConstants.REINDEX_PROPERTY_NAME, true);
        root.commit();

        oak3371();
    }

    private void oak3371() throws CommitFailedException {
        setTraversalEnabled(false);
        Tree test, t;

        Tree content = root.getTree("/").addChild("content");
        test = content.addChild("test");
        t = test.addChild("a");
        t.setProperty(JCR_PRIMARYTYPE, NT_FOLDER, Type.NAME);
        t.setProperty("foo", "bar");
        t = test.addChild("b");
        t.setProperty(JCR_PRIMARYTYPE, NT_FOLDER, Type.NAME);
        t.setProperty("foo", "cat");
        t = test.addChild("c");
        t.setProperty(JCR_PRIMARYTYPE, NT_FOLDER, Type.NAME);
        t = test.addChild("d");
        t.setProperty(JCR_PRIMARYTYPE, NT_FOLDER, Type.NAME);
        t.setProperty("foo", "bar cat");
        root.commit();

        assertEventually(() -> {
            assertQuery(
                    "SELECT * FROM [nt:folder] WHERE ISDESCENDANTNODE('/content/test') AND CONTAINS(foo, 'bar')",
                    List.of("/content/test/a", "/content/test/d"));
            assertQuery(
                    "SELECT * FROM [nt:folder] WHERE ISDESCENDANTNODE('/content/test') AND NOT CONTAINS(foo, 'bar')",
                    List.of("/content/test/b", "/content/test/c"));
            assertQuery(
                    "SELECT * FROM [nt:folder] WHERE ISDESCENDANTNODE('/content/test') AND CONTAINS(foo, 'bar cat')",
                    List.of("/content/test/d"));
            assertQuery(
                    "SELECT * FROM [nt:folder] WHERE ISDESCENDANTNODE('/content/test') AND NOT CONTAINS(foo, 'bar cat')",
                    List.of("/content/test/c"));
        });

        setTraversalEnabled(true);
    }

    protected void assertEventually(Runnable r) {
        TestUtil.assertEventually(r,
                ((repositoryOptionsUtil.isAsync() ? repositoryOptionsUtil.defaultAsyncIndexingTimeInSeconds : 0) + 3000) * 5);
    }
}
