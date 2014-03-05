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

import static com.google.common.collect.Lists.newArrayList;
import static org.apache.jackrabbit.JcrConstants.JCR_CONTENT;
import static org.apache.jackrabbit.JcrConstants.JCR_PRIMARYTYPE;
import static org.apache.jackrabbit.JcrConstants.NT_FILE;
import static org.apache.jackrabbit.JcrConstants.NT_FOLDER;
import static org.apache.jackrabbit.JcrConstants.JCR_DATA;

import java.util.ArrayList;
import java.util.Calendar;

import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.oak.Oak;
import org.apache.jackrabbit.oak.api.ContentRepository;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.index.aggregate.AggregateIndexProvider;
import org.apache.jackrabbit.oak.plugins.index.aggregate.NodeAggregator;
import org.apache.jackrabbit.oak.plugins.index.aggregate.SimpleNodeAggregator;
import static org.apache.jackrabbit.oak.plugins.memory.BinaryPropertyState.binaryProperty;
import org.apache.jackrabbit.oak.plugins.nodetype.write.InitialContent;
import org.apache.jackrabbit.oak.query.AbstractQueryTest;
import org.apache.jackrabbit.oak.spi.security.OpenSecurityProvider;
import org.junit.Ignore;
import org.junit.Test;

import com.google.common.collect.ImmutableList;

public class LuceneIndexAggregationTest extends AbstractQueryTest {

    @Override
    protected void createTestIndexNode() throws Exception {
        Tree index = root.getTree("/");
        createTestIndexNode(index, LuceneIndexConstants.TYPE_LUCENE);
        root.commit();
    }

    @Override
    protected ContentRepository createRepository() {
        return new Oak()
                .with(new InitialContent())
                .with(new OpenSecurityProvider())
                .with(AggregateIndexProvider
                        .wrap(new LowCostLuceneIndexProvider()
                                .with(getNodeAggregator())))
                .with(new LuceneIndexEditorProvider())
                .createContentRepository();
    }

    /**
     * <code>
     * <aggregate primaryType="nt:file"> 
     *   <include>jcr:content</include>
     *   <include>jcr:content/*</include>
     *   <include-property>jcr:content/jcr:lastModified</include-property>
     * </aggregate>
     * <code>
     * 
     */
    private static NodeAggregator getNodeAggregator() {
        return new SimpleNodeAggregator().newRuleWithName(NT_FILE,
                newArrayList(JCR_CONTENT, JCR_CONTENT + "/*")).newRuleWithName(
                NT_FOLDER, newArrayList("myFile"));
    }

    /**
     * simple index aggregation from jcr:content to nt:file
     * 
     */
    @Test
    public void testNtFileAggregate() throws Exception {

        String sqlBase = "SELECT * FROM [nt:file] as f WHERE";
        String sqlCat = sqlBase + " CONTAINS (f.*, 'cat')";
        String sqlDog = sqlBase + " CONTAINS (f.*, 'dog')";

        Tree file = root.getTree("/").addChild("myFile");
        file.setProperty(JCR_PRIMARYTYPE, NT_FILE, Type.NAME);
        Tree resource = file.addChild(JCR_CONTENT);
        resource.setProperty(JCR_PRIMARYTYPE, "nt:resource", Type.NAME);
        resource.setProperty("jcr:lastModified", Calendar.getInstance());
        resource.setProperty("jcr:encoding", "UTF-8");
        resource.setProperty("jcr:mimeType", "text/plain");
        resource.setProperty(binaryProperty(JCR_DATA,
                "the quick brown fox jumps over the lazy dog."));
        root.commit();
        assertQuery(sqlDog, ImmutableList.of("/myFile"));

        // update jcr:data
        root.getTree("/")
                .getChild("myFile")
                .getChild(JCR_CONTENT)
                .setProperty(
                        binaryProperty(JCR_DATA,
                                "the quick brown fox jumps over the lazy cat."));
        root.commit();
        assertQuery(sqlDog, new ArrayList<String>());
        assertQuery(sqlCat, ImmutableList.of("/myFile"));

        // replace jcr:content with unstructured
        root.getTree("/").getChild("myFile").getChild(JCR_CONTENT).remove();

        Tree unstrContent = root.getTree("/").getChild("myFile")
                .addChild(JCR_CONTENT);
        unstrContent.setProperty(JCR_PRIMARYTYPE, JcrConstants.NT_UNSTRUCTURED,
                Type.NAME);

        Tree foo = unstrContent.addChild("foo");
        foo.setProperty(JCR_PRIMARYTYPE, JcrConstants.NT_UNSTRUCTURED,
                Type.NAME);
        foo.setProperty("text", "the quick brown fox jumps over the lazy dog.");
        root.commit();

        assertQuery(sqlDog, ImmutableList.of("/myFile"));
        assertQuery(sqlCat, new ArrayList<String>());

        // remove foo
        root.getTree("/").getChild("myFile").getChild(JCR_CONTENT)
                .getChild("foo").remove();

        root.commit();
        assertQuery(sqlDog, new ArrayList<String>());
        assertQuery(sqlCat, new ArrayList<String>());

        // replace jcr:content again with resource
        root.getTree("/").getChild("myFile").getChild(JCR_CONTENT).remove();

        resource = root.getTree("/").getChild("myFile").addChild(JCR_CONTENT);
        resource.setProperty(JCR_PRIMARYTYPE, "nt:resource", Type.NAME);
        resource.setProperty("jcr:lastModified", Calendar.getInstance());
        resource.setProperty("jcr:encoding", "UTF-8");
        resource.setProperty("jcr:mimeType", "text/plain");
        resource.setProperty(binaryProperty(JCR_DATA,
                "the quick brown fox jumps over the lazy cat."));
        root.commit();
        assertQuery(sqlDog, new ArrayList<String>());
        assertQuery(sqlCat, ImmutableList.of("/myFile"));

    }

    @Test
    public void testChildNodeWithOr() throws Exception {
        Tree file = root.getTree("/").addChild("myFile");
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
        assertQuery(matchContentSimple, "xpath", ImmutableList.of("/myFile"));

        String matchContent = " //element(*, nt:file)[(jcr:contains(jcr:content, 'dog') or jcr:contains(jcr:content/@jcr:title, 'invalid') or jcr:contains(jcr:content/@jcr:description, 'invalid'))]";
        assertQuery(matchContent, "xpath", ImmutableList.of("/myFile"));

        String matchTitle = " //element(*, nt:file)[(jcr:contains(jcr:content, 'invalid') or jcr:contains(jcr:content/@jcr:title, 'title') or jcr:contains(jcr:content/@jcr:description, 'invalid'))]";
        assertQuery(matchTitle, "xpath", ImmutableList.of("/myFile"));

        String matchDesc = " //element(*, nt:file)[(jcr:contains(jcr:content, 'invalid') or jcr:contains(jcr:content/@jcr:title, 'invalid') or jcr:contains(jcr:content/@jcr:description, 'description'))]";
        assertQuery(matchDesc, "xpath", ImmutableList.of("/myFile"));

        String matchNone = " //element(*, nt:file)[(jcr:contains(jcr:content, 'invalid') or jcr:contains(jcr:content/@jcr:title, 'invalid') or jcr:contains(jcr:content/@jcr:description, 'invalid'))]";
        assertQuery(matchNone, "xpath", new ArrayList<String>());
    }

    @Test
    public void testChildNodeWithOrComposite() throws Exception {

        Tree folder = root.getTree("/").addChild("myFolder");
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

        String matchContentSimple = "//element(*, nt:folder)[(jcr:contains(myFile, 'dog'))]";
        assertQuery(matchContentSimple, "xpath", ImmutableList.of("/myFolder"));

        String matchContent = " //element(*, nt:folder)[(jcr:contains(myFile, 'dog') or jcr:contains(myFile/@jcr:title, 'invalid') or jcr:contains(myFile/@jcr:description, 'invalid'))]";
        assertQuery(matchContent, "xpath", ImmutableList.of("/myFolder"));

        String matchTitle = " //element(*, nt:folder)[(jcr:contains(myFile, 'invalid') or jcr:contains(myFile/@jcr:title, 'title') or jcr:contains(myFile/@jcr:description, 'invalid'))]";
        assertQuery(matchTitle, "xpath", ImmutableList.of("/myFolder"));

        String matchDesc = " //element(*, nt:folder)[(jcr:contains(myFile, 'invalid') or jcr:contains(myFile/@jcr:title, 'invalid') or jcr:contains(myFile/@jcr:description, 'description'))]";
        assertQuery(matchDesc, "xpath", ImmutableList.of("/myFolder"));

        String matchNone = " //element(*, nt:folder)[(jcr:contains(myFile, 'invalid') or jcr:contains(myFile/@jcr:title, 'invalid') or jcr:contains(myFile/@jcr:description, 'invalid'))]";
        assertQuery(matchNone, "xpath", new ArrayList<String>());

        String matchOnlyTitleOr = " //element(*, nt:folder)[(jcr:contains(myFile/@jcr:title, 'title') or jcr:contains(myFile/@jcr:title, 'unknown') )]";
        assertQuery(matchOnlyTitleOr, "xpath", ImmutableList.of("/myFolder"));
    }

    @Test
    public void testNodeTypes() throws Exception {

        Tree folder = root.getTree("/").addChild("myFolder");
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

        String matchContentSimple = "//*[( jcr:contains(., 'dog') and @jcr:primaryType = 'nt:file' )]";
        assertQuery(matchContentSimple, "xpath", ImmutableList.of("/myFolder/myFile"));

        String matchContentDouble = "//*[( jcr:contains(., 'dog') and (@jcr:primaryType = 'nt:file' or @jcr:primaryType = 'nt:folder') )]";
        assertQuery(matchContentDouble, "xpath", ImmutableList.of("/myFolder", "/myFolder/myFile"));
    }

    @Test
    public void testChildNodeProperty() throws Exception {
        Tree file = root.getTree("/").addChild("myFile");
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

        String matchChildSimple = "//*[( jcr:contains(@jcr:title, 'title') )]";
        assertQuery(matchChildSimple, "xpath", ImmutableList.of("/myFile/jcr:content"));

        String matchChildWithStar = "//*[( jcr:contains(., 'dog') and jcr:contains(@jcr:title, 'title') )]";
        assertQuery(matchChildWithStar, "xpath", ImmutableList.of("/myFile/jcr:content"));

    }


    @Test
    public void testChildNodeProperty2() throws Exception {

        Tree file = root.getTree("/").addChild("myFile");
        file.setProperty(JCR_PRIMARYTYPE, NT_FILE, Type.NAME);
        Tree resource = file.addChild(JCR_CONTENT);
        resource.setProperty(JCR_PRIMARYTYPE, "nt:resource", Type.NAME);
        resource.setProperty(binaryProperty(JCR_DATA,
                "the quick brown fox jumps over the lazy dog."));
        resource.setProperty("jcr:title", "title");
        resource.setProperty("jcr:description", "description");

        Tree file2 = root.getTree("/").addChild("myFile2");
        file2.setProperty(JCR_PRIMARYTYPE, NT_FILE, Type.NAME);
        Tree resource2 = file2.addChild(JCR_CONTENT);
        resource2.setProperty(JCR_PRIMARYTYPE, "nt:resource", Type.NAME);
        resource2.setProperty(binaryProperty(JCR_DATA,
                "the quick brown fox jumps over the lazy dog."));
        resource2.setProperty("jcr:title", "other");
        resource.setProperty("jcr:description", "title");

        root.commit();

        String matchChildSimple = "//*[( jcr:contains(jcr:content/@jcr:title, 'title') )]";
        assertQuery(matchChildSimple, "xpath", ImmutableList.of("/myFile"));

    }

    @Test
    public void testPreventDoubleAggregation() throws Exception {
        Tree file = root.getTree("/").addChild("myFile");
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
        assertQuery(matchChildSimple, "xpath",
                ImmutableList.of("/myFile"));
    }

    @Test
     @Ignore("OAK-828")
    public void testDifferentNodes() throws Exception {

        Tree folder = root.getTree("/").addChild("myFolder");
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

        assertQuery(
                "//element(*, nt:file)[jcr:contains(., 'dog')]", 
                "xpath", ImmutableList.of("/myFolder/myFile"));

        assertQuery(
                "//element(*, nt:file)[jcr:contains(., 'title')]", 
                "xpath", ImmutableList.of("/myFolder/myFile"));

        assertQuery(
                "//element(*, nt:file)[jcr:contains(., 'dog') and jcr:contains(., 'title')]", 
                "xpath", ImmutableList.of("/myFolder/myFile"));

    }

}
