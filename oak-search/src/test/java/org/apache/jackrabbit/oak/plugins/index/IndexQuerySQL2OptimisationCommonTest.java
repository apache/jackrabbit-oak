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

import org.apache.jackrabbit.oak.Oak;
import org.apache.jackrabbit.oak.api.ContentRepository;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.plugins.index.search.FulltextIndexConstants;
import org.apache.jackrabbit.oak.query.AbstractQueryTest;
import org.apache.jackrabbit.oak.spi.query.QueryIndexProvider;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;

import static org.apache.jackrabbit.JcrConstants.JCR_PRIMARYTYPE;
import static org.apache.jackrabbit.JcrConstants.NT_UNSTRUCTURED;
import static org.apache.jackrabbit.oak.api.Type.NAME;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public abstract class IndexQuerySQL2OptimisationCommonTest extends AbstractQueryTest {

    protected IndexOptions indexOptions;
    protected IndexEditorProvider editorProvider;
    protected QueryIndexProvider indexProvider;

    @Override
    protected ContentRepository createRepository() {
        return getOakRepo().createContentRepository();
    }

    @Override
    protected void createTestIndexNode() throws Exception {
        Tree index = root.getTree("/");
        Tree indexDefn = createTestIndexNode(index, indexOptions.getIndexType());
        TestUtil.useV2(indexDefn);
        indexDefn.setProperty(FulltextIndexConstants.EVALUATE_PATH_RESTRICTION, true);

        Tree props = TestUtil.newRulePropTree(indexDefn, "nt:base");
        props.getParent().setProperty(FulltextIndexConstants.INDEX_NODE_NAME, true);
        TestUtil.enablePropertyIndex(props, "c1/p", false);
        TestUtil.enableForFullText(props, FulltextIndexConstants.REGEX_ALL_PROPS, true);
        TestUtil.enablePropertyIndex(props, "a/name", false);
        TestUtil.enablePropertyIndex(props, "b/name", false);
        TestUtil.enableFunctionIndex(props, "length([name])");
        TestUtil.enableFunctionIndex(props, "lower([name])");
        TestUtil.enableFunctionIndex(props, "upper([name])");

        root.commit();
    }

    @Test
    public void oak2660() throws Exception {
        final String name = "name";
        final String surname = "surname";
        final String description = "description";
        final String added = "added";
        final String yes = "yes";

        Tree t;

        // re-define the lucene index
        t = root.getTree("/oak:index/" + TEST_INDEX_NAME);
        assertTrue(t.exists());
        t.remove();
        root.commit();
        assertFalse(root.getTree("/oak:index/" + TEST_INDEX_NAME).exists());

        t = root.getTree("/");
        Tree indexDefn = createTestIndexNode(t, indexOptions.getIndexType());
        TestUtil.useV2(indexDefn);

        Tree props = TestUtil.newRulePropTree(indexDefn, NT_UNSTRUCTURED);
        TestUtil.enablePropertyIndex(props, name, false);
        TestUtil.enableForFullText(props, surname, false);
        TestUtil.enableForFullText(props, description, false);
        TestUtil.enableForOrdered(props, added);

        root.commit();

        // creating the dataset
        List<String> expected = new ArrayList<>();
        Tree content = root.getTree("/").addChild("content");
        t = content.addChild("test1");
        t.setProperty(JCR_PRIMARYTYPE, NT_UNSTRUCTURED, NAME);
        t.setProperty(name, yes);
        t.setProperty(surname, yes);
        t.setProperty(description, yes);
        t.setProperty(added, Calendar.getInstance());
        expected.add(t.getPath());

        t = content.addChild("test2");
        t.setProperty(JCR_PRIMARYTYPE, NT_UNSTRUCTURED, NAME);
        t.setProperty(name, yes);
        t.setProperty(surname, yes);
        t.setProperty(description, "no");
        t.setProperty(added, Calendar.getInstance());
        expected.add(t.getPath());

        t = content.addChild("test3");
        t.setProperty(JCR_PRIMARYTYPE, NT_UNSTRUCTURED, NAME);
        t.setProperty(name, yes);
        t.setProperty(surname, "no");
        t.setProperty(description, "no");
        t.setProperty(added, Calendar.getInstance());
        expected.add(t.getPath());

        t = content.addChild("test4");
        t.setProperty(JCR_PRIMARYTYPE, NT_UNSTRUCTURED, NAME);
        t.setProperty(name, "no");
        t.setProperty(surname, yes);
        t.setProperty(description, "no");
        t.setProperty(added, Calendar.getInstance());
        expected.add(t.getPath());

        t = content.addChild("test5");
        t.setProperty(JCR_PRIMARYTYPE, NT_UNSTRUCTURED, NAME);
        t.setProperty(name, "no");
        t.setProperty(surname, "no");
        t.setProperty(description, yes);
        t.setProperty(added, Calendar.getInstance());
        expected.add(t.getPath());

        t = content.addChild("test6");
        t.setProperty(JCR_PRIMARYTYPE, NT_UNSTRUCTURED, NAME);
        t.setProperty(name, "no");
        t.setProperty(surname, "no");
        t.setProperty(description, "no");
        t.setProperty(added, Calendar.getInstance());

        root.commit();

        // asserting the initial state
        for (String s : expected) {
            assertTrue("wrong initial state", root.getTree(s).exists());
        }

        final String statement =
                "SELECT * " +
                        "FROM [" + NT_UNSTRUCTURED + "] AS c " +
                        "WHERE " +
                        "( " +
                        "c.[" + name + "] = '" + yes + "' " +
                        "OR CONTAINS(c.[" + surname + "], '" + yes + "') " +
                        "OR CONTAINS(c.[" + description + "], '" + yes + "') " +
                        ") " +
                        "AND ISDESCENDANTNODE(c, '" + content.getPath() + "') " +
                        "ORDER BY " + added + " DESC ";

        TestUtil.assertEventually(() -> assertQuery(statement, SQL2, expected), 3000 * 5);
    }

    protected abstract Oak getOakRepo();
}
