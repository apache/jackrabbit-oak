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
package org.apache.jackrabbit.oak.plugins.index.nodetype;

import static com.google.common.collect.ImmutableList.of;
import static org.apache.jackrabbit.JcrConstants.JCR_MIXINTYPES;
import static org.apache.jackrabbit.JcrConstants.JCR_PRIMARYTYPE;
import static org.apache.jackrabbit.oak.plugins.index.IndexUtils.createIndexDefinition;
import static org.apache.jackrabbit.oak.spi.nodetype.NodeTypeConstants.NT_UNSTRUCTURED;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;

import javax.jcr.query.Query;

import org.apache.jackrabbit.oak.Oak;
import org.apache.jackrabbit.oak.api.ContentRepository;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.index.property.PropertyIndexEditorProvider;
import org.apache.jackrabbit.oak.InitialContent;
import org.apache.jackrabbit.oak.query.AbstractQueryTest;
import org.apache.jackrabbit.oak.spi.security.OpenSecurityProvider;
import org.junit.Test;

/**
 * Tests the node type index implementation.
 */
public class NodeTypeIndexQueryTest extends AbstractQueryTest {

    @Override
    protected ContentRepository createRepository() {
        return new Oak().with(new InitialContent())
                .with(new OpenSecurityProvider())
                .with(new NodeTypeIndexProvider())
                .with(new PropertyIndexEditorProvider())
                .createContentRepository();
    }

    private static Tree child(Tree t, String n, String type) {
        Tree t1 = t.addChild(n);
        t1.setProperty(JCR_PRIMARYTYPE, type, Type.NAME);
        return t1;
    }

    private static void mixLanguage(Tree t, String n) {
        Tree c = t.addChild(n);
        c.setProperty(JCR_PRIMARYTYPE, "nt:unstructured", Type.NAME);
        c.setProperty(JCR_MIXINTYPES, of("mix:language"), Type.NAMES);
    }

    @Test
    public void query() throws Exception {
        setTraversalEnabled(false);

        Tree t = root.getTree("/");
        child(t, "a", "nt:unstructured");
        child(t, "b", "nt:unstructured");
        child(t, "c", "nt:folder");
        child(t, "d", "nt:folder");
        mixLanguage(t, "e");
        mixLanguage(t, "f");

        Tree n = root.getTree("/oak:index");
        createIndexDefinition(n, "nodetype", false, new String[] {
                JCR_PRIMARYTYPE, JCR_MIXINTYPES }, new String[] { "nt:folder",
                "mix:language" });

        root.commit();

        assertQuery("select [jcr:path] from [nt:unstructured] ",
                new ArrayList<String>());
        assertQuery("select [jcr:path] from [nt:folder] ", of("/c", "/d"));
        assertQuery("select [jcr:path] from [mix:language] ", of("/e", "/f"));

        setTraversalEnabled(true);
    }
    
    @Test
    public void oak3371() throws Exception {
        setTraversalEnabled(false);
        Tree t, t1;

        Tree n = root.getTree("/oak:index");
        createIndexDefinition(n, "nodeType", false, new String[] {
                JCR_PRIMARYTYPE }, new String[] { NT_UNSTRUCTURED });

        root.commit();
        
        t = root.getTree("/");
        t = child(t, "test", NT_UNSTRUCTURED);
        t1 = child(t, "a", NT_UNSTRUCTURED);
        t1.setProperty("foo", "bar");
        child(t, "b", NT_UNSTRUCTURED);
        
        root.commit();
        
        List<String> plan;

        plan = executeQuery(
                "explain SELECT * FROM [nt:unstructured] " + 
                "WHERE ISDESCENDANTNODE([/test]) " + 
                "AND CONTAINS(foo, 'bar')", 
                Query.JCR_SQL2, false);
        assertEquals(1, plan.size());
        assertTrue(plan.get(0).contains("no-index"));
        assertEquals("[nt:unstructured] as [nt:unstructured] /* no-index\n" +
                "  where (isdescendantnode([nt:unstructured], [/test]))\n" +
                "  and (contains([nt:unstructured].[foo], 'bar')) */", 
                plan.get(0));

        plan = executeQuery(
                "explain SELECT * FROM [nt:unstructured] " + 
                "WHERE ISDESCENDANTNODE([/test]) " + 
                "AND NOT CONTAINS(foo, 'bar')", 
                Query.JCR_SQL2, false);
        assertEquals(1, plan.size());
        assertTrue(plan.get(0).contains("no-index"));
        assertEquals("[nt:unstructured] as [nt:unstructured] /* no-index\n" +
                "  where (isdescendantnode([nt:unstructured], [/test]))\n" +
                "  and (not contains([nt:unstructured].[foo], 'bar')) */", 
                plan.get(0));
        
        plan = executeQuery(
                "explain SELECT * FROM [nt:unstructured] " + 
                "WHERE ISDESCENDANTNODE([/test]) " + 
                "AND NOT NOT CONTAINS(foo, 'bar')", 
                Query.JCR_SQL2, false);
        assertEquals(1, plan.size());
        assertTrue(plan.get(0).contains("no-index"));
        assertEquals("[nt:unstructured] as [nt:unstructured] /* no-index\n" +
                "  where (isdescendantnode([nt:unstructured], [/test]))\n" +
                "  and (contains([nt:unstructured].[foo], 'bar')) */", 
                plan.get(0));
        
        setTraversalEnabled(true);
    }
}