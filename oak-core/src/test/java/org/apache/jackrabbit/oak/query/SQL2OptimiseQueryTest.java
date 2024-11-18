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
package org.apache.jackrabbit.oak.query;

import static java.util.Objects.requireNonNull;
import static org.apache.jackrabbit.guava.common.collect.ImmutableList.of;
import static javax.jcr.query.Query.JCR_SQL2;
import static org.apache.jackrabbit.JcrConstants.JCR_PRIMARYTYPE;
import static org.apache.jackrabbit.oak.api.Type.NAME;
import static org.apache.jackrabbit.oak.spi.nodetype.NodeTypeConstants.NT_OAK_UNSTRUCTURED;
import static org.apache.jackrabbit.oak.InitialContentHelper.INITIAL_CONTENT;
import static org.apache.jackrabbit.oak.query.QueryEngineImpl.QuerySelectionMode.ALTERNATIVE;
import static org.apache.jackrabbit.oak.query.QueryEngineImpl.QuerySelectionMode.CHEAPEST;
import static org.apache.jackrabbit.oak.query.QueryEngineImpl.QuerySelectionMode.ORIGINAL;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import java.text.ParseException;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.jcr.RepositoryException;

import org.apache.jackrabbit.oak.Oak;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.ContentRepository;
import org.apache.jackrabbit.oak.api.QueryEngine;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.namepath.impl.LocalNameMapper;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.namepath.impl.NamePathMapperImpl;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeStore;
import org.apache.jackrabbit.oak.InitialContent;
import org.apache.jackrabbit.oak.query.ast.NodeTypeInfoProvider;
import org.apache.jackrabbit.oak.spi.security.OpenSecurityProvider;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.jetbrains.annotations.NotNull;
import org.junit.Test;

/**
 * aim to cover the various aspects of Query.optimise()
 */
public class SQL2OptimiseQueryTest extends  AbstractQueryTest {
    private NodeStore store;
    private QueryEngineSettings qeSettings = new QueryEngineSettings() {
        @Override
        public boolean isSql2Optimisation() {
            return true;
        }
    };
    
    @Test
    public void limitUnionSize() throws ParseException {
        String query = "SELECT * FROM [nt:base]\n"
                + "WHERE (CONTAINS(*, '1') AND ([jcr:uuid] LIKE '2' OR [jcr:uuid] LIKE '3'))\n"
                + "  AND ((CONTAINS(*, '4') AND ([jcr:uuid] LIKE '5' OR [jcr:uuid] LIKE '6'))\n"
                + "       OR (CONTAINS(*, '0a') AND ([jcr:uuid] LIKE '0b' OR [jcr:uuid] LIKE '0c'))\n"
                + "       OR (CONTAINS(*, '1a') AND ([jcr:uuid] LIKE '1b' OR [jcr:uuid] LIKE '1c'))\n"
                + "       OR (CONTAINS(*, '2a') AND ([jcr:uuid] LIKE '2b' OR [jcr:uuid] LIKE '2c'))\n"
                + "       OR (CONTAINS(*, '3a') AND ([jcr:uuid] LIKE '3b' OR [jcr:uuid] LIKE '3c'))\n"
                + "       OR (CONTAINS(*, '4a') AND ([jcr:uuid] LIKE '4b' OR [jcr:uuid] LIKE '4c'))\n"
                + "       OR (CONTAINS(*, '5a') AND ([jcr:uuid] LIKE '5b' OR [jcr:uuid] LIKE '5c'))\n"
                + "       OR (CONTAINS(*, '6a') AND ([jcr:uuid] LIKE '6b' OR [jcr:uuid] LIKE '6c'))\n"
                + "       OR (CONTAINS(*, '7a') AND ([jcr:uuid] LIKE '7b' OR [jcr:uuid] LIKE '7c'))\n"
                + "       OR (CONTAINS(*, '8a') AND ([jcr:uuid] LIKE '8b' OR [jcr:uuid] LIKE '8c'))\n"
                + "       OR (CONTAINS(*, '9a') AND ([jcr:uuid] LIKE '9b' OR [jcr:uuid] LIKE '9c'))\n"
                + "       OR (CONTAINS(*, 'ea') AND ([jcr:uuid] LIKE 'eb' OR [jcr:uuid] LIKE 'ec')))\n"
                + "  AND ((CONTAINS(*, '10') AND ([jcr:uuid] LIKE '11' OR [jcr:uuid] LIKE '12'))\n"
                + "       OR (CONTAINS(*, '13') AND ([jcr:uuid] LIKE '14' OR [jcr:uuid] LIKE '15')))";
        SQL2Parser parser = SQL2ParserTest.createTestSQL2Parser(
                getMappings(), getNodeTypes(), qeSettings);
        Query original;
        original = parser.parse(query, false);
        assertNotNull(original);
        String alternative = original.buildAlternativeQuery().toString();
        assertEquals(60825, alternative.length());
    }

    /**
     * checks the {@code Query#optimise()} calls for the conversion from OR to UNION from a query
     * POV; ensuring that it returns always the same, expected resultset.
     * 
     * @throws RepositoryException
     * @throws CommitFailedException
     */
    @Test
    public void orToUnions() throws RepositoryException, CommitFailedException {
        Tree test, t;
        List<String> original, optimised, cheapest, expected;
        String statement;
        
        test = root.getTree("/").addChild("test");
        test.setProperty(JCR_PRIMARYTYPE, NT_OAK_UNSTRUCTURED, NAME);
        t = addChildWithProperty(test, "a", "p", "a");
        t.setProperty("p1", "a1");
        t = addChildWithProperty(test, "b", "p", "b");
        t.setProperty("p1", "b1");
        t.setProperty("p2", "a");
        t = addChildWithProperty(test, "c", "p", "c");
        t.setProperty("p3", "a");
        addChildWithProperty(test, "d", "p", "d");
        addChildWithProperty(test, "e", "p", "e");
        test = root.getTree("/").addChild("test2");
        addChildWithProperty(test, "a", "p", "a");
        root.commit();
        
        statement = String.format("SELECT * FROM [%s] WHERE p = 'a' OR p = 'b'",
            NT_OAK_UNSTRUCTURED);
        expected = of("/test/a", "/test/b", "/test2/a");
        setQuerySelectionMode(ORIGINAL);
        original = executeQuery(statement, JCR_SQL2, true);
        setQuerySelectionMode(ALTERNATIVE);
        optimised = executeQuery(statement, JCR_SQL2, true);
        setQuerySelectionMode(CHEAPEST);
        cheapest = executeQuery(statement, JCR_SQL2, true);
        assertOrToUnionResults(expected, original, optimised, cheapest);
        
        statement = String.format(
            "SELECT * FROM [%s] WHERE p = 'a' OR p = 'b' OR p = 'c' OR p = 'd' OR p = 'e' ",
            NT_OAK_UNSTRUCTURED);
        expected = of("/test/a", "/test/b", "/test/c", "/test/d", "/test/e", "/test2/a");
        setQuerySelectionMode(ORIGINAL);
        original = executeQuery(statement, JCR_SQL2, true);
        setQuerySelectionMode(ALTERNATIVE);
        optimised = executeQuery(statement, JCR_SQL2, true);
        setQuerySelectionMode(CHEAPEST);
        cheapest = executeQuery(statement, JCR_SQL2, true);
        assertOrToUnionResults(expected, original, optimised, cheapest);

        statement = String.format(
            "SELECT * FROM [%s] WHERE (p = 'a' OR p = 'b') AND (p1 = 'a1' OR p1 = 'b1')",
            NT_OAK_UNSTRUCTURED);
        expected = of("/test/a", "/test/b");
        setQuerySelectionMode(ORIGINAL);
        original = executeQuery(statement, JCR_SQL2, true);
        setQuerySelectionMode(ALTERNATIVE);
        optimised = executeQuery(statement, JCR_SQL2, true);
        setQuerySelectionMode(CHEAPEST);
        cheapest = executeQuery(statement, JCR_SQL2, true);
        assertOrToUnionResults(expected, original, optimised, cheapest);

        statement = String.format(
            "SELECT * FROM [%s] WHERE (p = 'a' AND p1 = 'a1') OR (p = 'b' AND p1 = 'b1')",
            NT_OAK_UNSTRUCTURED);
        expected = of("/test/a", "/test/b");
        setQuerySelectionMode(ORIGINAL);
        original = executeQuery(statement, JCR_SQL2, true);
        setQuerySelectionMode(ALTERNATIVE);
        optimised = executeQuery(statement, JCR_SQL2, true);
        setQuerySelectionMode(CHEAPEST);
        cheapest = executeQuery(statement, JCR_SQL2, true);
        assertOrToUnionResults(expected, original, optimised, cheapest);
        
        statement = "SELECT * FROM [oak:Unstructured] AS c "
            + "WHERE ( c.[p] = 'a' "
            + "OR c.[p2] = 'a' " 
            + "OR c.[p3] = 'a') " 
            + "AND ISDESCENDANTNODE(c, '/test') "
            + "ORDER BY added DESC";
        expected = of("/test/a", "/test/b", "/test/c");
        setQuerySelectionMode(ORIGINAL);
        original = executeQuery(statement, JCR_SQL2, true);
        setQuerySelectionMode(ALTERNATIVE);
        optimised = executeQuery(statement, JCR_SQL2, true);
        setQuerySelectionMode(CHEAPEST);
        cheapest = executeQuery(statement, JCR_SQL2, true);
        assertOrToUnionResults(expected, original, optimised, cheapest);
    }
    
    private static void assertOrToUnionResults(@NotNull List<String> expected, 
                                               @NotNull List<String> original,
                                               @NotNull List<String> optimised,
                                               @NotNull List<String> cheapest) {
        // checks that all the three list are the expected content
        assertThat(requireNonNull(original), is(requireNonNull(expected)));        
        assertThat(requireNonNull(optimised), is(expected));
        assertThat(requireNonNull(cheapest), is(expected));
        
        // check that all the three lists contains the same. Paranoid but still a fast check
        assertThat(original, is(optimised));
        assertThat(optimised, is(cheapest));
        assertThat(cheapest, is(original));
    }

    private static Tree addChildWithProperty(@NotNull Tree father, @NotNull String name,
                                             @NotNull String propName, @NotNull String propValue) {
        Tree t = requireNonNull(father).addChild(requireNonNull(name));
        t.setProperty(JCR_PRIMARYTYPE, NT_OAK_UNSTRUCTURED, NAME);
        t.setProperty(requireNonNull(propName), requireNonNull(propValue));
        return t;
    }
    
    /**
     * ensure that an optimisation is available for the provided queries.
     * 
     * @throws ParseException
     */
    @Test
    public void optimise() throws ParseException {
        SQL2Parser parser = SQL2ParserTest.createTestSQL2Parser(
                getMappings(), getNodeTypes(), qeSettings);
        String statement;
        Query original, optimised;

        statement = 
            "SELECT * FROM [nt:unstructured] AS c "
                + "WHERE "
                + "(c.[p1]='a' OR c.[p2]='b') ";
        original = parser.parse(statement, false);
        assertNotNull(original);
        optimised = original.buildAlternativeQuery();
        assertNotNull(optimised);
        assertNotSame(original, optimised);
        assertTrue(optimised instanceof UnionQueryImpl);

        statement = 
            "SELECT * FROM [nt:unstructured] AS c "
                + "WHERE "
                + "(c.[p1]='a' OR c.[p2]='b') "
                + "AND "
                + "ISDESCENDANTNODE(c, '/test') ";
        original = parser.parse(statement, false);
        assertNotNull(original);
        optimised = original.buildAlternativeQuery();
        assertNotNull(optimised);
        assertNotSame(original, optimised);
        
        statement = 
            "SELECT * FROM [nt:unstructured] AS c "
                + "WHERE "
                + "(c.[p1]='a' OR c.[p2]='b' OR c.[p3]='c') "
                + "AND "
                + "ISDESCENDANTNODE(c, '/test') ";
        original = parser.parse(statement, false);
        assertNotNull(original);
        optimised = original.buildAlternativeQuery();
        assertNotNull(optimised);
        assertNotSame(original, optimised);
    }
    
    /**
     * ensure that an optimisation is available for the provided queries.
     * 
     * @throws ParseException
     */
    @Test
    public void optimiseAndOrAnd() throws ParseException {
        optimiseAndOrAnd(
                "select * from [nt:unstructured] as [c] " + 
                "where isdescendantnode('/tmp') " + 
                "and ([a]=1 or [b]=2) and ([c]=3 or [d]=4)",
                "(isdescendantnode(c, /tmp)) and (d = 4) and (b = 2), " + 
                "(isdescendantnode(c, /tmp)) and (d = 4) and (a = 1), " + 
                "(isdescendantnode(c, /tmp)) and (c = 3) and (b = 2), " + 
                "(isdescendantnode(c, /tmp)) and (c = 3) and (a = 1)");
        optimiseAndOrAnd(
                "select * from [nt:unstructured] as [c] " + 
                "where ([a]=1 or [b]=2) and ([x]=3 or [y]=4)",
                "(y = 4) and (b = 2), " + 
                "(y = 4) and (a = 1), " + 
                "(x = 3) and (b = 2), " + 
                "(x = 3) and (a = 1)");
        optimiseAndOrAnd(
                "select * from [nt:unstructured] as [c] " + 
                "where ([a]=1 or [b]=2 or ([c]=3 and [d]=4)) and ([x]=5 or [y]=6)",
                "(y = 6) and ((c = 3) and (d = 4)), " + 
                "(y = 6) and (b = 2), " + 
                "(y = 6) and (a = 1), " + 
                "(x = 5) and ((c = 3) and (d = 4)), " + 
                "(x = 5) and (b = 2), " + 
                "(x = 5) and (a = 1)");
    }
    
    private void optimiseAndOrAnd(String statement, String expected) throws ParseException {
        SQL2Parser parser = SQL2ParserTest.createTestSQL2Parser(
                getMappings(), getNodeTypes(), qeSettings);
        Query original;
        original = parser.parse(statement, false);
        assertNotNull(original);
        String optimized = original.buildAlternativeQuery().toString();
        optimized = optimized.replaceAll("\\[", "").replaceAll("\\]","");
        optimized = optimized.replaceAll("select c.jcr:primaryType as c.jcr:primaryType ", "");
        optimized = optimized.replaceAll("from nt:unstructured as c where ", "");
        optimized = optimized.replaceAll("c\\.", "");
        optimized = optimized.replaceAll(" union ", ", ");
        assertEquals(expected,  optimized);
    }
    
    @Test
    public void optimizeKeepsQueryOptions() throws ParseException {
        SQL2Parser parser = SQL2ParserTest.createTestSQL2Parser(
                getMappings(), getNodeTypes(), qeSettings);
        Query original;
        String statement = "select * from [nt:unstructured] as [c] " + 
                "where [a]=1 or [b]=2 option(index tag x)";
        original = parser.parse(statement, false);
        assertNotNull(original);
        UnionQueryImpl alt = (UnionQueryImpl) original.buildAlternativeQuery();
        for(Query c : alt.getChildren()) {
            assertEquals("x", ((QueryImpl) c).getQueryOptions().indexTag);
        }
    }
    
    private NamePathMapper getMappings() {
        return new NamePathMapperImpl(
            new LocalNameMapper(root, QueryEngine.NO_MAPPINGS));
    }
    
    private static NodeTypeInfoProvider getNodeTypes() {
        return new NodeStateNodeTypeInfoProvider(INITIAL_CONTENT);
    }
    
    @Override
    protected ContentRepository createRepository() {
        store = new MemoryNodeStore();
        return new Oak(store)
        .with(new OpenSecurityProvider())
        .with(new InitialContent())
        .with(qeSettings)
        .createContentRepository();
    }
}
