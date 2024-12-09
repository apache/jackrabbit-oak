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
package org.apache.jackrabbit.oak.plugins.index.lucene.invalidData;

import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.INDEX_DEFINITIONS_NAME;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.INDEX_DEFINITIONS_NODE_TYPE;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.TYPE_PROPERTY_NAME;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.MatcherAssert.assertThat;

import java.util.List;
import java.util.regex.PatternSyntaxException;

import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.oak.InitialContentHelper;
import org.apache.jackrabbit.oak.Oak;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.ContentRepository;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.junit.LogCustomizer;
import org.apache.jackrabbit.oak.plugins.index.IndexUpdate;
import org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexConstants;
import org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexEditorProvider;
import org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexProvider;
import org.apache.jackrabbit.oak.plugins.index.nodetype.NodeTypeIndexProvider;
import org.apache.jackrabbit.oak.plugins.index.property.PropertyIndexEditorProvider;
import org.apache.jackrabbit.oak.plugins.index.search.FulltextIndexConstants;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeStore;
import org.apache.jackrabbit.oak.query.AbstractQueryTest;
import org.apache.jackrabbit.oak.spi.commit.Observer;
import org.apache.jackrabbit.oak.spi.filter.PathFilter;
import org.apache.jackrabbit.oak.spi.query.QueryIndexProvider;
import org.apache.jackrabbit.oak.spi.security.OpenSecurityProvider;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.apache.lucene.codecs.Codec;
import org.hamcrest.core.IsCollectionContaining;
import org.junit.Test;

import ch.qos.logback.classic.Level;

public class InvalidIndexDefinitionTest extends AbstractQueryTest {

    private LuceneIndexEditorProvider editorProvider;

    private NodeStore nodeStore;
    
    @Override
    protected ContentRepository createRepository() {
        editorProvider = new LuceneIndexEditorProvider();
        LuceneIndexProvider provider = new LuceneIndexProvider();
        nodeStore = new MemoryNodeStore(InitialContentHelper.INITIAL_CONTENT);
        return new Oak(nodeStore)
                .with(new OpenSecurityProvider())
                .with((QueryIndexProvider) provider)
                .with((Observer) provider)
                .with(editorProvider)
                .with(new PropertyIndexEditorProvider())
                .with(new NodeTypeIndexProvider())
                .createContentRepository();
    }
    
    @Test
    public void allFine() throws CommitFailedException {
        createIndexNodeAndData();
        root.commit();
        String query = "select [jcr:path] from [nt:base] where isdescendantnode('/tmp') and upper([test]) = 'HELLO'";
        assertThat(explain(query), containsString("lucene:test"));
        assertQuery(query, List.of("/tmp/testNode"));
    }
    
    @Test(expected = IllegalArgumentException.class)
    public void invalidCodec() throws CommitFailedException {
        Tree def = createIndexNodeAndData();
        // An incorrect value throws an exception, for example:
        // java.lang.IllegalArgumentException: A SPI class of type org.apache.lucene.codecs.Codec with name 'Lucene46x' does not exist.
        String codecValue = Codec.getDefault().getName() + "x";
        def.setProperty(LuceneIndexConstants.CODEC_NAME, codecValue);
        root.commit();
    }

    @Test(expected = IllegalArgumentException.class)
    public void invalidCompatMode() throws CommitFailedException {
        Tree def = createIndexNodeAndData();
        // 3 results in IllegalArgumentException: Unknown version : 3
        def.setProperty(LuceneIndexConstants.COMPAT_MODE, 3);
        root.commit();
    }
    
    @Test(expected = PatternSyntaxException.class)
    public void invalidValueRegex() throws CommitFailedException {
        Tree def = createIndexNodeAndData();
        // An incorrect value, for example "[a-z", results in
        // java.util.regex.PatternSyntaxException: Unclosed character class near index 3
        def.setProperty(LuceneIndexConstants.PROP_VALUE_REGEX, "[a-z");
        root.commit();
    }
    
    @Test(expected = PatternSyntaxException.class)
    public void invalidQueryFilterRegex() throws CommitFailedException {
        Tree def = createIndexNodeAndData();
        // An incorrect value, for example "[a-z", results in
        // java.util.regex.PatternSyntaxException: Unclosed character class near index 3
        def.setProperty(LuceneIndexConstants.PROP_QUERY_FILTER_REGEX, "[a-z");
        root.commit();
    }
    
    @Test(expected = IllegalArgumentException.class)
    public void invalidBlobSize() throws CommitFailedException {
        Tree def = createIndexNodeAndData();
        // 1L + Integer.MAX_VALUE results in IllegalArgumentException: Out of range: 2147483648
        def.setProperty("blobSize", 1L + Integer.MAX_VALUE);
        root.commit();
    }    

    @Test
    public void negativeBlobSize() throws CommitFailedException {
        Tree def = createIndexNodeAndData();
        // there's a minimum blobSize (now), so negative values are ignored
        def.setProperty("blobSize", -1);
        root.commit();
        String query = "select [jcr:path] from [nt:base] where isdescendantnode('/tmp') and upper([test]) = 'HELLO'";
        assertThat(explain(query), containsString("lucene:test"));
        assertQuery(query, List.of("/tmp/testNode"));
    }    

    @Test(expected = IllegalArgumentException.class)
    public void invalidMaxFieldLength() throws CommitFailedException {
        Tree def = createIndexNodeAndData();
        // 1L + Integer.MAX_VALUE results in IllegalArgumentException: Out of range: 2147483648
        def.setProperty(FulltextIndexConstants.MAX_FIELD_LENGTH, 1L + Integer.MAX_VALUE);
        root.commit();
    }    

    @Test
    public void invalidEvaluatePathRestriction() throws CommitFailedException {
        Tree def = createIndexNodeAndData();
        // errors here are ignored
        def.setProperty(LuceneIndexConstants.EVALUATE_PATH_RESTRICTION, "abc");
        root.commit();
        String query = "select [jcr:path] from [nt:base] where isdescendantnode('/tmp') and upper([test]) = 'HELLO'";
        assertThat(explain(query), containsString("lucene:test"));
        assertQuery(query, List.of("/tmp/testNode"));
    }

    @Test
    public void invalidIncludedPaths() throws CommitFailedException {
        // this will commit, but in oak-run it will not work:
        // ERROR o.a.j.o.p.i.search.IndexDefinition - Config error for index definition at /oak:index/... . 
        // Please correct the index definition and reindex after correction. Additional Info : No valid include provided. Includes [/tmp], Excludes [/tmp]
        // java.lang.IllegalStateException: No valid include provided. Includes [/tmp], Excludes [/tmp]
        LogCustomizer customLogs = LogCustomizer.forLogger(IndexUpdate.class.getName()).enable(Level.ERROR).create();
        Tree def = createIndexNodeAndData();
        def.setProperty(PathFilter.PROP_INCLUDED_PATHS, List.of("/tmp/testNode"), Type.STRINGS);
        def.setProperty(PathFilter.PROP_EXCLUDED_PATHS, List.of("/tmp"), Type.STRINGS);
        try {
            customLogs.starting();
            String expectedLogMessage = "Unable to get Index Editor for index at /oak:index/test . Please correct the index definition and reindex after correction. Additional Info : java.lang.IllegalStateException: No valid include provided. Includes [/tmp/testNode], Excludes [/tmp]";
            root.commit();
            assertThat(customLogs.getLogs(), IsCollectionContaining.hasItem(expectedLogMessage));
        } finally {
            customLogs.finished();
        }
        String query = "select [jcr:path] from [nt:base] where isdescendantnode('/tmp') and upper([test]) = 'HELLO'";
        assertThat(explain(query), containsString("traverse"));
        assertQuery(query, List.of("/tmp/testNode"));
    }
    
    @Test
    public void invalidPropertyBoost() throws CommitFailedException {
        // errors here are ignored (including Double.POSITIVE_INFINITY, NEGATIVE_INFINITY)
        Tree def = createIndexNodeAndData();
        Tree indexRules = def.getChild(LuceneIndexConstants.INDEX_RULES);
        Tree ntBase = indexRules.getChild("nt:base");
        Tree properties = ntBase.getChild(FulltextIndexConstants.PROP_NODE);
        Tree test = properties.getChild("test");
        test.setProperty(FulltextIndexConstants.FIELD_BOOST, Double.NEGATIVE_INFINITY);
        root.commit();
        String query = "select [jcr:path] from [nt:base] where isdescendantnode('/tmp') and upper([test]) = 'HELLO'";
        assertThat(explain(query), containsString("lucene:test"));
        assertQuery(query, List.of("/tmp/testNode"));
    }    
    
    @Test
    public void invalidPropertyFunction() throws CommitFailedException {
        // errors here are ignored (including Double.POSITIVE_INFINITY, NEGATIVE_INFINITY)
        Tree def = createIndexNodeAndData();
        Tree indexRules = def.getChild(LuceneIndexConstants.INDEX_RULES);
        Tree ntBase = indexRules.getChild("nt:base");
        Tree properties = ntBase.getChild(FulltextIndexConstants.PROP_NODE);
        Tree test = properties.getChild("test");
        test.removeProperty(FulltextIndexConstants.PROP_NAME);
        // errors here are ignored - just the index is not used then
        // ("./test" is not a supported syntax)
        test.setProperty(FulltextIndexConstants.PROP_FUNCTION, "upper([./test])");
        root.commit();
        String query = "select [jcr:path] from [nt:base] where isdescendantnode('/tmp') and upper([./test]) = 'HELLO'";
        assertThat(explain(query), containsString("traverse"));
        assertQuery(query, List.of("/tmp/testNode"));
    }    
    
    Tree createIndexNodeAndData() throws CommitFailedException {
        Tree tmp = root.getTree("/").addChild("tmp");
        tmp.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);
        Tree testNode = tmp.addChild("testNode");
        testNode.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);
        testNode.setProperty("test", "hello");
        root.commit();
        
        Tree index = root.getTree("/");
        Tree def = index.addChild(INDEX_DEFINITIONS_NAME).addChild("test");
        def.setProperty(JcrConstants.JCR_PRIMARYTYPE, INDEX_DEFINITIONS_NODE_TYPE, Type.NAME);
        
        def.setProperty(TYPE_PROPERTY_NAME, LuceneIndexConstants.TYPE_LUCENE);
        
        // we don't set it now to speed up testing
        // def.setProperty(IndexConstants.ASYNC_PROPERTY_NAME, "async");
        
        Tree indexRules = def.addChild(LuceneIndexConstants.INDEX_RULES);
        indexRules.setProperty(JcrConstants.JCR_PRIMARYTYPE, JcrConstants.NT_UNSTRUCTURED, Type.NAME);

        // errors here are ignored
        Tree ntBase = indexRules.addChild("nt:base");
        ntBase.setProperty(JcrConstants.JCR_PRIMARYTYPE, JcrConstants.NT_UNSTRUCTURED, Type.NAME);
        Tree properties = ntBase.addChild(FulltextIndexConstants.PROP_NODE);
        properties.setProperty(JcrConstants.JCR_PRIMARYTYPE, JcrConstants.NT_UNSTRUCTURED, Type.NAME);
        
        Tree test = properties.addChild("test");
        test.setProperty(JcrConstants.JCR_PRIMARYTYPE, JcrConstants.NT_UNSTRUCTURED, Type.NAME);

        // use a function index
        test.setProperty(FulltextIndexConstants.PROP_FUNCTION, "upper([test])");
        
        // errors here are ignored
        test.setProperty(FulltextIndexConstants.PROP_ORDERED, true);
        
        // errors here are ignored
        test.setProperty(FulltextIndexConstants.PROP_PROPERTY_INDEX, true);
        
        // errors here are ignored
        test.setProperty(FulltextIndexConstants.PROP_NOT_NULL_CHECK_ENABLED, true);

        // errors here are ignored
        test.setProperty(FulltextIndexConstants.PROP_NULL_CHECK_ENABLED, true);

        return def;
    }
    
    protected String explain(String query){
        String explain = "explain " + query;
        return executeQuery(explain, "JCR-SQL2").get(0);
    }

}
