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
package org.apache.jackrabbit.oak.plugins.index.solr.configuration;

import java.io.Reader;
import java.io.StringReader;
import java.util.regex.Pattern;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.core.KeywordTokenizer;
import org.apache.lucene.analysis.miscellaneous.LengthFilter;
import org.apache.lucene.analysis.miscellaneous.RemoveDuplicatesTokenFilter;
import org.apache.lucene.analysis.path.PathHierarchyTokenizer;
import org.apache.lucene.analysis.pattern.PatternCaptureGroupTokenFilter;
import org.apache.lucene.analysis.pattern.PatternReplaceFilter;
import org.apache.lucene.analysis.reverse.ReverseStringFilter;
import org.apache.lucene.util.Version;
import org.junit.Before;
import org.junit.Test;

import static org.apache.lucene.analysis.BaseTokenStreamTestCase.assertAnalyzesTo;
import static org.apache.lucene.analysis.BaseTokenStreamTestCase.assertTokenStreamContents;

/**
 * Testcase for checking default analyzers configurations behave as expected with regards to path related restrictions
 *
 * Note that default Solr analyzers for Oak should be equivalent to the ones programmatically defined here.
 */
public class DefaultAnalyzersConfigurationTest {

    private Analyzer parentPathIndexingAnalyzer;
    private Analyzer parentPathSearchingAnalyzer;
    private Analyzer exactPathAnalyzer;
    private Analyzer directChildrenPathIndexingAnalyzer;
    private Analyzer directChildrenPathSearchingAnalyzer;
    private Analyzer allChildrenPathIndexingAnalyzer;
    private Analyzer allChildrenPathSearchingAnalyzer;

    @Before
    public void setUp() throws Exception {
        this.exactPathAnalyzer = new Analyzer() {
            @Override
            protected TokenStreamComponents createComponents(String fieldName) {
                Tokenizer source = new KeywordTokenizer();
                return new TokenStreamComponents(source);
            }
        };
        this.parentPathIndexingAnalyzer = new Analyzer() {
            @Override
            protected TokenStreamComponents createComponents(String fieldName) {
                Tokenizer source = new KeywordTokenizer();
                return new TokenStreamComponents(source);
            }
        };
        this.parentPathSearchingAnalyzer = new Analyzer() {
            @Override
            protected TokenStreamComponents createComponents(String fieldName) {
                Tokenizer source = new KeywordTokenizer();
                TokenStream filter = new ReverseStringFilter(source);
                filter = new PatternReplaceFilter(filter, Pattern.compile("[^\\/]+\\/"), "", false);
                filter = new ReverseStringFilter(filter);
                return new TokenStreamComponents(source, filter);
            }
        };

        this.directChildrenPathIndexingAnalyzer = new Analyzer() {
            @Override
            protected TokenStreamComponents createComponents(String fieldName) {
                Tokenizer source = new KeywordTokenizer();
                TokenStream filter = new ReverseStringFilter(source);
                filter = new LengthFilter(filter, 2, Integer.MAX_VALUE);
                filter = new PatternReplaceFilter(filter, Pattern.compile("([^\\/]+)(\\/)"), "$2", false);
                filter = new PatternReplaceFilter(filter, Pattern.compile("(\\/)(.+)"), "$2", false);
                filter = new ReverseStringFilter(filter);
                return new TokenStreamComponents(source, filter);
            }
        };
        this.directChildrenPathSearchingAnalyzer = new Analyzer() {
            @Override
            protected TokenStreamComponents createComponents(String fieldName) {
                Tokenizer source = new KeywordTokenizer();
                return new TokenStreamComponents(source);
            }
        };

        this.allChildrenPathIndexingAnalyzer = new Analyzer() {
            @Override
            protected TokenStreamComponents createComponents(String fieldName) {
                Tokenizer source = new PathHierarchyTokenizer();
                TokenStream filter = new PatternCaptureGroupTokenFilter(source, false, Pattern.compile("((\\/).*)"));
                filter = new RemoveDuplicatesTokenFilter(filter);
                return new TokenStreamComponents(source, filter);
            }
        };
        this.allChildrenPathSearchingAnalyzer = new Analyzer() {
            @Override
            protected TokenStreamComponents createComponents(String fieldName) {
                Tokenizer source = new KeywordTokenizer();
                return new TokenStreamComponents(source);
            }
        };
    }

    @Test
    public void testAllChildrenIndexingTokenization() throws Exception {
        try {
            TokenStream ts = allChildrenPathIndexingAnalyzer.tokenStream("text", new StringReader("/jcr:a/jcr:b/c/jcr:d"));
            assertTokenStreamContents(ts, new String[]{"/jcr:a", "/", "/jcr:a/jcr:b", "/jcr:a/jcr:b/c", "/jcr:a/jcr:b/c/jcr:d"});
        } finally {
            allChildrenPathIndexingAnalyzer.close();
        }
    }

    @Test
    public void testAllChildrenSearchingTokenization() throws Exception {
        try {
            TokenStream ts = allChildrenPathSearchingAnalyzer.tokenStream("text", new StringReader("/jcr:a/jcr:b/jcr:c"));
            assertTokenStreamContents(ts, new String[]{"/jcr:a/jcr:b/jcr:c"});
        } finally {
            allChildrenPathSearchingAnalyzer.close();
        }
    }

    @Test
    public void testDirectChildrenPathIndexingTokenization() throws Exception {
        try {
            TokenStream ts = directChildrenPathIndexingAnalyzer.tokenStream("text", new StringReader("/jcr:a/b/jcr:c"));
            assertTokenStreamContents(ts, new String[]{"/jcr:a/b"});
            ts = directChildrenPathIndexingAnalyzer.tokenStream("text", new StringReader("/jcr:a"));
            assertTokenStreamContents(ts, new String[]{"/"});
            ts = directChildrenPathIndexingAnalyzer.tokenStream("text", new StringReader("/"));
            assertTokenStreamContents(ts, new String[]{});
        } finally {
            directChildrenPathIndexingAnalyzer.close();
        }
    }

    @Test
    public void testDirectChildrenPathSearchingTokenization() throws Exception {
        try {
            TokenStream ts = directChildrenPathSearchingAnalyzer.tokenStream("text", new StringReader("/jcr:a/jcr:b"));
            assertTokenStreamContents(ts, new String[]{"/jcr:a/jcr:b"});
        } finally {
            directChildrenPathSearchingAnalyzer.close();
        }
    }

    @Test
    public void testExactPathIndexingTokenizationAndSearch() throws Exception {
        try {
            TokenStream ts = exactPathAnalyzer.tokenStream("text", new StringReader("/jcr:a/jcr:b/c"));
            assertTokenStreamContents(ts, new String[]{"/jcr:a/jcr:b/c"});
        } finally {
            exactPathAnalyzer.close();
        }
    }

    @Test
    public void testParentPathSearchingTokenization() throws Exception {
        try {
            TokenStream ts = parentPathSearchingAnalyzer.tokenStream("text", new StringReader("/jcr:a/b/jcr:c"));
            assertTokenStreamContents(ts, new String[]{"/jcr:a/b"});
        } finally {
            parentPathSearchingAnalyzer.close();
        }
    }

    @Test
    public void testParentPathIndexingTokenization() throws Exception {
        try {
            TokenStream ts = parentPathIndexingAnalyzer.tokenStream("text", new StringReader("/a/b"));
            assertTokenStreamContents(ts, new String[]{"/a/b"});
        } finally {
            parentPathIndexingAnalyzer.close();
        }
    }

    @Test
    public void testAllChildrenPathMatching() throws Exception {
        String nodePath = "/jcr:a/jcr:b/c";
        String descendantPath = nodePath + "/d/jcr:e";
        assertAnalyzesTo(allChildrenPathIndexingAnalyzer, descendantPath, new String[]{"/jcr:a", "/", "/jcr:a/jcr:b", "/jcr:a/jcr:b/c", "/jcr:a/jcr:b/c/d", "/jcr:a/jcr:b/c/d/jcr:e"});
        assertAnalyzesTo(allChildrenPathSearchingAnalyzer, nodePath, new String[]{nodePath});
        assertAnalyzesTo(allChildrenPathSearchingAnalyzer, "/jcr:a", new String[]{"/jcr:a"});
        assertAnalyzesTo(allChildrenPathSearchingAnalyzer, "/jcr:a/b", new String[]{"/jcr:a/b"});
        assertAnalyzesTo(allChildrenPathSearchingAnalyzer, "/a/b/c", new String[]{"/a/b/c"});
        assertAnalyzesTo(allChildrenPathSearchingAnalyzer, "/a/b/c/d", new String[]{"/a/b/c/d"});
        assertAnalyzesTo(allChildrenPathSearchingAnalyzer, "/a/b/c/d/jcr:e", new String[]{"/a/b/c/d/jcr:e"});
        assertAnalyzesTo(allChildrenPathSearchingAnalyzer, "/", new String[]{"/"});
    }

    @Test
    public void testAllChildrenPathMatchingOnRootNode() throws Exception {
        String nodePath = "/";
        String descendantPath = nodePath + "jcr:a/jcr:b";
        assertAnalyzesTo(allChildrenPathIndexingAnalyzer, descendantPath, new String[]{"/jcr:a", "/", "/jcr:a/jcr:b"});
    }

    @Test
    public void testDirectChildrenPathMatching() throws Exception {
        String nodePath = "/a/b/c";
        String childPath = nodePath + "/d";
        assertAnalyzesTo(directChildrenPathIndexingAnalyzer, childPath, new String[]{nodePath});
        assertAnalyzesTo(directChildrenPathSearchingAnalyzer, nodePath, new String[]{nodePath});

        nodePath = "/";
        childPath = nodePath + "/jcr:a";
        assertAnalyzesTo(directChildrenPathIndexingAnalyzer, childPath, new String[]{nodePath});
        assertAnalyzesTo(directChildrenPathSearchingAnalyzer, nodePath, new String[]{nodePath});

        String childPath1 = "/test/jcr:resource";
        String childPath2 = "/test/resource";

        nodePath = "/test";
        assertAnalyzesTo(directChildrenPathIndexingAnalyzer, childPath1, new String[]{nodePath});
        assertAnalyzesTo(directChildrenPathIndexingAnalyzer, childPath2, new String[]{nodePath});
        assertAnalyzesTo(directChildrenPathSearchingAnalyzer, nodePath, new String[]{nodePath});
    }

    @Test
    public void testParentPathMatching() throws Exception {
        String parentPath = "/a/b";
        String nodePath = parentPath + "/jcr:c";
        assertAnalyzesTo(parentPathIndexingAnalyzer, parentPath, new String[]{parentPath});
        assertAnalyzesTo(parentPathSearchingAnalyzer, nodePath, new String[]{parentPath});
    }

}
