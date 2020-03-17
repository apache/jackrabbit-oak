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

package org.apache.jackrabbit.oak.plugins.index.lucene;

import java.io.ByteArrayOutputStream;
import java.io.PrintWriter;
import java.io.Reader;
import java.lang.reflect.Field;
import java.util.Map;

import com.google.common.collect.ImmutableList;
import org.apache.commons.io.IOUtils;
import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.oak.plugins.index.lucene.NodeStateAnalyzerFactory.NodeStateResourceLoader;
import org.apache.jackrabbit.oak.plugins.index.lucene.util.TokenizerChain;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.lucene.analysis.charfilter.HTMLStripCharFilterFactory;
import org.apache.lucene.analysis.charfilter.MappingCharFilterFactory;
import org.apache.lucene.analysis.core.LowerCaseFilterFactory;
import org.apache.lucene.analysis.core.LowerCaseTokenizer;
import org.apache.lucene.analysis.core.StopFilterFactory;
import org.apache.lucene.analysis.core.WhitespaceTokenizerFactory;
import org.apache.lucene.analysis.path.PathHierarchyTokenizerFactory;
import org.apache.lucene.analysis.util.CharArraySet;
import org.apache.lucene.analysis.util.ClasspathResourceLoader;
import org.apache.lucene.analysis.util.StopwordAnalyzerBase;
import org.apache.lucene.util.Version;
import org.junit.Test;

import static org.apache.jackrabbit.JcrConstants.JCR_CONTENT;
import static org.apache.jackrabbit.JcrConstants.JCR_DATA;
import static org.apache.jackrabbit.JcrConstants.JCR_PRIMARYTYPE;
import static org.apache.jackrabbit.oak.api.Type.NAMES;
import static org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexConstants.ANL_CHAR_FILTERS;
import static org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexConstants.ANL_CLASS;
import static org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexConstants.ANL_LUCENE_MATCH_VERSION;
import static org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexConstants.ANL_NAME;
import static org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexConstants.ANL_FILTERS;
import static org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexConstants.ANL_TOKENIZER;
import static org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.EMPTY_NODE;
import static org.apache.jackrabbit.oak.plugins.tree.TreeConstants.OAK_CHILD_ORDER;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class NodeStateAnalyzerFactoryTest {

    private NodeStateAnalyzerFactory factory = new NodeStateAnalyzerFactory(LuceneIndexConstants.VERSION);

    @Test
    public void analyzerViaReflection() throws Exception{
        NodeBuilder nb = EMPTY_NODE.builder();
        nb.setProperty(ANL_CLASS, TestAnalyzer.class.getName());

        TestAnalyzer analyzer = (TestAnalyzer) factory.createInstance(nb.getNodeState());
        assertNotNull(analyzer);
        assertEquals(LuceneIndexConstants.VERSION, analyzer.matchVersion);

        nb.setProperty(LuceneIndexConstants.ANL_LUCENE_MATCH_VERSION, Version.LUCENE_31.toString());
        analyzer = (TestAnalyzer) factory.createInstance(nb.getNodeState());
        assertEquals("Version field not picked from config",Version.LUCENE_31, analyzer.matchVersion);

        byte[] stopWords = newCharArraySet("foo", "bar");
        createFileNode(nb, LuceneIndexConstants.ANL_STOPWORDS, stopWords);
        analyzer = (TestAnalyzer) factory.createInstance(nb.getNodeState());

        assertTrue("Configured stopword set not used",analyzer.getStopwordSet().contains("foo"));
    }

    @Test
    public void analyzerByComposition_Tokenizer() throws Exception{
        NodeBuilder nb = EMPTY_NODE.builder();
        nb.child(ANL_TOKENIZER).setProperty(ANL_NAME, "whitespace");

        TokenizerChain analyzer = (TokenizerChain) factory.createInstance(nb.getNodeState());
        assertEquals(WhitespaceTokenizerFactory.class.getName(), analyzer.getTokenizer().getClassArg());

        nb.child(ANL_TOKENIZER)
                .setProperty(ANL_NAME, "pathhierarchy")
                .setProperty("delimiter", "#");
        analyzer = (TokenizerChain) factory.createInstance(nb.getNodeState());
        assertEquals(PathHierarchyTokenizerFactory.class.getName(), analyzer.getTokenizer().getClassArg());
        assertEquals('#', getValue(analyzer.getTokenizer(), "delimiter"));
    }

    @Test
    public void analyzerByComposition_TokenFilter() throws Exception{
        NodeBuilder nb = EMPTY_NODE.builder();
        nb.child(ANL_TOKENIZER).setProperty(ANL_NAME, "whitespace");

        NodeBuilder filters = nb.child(ANL_FILTERS);
        filters.setProperty(OAK_CHILD_ORDER, ImmutableList.of("stop", "LowerCase"),NAMES);
        filters.child("LowerCase").setProperty(ANL_NAME, "LowerCase");
        filters.child("LowerCase").setProperty(JCR_PRIMARYTYPE, "nt:unstructured");
        //name is optional. Derived from nodeName
        filters.child("stop").setProperty(ANL_LUCENE_MATCH_VERSION, Version.LUCENE_31.toString());

        TokenizerChain analyzer = (TokenizerChain) factory.createInstance(nb.getNodeState());
        assertEquals(2, analyzer.getFilters().length);

        //check the order
        assertEquals(StopFilterFactory.class.getName(), analyzer.getFilters()[0].getClassArg());
        assertEquals(LowerCaseFilterFactory.class.getName(), analyzer.getFilters()[1].getClassArg());

        assertTrue(analyzer.getFilters()[0].isExplicitLuceneMatchVersion());
    }

    @Test
    public void analyzerByComposition_CharFilter() throws Exception{
        NodeBuilder nb = EMPTY_NODE.builder();
        nb.child(ANL_TOKENIZER).setProperty(ANL_NAME, "whitespace");

        NodeBuilder filters = nb.child(ANL_CHAR_FILTERS);
        filters.setProperty(OAK_CHILD_ORDER, ImmutableList.of("htmlStrip", "mapping"),NAMES);
        filters.child("mapping").setProperty(ANL_NAME, "mapping");
        filters.child("htmlStrip"); //name is optional. Derived from nodeName

        TokenizerChain analyzer = (TokenizerChain) factory.createInstance(nb.getNodeState());
        assertEquals(2, analyzer.getCharFilters().length);

        //check the order
        assertEquals(HTMLStripCharFilterFactory.class.getName(), analyzer.getCharFilters()[0].getClassArg());
        assertEquals(MappingCharFilterFactory.class.getName(), analyzer.getCharFilters()[1].getClassArg());
    }

    @Test
    public void analyzerByComposition_FileResource() throws Exception{
        NodeBuilder nb = EMPTY_NODE.builder();
        nb.child(ANL_TOKENIZER).setProperty(ANL_NAME, "whitespace");

        NodeBuilder filters = nb.child(ANL_FILTERS);
        //name is optional. Derived from nodeName
        NodeBuilder stop = filters.child("stop");
        stop.setProperty("words", "set1.txt, set2.txt");
        createFileNode(stop, "set1.txt", newCharArraySet("foo", "bar"));
        createFileNode(stop, "set2.txt", newCharArraySet("foo1", "bar1"));

        TokenizerChain analyzer = (TokenizerChain) factory.createInstance(nb.getNodeState());
        assertEquals(1, analyzer.getFilters().length);

        //check the order
        assertEquals(StopFilterFactory.class.getName(), analyzer.getFilters()[0].getClassArg());

        StopFilterFactory sff = (StopFilterFactory) analyzer.getFilters()[0];
        assertTrue(sff.getStopWords().contains("foo"));
        assertTrue(sff.getStopWords().contains("foo1"));
    }

    @Test
    public void nodeStateResourceLoader() throws Exception{
        byte[] testData = "hello".getBytes();
        NodeBuilder nb = EMPTY_NODE.builder();
        createFileNode(nb, "foo", testData);

        NodeStateResourceLoader loader = new NodeStateResourceLoader(nb.getNodeState(),
                new ClasspathResourceLoader());
        assertArrayEquals(testData, IOUtils.toByteArray(loader.openResource("foo")));
    }

    @Test
    public void nodeStateAsMap() throws Exception{
        NodeBuilder nb = EMPTY_NODE.builder();
        nb.setProperty("a", "a");
        nb.setProperty("b", 1);
        nb.setProperty(JcrConstants.JCR_PRIMARYTYPE, "nt:base");
        nb.setProperty(":hiddenProp", "hiddenValue");

        Map<String, String> result = factory.convertNodeState(nb.getNodeState());
        assertEquals("a", result.get("a"));
        assertEquals("1", result.get("b"));
        assertNull(result.get(JcrConstants.JCR_PRIMARYTYPE));
        assertNull(result.get(":hiddenProp"));
    }

    private static NodeBuilder createFileNode(NodeBuilder nb, String nodeName, byte[] content){
        return nb.child(nodeName).child(JCR_CONTENT).setProperty(JCR_DATA, content);
    }

    private static byte[] newCharArraySet(String ... words){
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        PrintWriter pw = new PrintWriter(baos);
        for (String word : words){
            pw.println(word);
        }
        pw.close();
        return baos.toByteArray();
    }

    public static class TestAnalyzer extends StopwordAnalyzerBase{
        final Version matchVersion;

        public TestAnalyzer(Version matchVersion) {
            super(matchVersion);
            this.matchVersion = matchVersion;
        }

        public TestAnalyzer(Version version, CharArraySet stopwords) {
            super(version, stopwords);
            this.matchVersion = version;
        }

        @Override
        protected TokenStreamComponents createComponents(final String fieldName, final Reader reader) {
            return new TokenStreamComponents(new LowerCaseTokenizer(matchVersion, reader));
        }
    }

    private static Object getValue(Object o, String fieldName) throws NoSuchFieldException, IllegalAccessException {
        Field f = o.getClass().getDeclaredField(fieldName);
        f.setAccessible(true);
        return f.get(o);
    }
}
