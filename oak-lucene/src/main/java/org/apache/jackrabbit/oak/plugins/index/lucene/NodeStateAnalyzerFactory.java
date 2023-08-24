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

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.jackrabbit.guava.common.collect.ImmutableSet;
import org.apache.jackrabbit.guava.common.collect.Maps;
import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.index.lucene.util.TokenizerChain;
import org.apache.jackrabbit.oak.plugins.index.search.FulltextIndexConstants;
import org.apache.jackrabbit.oak.plugins.index.search.util.ConfigUtil;
import org.apache.jackrabbit.oak.plugins.tree.factories.TreeFactory;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStateUtils;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.AbstractAnalysisFactory;
import org.apache.lucene.analysis.CharArraySet;
import org.apache.lucene.analysis.CharFilterFactory;
import org.apache.lucene.util.ModuleResourceLoader;
import org.apache.lucene.util.ResourceLoader;
import org.apache.lucene.util.ResourceLoaderAware;
import org.apache.lucene.analysis.StopwordAnalyzerBase;
import org.apache.lucene.analysis.TokenFilterFactory;
import org.apache.lucene.analysis.TokenizerFactory;
import org.apache.lucene.analysis.WordlistLoader;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.Version;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.jackrabbit.guava.common.base.Preconditions.checkNotNull;
import static org.apache.jackrabbit.guava.common.collect.Lists.newArrayList;

/**
 * Constructs a Lucene Analyzer from nodes (based on NodeState content).
 *
 * Approach taken is similar to one taken in
 * org.apache.solr.schema.FieldTypePluginLoader which is implemented for xml
 * based config. Resource lookup are performed via binary property access
 */
final class NodeStateAnalyzerFactory {
    private static final AtomicBoolean versionWarningAlreadyLogged = new AtomicBoolean(false);

    private static final Set<String> IGNORE_PROP_NAMES = ImmutableSet.of(
            FulltextIndexConstants.ANL_CLASS,
            FulltextIndexConstants.ANL_NAME,
            JcrConstants.JCR_PRIMARYTYPE
    );

    private static final Logger log = LoggerFactory.getLogger(NodeStateAnalyzerFactory.class);

    private final ResourceLoader defaultLoader;
    private final Version defaultVersion;

    NodeStateAnalyzerFactory(Version defaultVersion){
        this(new ModuleResourceLoader(NodeStateAnalyzerFactory.class.getModule()), defaultVersion);
    }

    NodeStateAnalyzerFactory(ResourceLoader defaultLoader, Version defaultVersion) {
        this.defaultLoader = defaultLoader;
        this.defaultVersion = defaultVersion;
    }

    public Analyzer createInstance(NodeState state) {
        if (state.hasProperty(FulltextIndexConstants.ANL_CLASS)){
            return createAnalyzerViaReflection(state);
        }
        return composeAnalyzer(state);
    }

    private Analyzer composeAnalyzer(NodeState state) {
        TokenizerFactory tf = loadTokenizer(state.getChildNode(FulltextIndexConstants.ANL_TOKENIZER));
        CharFilterFactory[] cfs = loadCharFilterFactories(state.getChildNode(FulltextIndexConstants.ANL_CHAR_FILTERS));
        TokenFilterFactory[] tffs = loadTokenFilterFactories(state.getChildNode(FulltextIndexConstants.ANL_FILTERS));
        return new TokenizerChain(cfs, tf, tffs);
    }

    private TokenFilterFactory[] loadTokenFilterFactories(NodeState tokenFiltersState) {
        List<TokenFilterFactory> result = newArrayList();

        Tree tree = TreeFactory.createReadOnlyTree(tokenFiltersState);
        for (Tree t : tree.getChildren()){
            NodeState state = tokenFiltersState.getChildNode(t.getName());

            String factoryType = getFactoryType(state, t.getName());
            Map<String, String> args = convertNodeState(state);
            TokenFilterFactory cf = TokenFilterFactory.forName(factoryType, args);
            init(cf, state);
            result.add(cf);
        }

        return result.toArray(new TokenFilterFactory[result.size()]);
    }

    private CharFilterFactory[] loadCharFilterFactories(NodeState charFiltersState) {
        List<CharFilterFactory> result = newArrayList();

        //Need to read children in order
        Tree tree = TreeFactory.createReadOnlyTree(charFiltersState);
        for (Tree t : tree.getChildren()){
            NodeState state = charFiltersState.getChildNode(t.getName());

            String factoryType = getFactoryType(state, t.getName());
            Map<String, String> args = convertNodeState(state);
            CharFilterFactory cf = CharFilterFactory.forName(factoryType, args);
            init(cf, state);
            result.add(cf);
        }

        return result.toArray(new CharFilterFactory[result.size()]);
    }

    private TokenizerFactory loadTokenizer(NodeState state) {
        String clazz = checkNotNull(state.getString(FulltextIndexConstants.ANL_NAME));
        Map<String, String> args = convertNodeState(state);
        TokenizerFactory tf = TokenizerFactory.forName(clazz, args);
        init(tf, state);
        return tf;
    }

    private Analyzer createAnalyzerViaReflection(NodeState state) {
        String clazz = state.getString(FulltextIndexConstants.ANL_CLASS);
        Class<? extends Analyzer> analyzerClazz = defaultLoader.findClass(clazz, Analyzer.class);

        Version matchVersion = getVersion(state);
        CharArraySet stopwords = null;
        if (StopwordAnalyzerBase.class.isAssignableFrom(analyzerClazz)
                && state.hasChildNode(FulltextIndexConstants.ANL_STOPWORDS)) {
            try {
                stopwords = loadStopwordSet(state.getChildNode(FulltextIndexConstants.ANL_STOPWORDS),
                        FulltextIndexConstants.ANL_STOPWORDS, matchVersion);
            } catch (IOException e) {
                throw new RuntimeException("Error occurred while loading stopwords", e);
            }
        }
        Constructor<? extends Analyzer> c = null;

        try {
            if (stopwords != null) {
                c = analyzerClazz.getConstructor(Version.class, CharArraySet.class);
                return c.newInstance(matchVersion, stopwords);
            } else {
                c = analyzerClazz.getConstructor(Version.class);
                return c.newInstance(matchVersion);
            }
        } catch (NoSuchMethodException e) {
            throw new RuntimeException("Error occurred while instantiating Analyzer for " + analyzerClazz, e);
        } catch (InstantiationException e) {
            throw new RuntimeException("Error occurred while instantiating Analyzer for " + analyzerClazz, e);
        } catch (IllegalAccessException e) {
            throw new RuntimeException("Error occurred while instantiating Analyzer for " + analyzerClazz, e);
        } catch (InvocationTargetException e) {
            throw new RuntimeException("Error occurred while instantiating Analyzer for " + analyzerClazz, e);
        }
    }

    private void init(AbstractAnalysisFactory o, NodeState state) {
        if (o instanceof ResourceLoaderAware) {
            try {
                ((ResourceLoaderAware) o).inform(new NodeStateResourceLoader(state, defaultLoader));
            } catch (IOException e) {
                throw new IllegalArgumentException("Error occurred while initializing type " + o.getClass(), e);
            }
        }

        if (state.hasProperty(LuceneIndexConstants.ANL_LUCENE_MATCH_VERSION)){
            o.setExplicitLuceneMatchVersion(true);
        }
    }

    Map<String, String> convertNodeState(NodeState state) {
        Map<String, String> result = Maps.newHashMap();
        for (PropertyState ps : state.getProperties()) {
            String name = ps.getName();
            if (ps.getType() != Type.BINARY
                    && !ps.isArray()
                    && !(name != null && NodeStateUtils.isHidden(name))
                    && !IGNORE_PROP_NAMES.contains(name)) {
                result.put(name, ps.getValue(Type.STRING));
            }
        }
        result.put(LuceneIndexConstants.ANL_LUCENE_MATCH_VERSION, getVersion(state).toString());
        return result;
    }

    private Version getVersion(NodeState state){
        Version version = defaultVersion;
        if (state.hasProperty(LuceneIndexConstants.ANL_LUCENE_MATCH_VERSION)){
            version = parseLuceneVersionString(state.getString(LuceneIndexConstants.ANL_LUCENE_MATCH_VERSION));
        }
        return version;
    }

    private static String getFactoryType(NodeState state, String nodeStateName){
        String type = state.getString(FulltextIndexConstants.ANL_NAME);
        return type != null ? type : nodeStateName;
    }

    @SuppressWarnings("deprecation")
    private static Version parseLuceneVersionString(final String matchVersion) {
        final Version version = Version.parseLeniently(matchVersion);
        if (version == Version.LUCENE_CURRENT && !versionWarningAlreadyLogged.getAndSet(true)) {
            log.warn(
                    "You should not use LATEST as luceneMatchVersion property: "+
                            "if you use this setting, and then Solr upgrades to a newer release of Lucene, "+
                            "sizable changes may happen. If precise back compatibility is important "+
                            "then you should instead explicitly specify an actual Lucene version."
            );
        }
        return version;
    }

    private static CharArraySet loadStopwordSet(NodeState file, String name,
                                                Version matchVersion) throws IOException {
        Blob blob = ConfigUtil.getBlob(file, name);
        Reader stopwords = new InputStreamReader(blob.getNewStream(), IOUtils.CHARSET_UTF_8);
        try {
            return WordlistLoader.getWordSet(stopwords, matchVersion);
        } finally {
            IOUtils.close(stopwords);
        }
    }

    static class NodeStateResourceLoader implements ResourceLoader {
        private final NodeState state;
        private final ResourceLoader delegate;

        public NodeStateResourceLoader(NodeState state, ResourceLoader delegate) {
            this.state = state;
            this.delegate = delegate;
        }

        @Override
        public InputStream openResource(String resource) throws IOException {
            if (state.hasChildNode(resource)){
                return ConfigUtil.getBlob(state.getChildNode(resource), resource).getNewStream();
            }
            return delegate.openResource(resource);
        }

        @Override
        public <T> Class<? extends T> findClass(String cname, Class<T> expectedType) {
            //For factories the cname is not FQN. Instead its the name without suffix
            //For e.g. for WhitespaceTokenizerFactory its 'whitespace'
            if (CharFilterFactory.class.isAssignableFrom(expectedType)) {
                return CharFilterFactory.lookupClass(cname).asSubclass(expectedType);
            } else if (TokenizerFactory.class.isAssignableFrom(expectedType)) {
                return TokenizerFactory.lookupClass(cname).asSubclass(expectedType);
            } else if (TokenFilterFactory.class.isAssignableFrom(expectedType)) {
                return TokenFilterFactory.lookupClass(cname).asSubclass(expectedType);
            }
            return delegate.findClass(cname, expectedType);
        }

        @Override
        public <T> T newInstance(String cname, Class<T> expectedType) {
            throw new UnsupportedOperationException();
        }
    }
}
