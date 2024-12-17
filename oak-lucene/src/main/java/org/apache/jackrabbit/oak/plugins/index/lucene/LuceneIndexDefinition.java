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

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.jackrabbit.oak.plugins.index.lucene.util.CompressingCodec;
import org.apache.jackrabbit.oak.plugins.index.lucene.util.TokenizerChain;
import org.apache.jackrabbit.oak.plugins.index.lucene.writer.CommitMitigatingTieredMergePolicy;
import org.apache.jackrabbit.oak.plugins.index.search.FieldNames;
import org.apache.jackrabbit.oak.plugins.index.search.FulltextIndexConstants;
import org.apache.jackrabbit.oak.plugins.index.search.IndexDefinition;
import org.apache.jackrabbit.oak.plugins.index.search.IndexFormatVersion;
import org.apache.jackrabbit.oak.spi.state.ChildNodeEntry;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.miscellaneous.LimitTokenCountAnalyzer;
import org.apache.lucene.analysis.miscellaneous.PerFieldAnalyzerWrapper;
import org.apache.lucene.analysis.path.PathHierarchyTokenizerFactory;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.index.LogByteSizeMergePolicy;
import org.apache.lucene.index.LogDocMergePolicy;
import org.apache.lucene.index.MergePolicy;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.TieredMergePolicy;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.jackrabbit.oak.plugins.index.search.FulltextIndexConstants.ANL_DEFAULT;
import static org.apache.jackrabbit.oak.plugins.index.search.FulltextIndexConstants.INDEX_ORIGINAL_TERM;
import static org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexConstants.VERSION;
import static org.apache.jackrabbit.oak.plugins.index.search.util.ConfigUtil.getOptionalValue;

public class LuceneIndexDefinition extends IndexDefinition {
    private static final Logger log = LoggerFactory.getLogger(LuceneIndexDefinition.class);

    private final boolean saveDirListing;

    private final Map<String, Analyzer> analyzers;
    private final Analyzer analyzer;

    private final Codec codec;

    private final int maxFieldLength;

    public LuceneIndexDefinition(NodeState root, NodeState defn, String indexPath) {
        this(root, getIndexDefinitionState(defn), determineIndexFormatVersion(defn), determineUniqueId(defn), indexPath);
    }

    LuceneIndexDefinition(NodeState root, NodeState defn, IndexFormatVersion version, String uid, String indexPath) {
        super(root, defn, version, uid, indexPath);

        this.saveDirListing = getOptionalValue(defn, LuceneIndexConstants.SAVE_DIR_LISTING, true);
        this.maxFieldLength = getOptionalValue(defn, LuceneIndexConstants.MAX_FIELD_LENGTH, DEFAULT_MAX_FIELD_LENGTH);
        this.analyzers = collectAnalyzers(defn);
        this.analyzer = createAnalyzer();
        this.codec = createCodec();
    }

    public static Builder newBuilder(NodeState root, NodeState defn, String indexPath){
        return (Builder)new Builder()
                .root(root)
                .defn(defn)
                .indexPath(indexPath);
    }

    public static class Builder extends IndexDefinition.Builder {
        @Override
        public LuceneIndexDefinition build() {
            return (LuceneIndexDefinition)super.build();
        }

        @Override
        public LuceneIndexDefinition.Builder reindex() {
            super.reindex();
            return this;
        }

        @Override
        protected IndexDefinition createInstance(NodeState indexDefnStateToUse) {
            return new LuceneIndexDefinition(root, indexDefnStateToUse, version, uid, indexPath);
        }
    }

    @Override
    protected String getDefaultFunctionName() {
        return "lucene";//TODO should this be LuceneIndexConstants.TYPE_LUCENE?
    }

    @Override
    protected double getDefaultCostPerEntry(IndexFormatVersion version) {
        //For older format cost per entry would be higher as it does a runtime
        //aggregation
        return version == IndexFormatVersion.V1 ?  1.5 : 1.0;
    }

    public boolean saveDirListing() {
        return saveDirListing;
    }

    @Nullable
    public Codec getCodec() {
        return codec;
    }

    @NotNull
    public MergePolicy getMergePolicy() {
        // MP is not cached to avoid complaining about multiple IWs with multiplexing writers
        return createMergePolicy();
    }

    public Analyzer getAnalyzer(){
        return analyzer;
    }

    //~---------------------------------------------------< Analyzer >

    private Analyzer createAnalyzer() {
        Analyzer result;
        Analyzer defaultAnalyzer = LuceneIndexConstants.ANALYZER;
        if (analyzers.containsKey(ANL_DEFAULT)){
            defaultAnalyzer = analyzers.get(ANL_DEFAULT);
        }
        if (!evaluatePathRestrictions()){
            result = defaultAnalyzer;
        } else {
            Map<String, Analyzer> analyzerMap = Map.of(
                    FieldNames.ANCESTORS, new TokenizerChain(new PathHierarchyTokenizerFactory(Collections.emptyMap())));
            result = new PerFieldAnalyzerWrapper(defaultAnalyzer, analyzerMap);
        }

        //In case of negative value no limits would be applied
        if (maxFieldLength < 0){
            return result;
        }
        return new LimitTokenCountAnalyzer(result, maxFieldLength);
    }

    private static Map<String, Analyzer> collectAnalyzers(NodeState defn) {
        Map<String, Analyzer> analyzerMap = new HashMap<>();
        NodeStateAnalyzerFactory factory = new NodeStateAnalyzerFactory(VERSION);
        NodeState analyzersTree = defn.getChildNode(FulltextIndexConstants.ANALYZERS);
        for (ChildNodeEntry cne : analyzersTree.getChildNodeEntries()) {
            Analyzer a = factory.createInstance(cne.getNodeState());
            analyzerMap.put(cne.getName(), a);
        }

        if (getOptionalValue(analyzersTree, INDEX_ORIGINAL_TERM, false) && !analyzerMap.containsKey(ANL_DEFAULT)) {
            analyzerMap.put(ANL_DEFAULT, new OakAnalyzer(VERSION, true));
        }

        return Collections.unmodifiableMap(analyzerMap);
    }


    //~---------------------------------------------< utility >

    private Codec createCodec() {
        String mmp = System.getProperty("oak.lucene.compressing-codec");
        if (mmp != null) {
            return new CompressingCodec();
        }

        String codecName = getOptionalValue(definition, LuceneIndexConstants.CODEC_NAME, null);
        Codec codec = null;
        if (codecName != null) {
            // prevent LUCENE-6482
            // (also done in LuceneIndexProviderService, just to be save)
            OakCodec ensureLucene46CodecLoaded = new OakCodec();
            // to ensure the JVM doesn't optimize away object creation
            // (probably not really needed; just to be save)
            log.debug("Lucene46Codec is loaded: {}", ensureLucene46CodecLoaded);
            codec = Codec.forName(codecName);
            log.debug("Codec is loaded: {}", codecName);
        } else if (fullTextEnabled) {
            codec = new OakCodec();
        }
        return codec;
    }

    private MergePolicy createMergePolicy() {
        String mmp = System.getProperty("oak.lucene.cmmp");
        if (mmp != null) {
            return new CommitMitigatingTieredMergePolicy();
        }

        String mergePolicyName = getOptionalValue(definition, LuceneIndexConstants.MERGE_POLICY_NAME, null);
        MergePolicy mergePolicy = null;
        if (mergePolicyName != null) {
            if (mergePolicyName.equalsIgnoreCase("no")) {
                mergePolicy = NoMergePolicy.COMPOUND_FILES;
            } else if (mergePolicyName.equalsIgnoreCase("mitigated")) {
                mergePolicy = new CommitMitigatingTieredMergePolicy();
            } else if (mergePolicyName.equalsIgnoreCase("tiered") || mergePolicyName.equalsIgnoreCase("default")) {
                mergePolicy = new TieredMergePolicy();
            } else if (mergePolicyName.equalsIgnoreCase("logbyte")) {
                mergePolicy = new LogByteSizeMergePolicy();
            } else if (mergePolicyName.equalsIgnoreCase("logdoc")) {
                mergePolicy = new LogDocMergePolicy();
            }
        }
        if (mergePolicy == null) {
            mergePolicy = new TieredMergePolicy();
        }
        return mergePolicy;
    }
}
