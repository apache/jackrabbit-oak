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
package org.apache.jackrabbit.oak.plugins.index.mongot.index;

import java.util.List;

import org.apache.jackrabbit.oak.plugins.index.mongot.MongotIndexDefinition;
import org.apache.jackrabbit.oak.plugins.index.search.FieldNames;
import org.apache.jackrabbit.oak.plugins.index.search.FulltextIndexConstants;
import org.apache.jackrabbit.oak.plugins.index.search.util.IndexDefinitionBuilder;
import org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState;
import org.apache.jackrabbit.oak.api.Tree;
import org.bson.Document;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;

public class MongotSearchIndexDefinitionBuilderTest {

    @Test
    public void mapsOakDefaultAnalyzer() {
        Document searchDefinition = MongotSearchIndexDefinitionBuilder.build(definition(builder()));

        assertEquals("oak_default", searchDefinition.getString("analyzer"));
        assertEquals("lucene.standard", searchDefinition.getString("searchAnalyzer"));
        Document custom = searchDefinition.getList("analyzers", Document.class).get(0);
        assertEquals(new Document("type", "standard"), custom.get("tokenizer"));
        assertEquals(List.of(
                        new Document("type", "lowercase"),
                        new Document("type", "wordDelimiterGraph")
                                .append("delimiterOptions",
                                        new Document("preserveOriginal", true)
                                                .append("splitOnCaseChange", false)
                                                .append("splitOnNumerics", false))),
                custom.getList("tokenFilters", Document.class));
    }

    @Test
    public void mapsBuiltInAnalyzerAndVirtualFields() {
        IndexDefinitionBuilder builder = builder();
        builder.indexRule("nt:base").property("title")
                .analyzed().nodeScopeIndex().useInSuggest().useInSpellcheck();
        Tree analyzer = builder.getBuilderTree()
                .addChild(FulltextIndexConstants.ANALYZERS)
                .addChild(FulltextIndexConstants.ANL_DEFAULT);
        analyzer.setProperty(FulltextIndexConstants.ANL_CLASS,
                "org.apache.lucene.analysis.en.EnglishAnalyzer");

        Document searchDefinition = MongotSearchIndexDefinitionBuilder.build(definition(builder));

        assertEquals("lucene.english", searchDefinition.getString("analyzer"));
        Document fields = searchDefinition.get("mappings", Document.class)
                .get("fields", Document.class);
        assertEquals("autocomplete", fields.get(MongoFieldNames.SUGGEST, Document.class)
                .getString("type"));
        assertEquals("lucene.standard", fields.get(MongoFieldNames.SUGGEST, Document.class)
                .getString("analyzer"));
        assertEquals("string", fields.get(MongoFieldNames.SPELLCHECK, Document.class)
                .getString("type"));
    }

    @Test
    public void mapsBuiltInAnalyzerName() {
        IndexDefinitionBuilder builder = builder();
        Tree analyzer = builder.getBuilderTree()
                .addChild(FulltextIndexConstants.ANALYZERS)
                .addChild(FulltextIndexConstants.ANL_DEFAULT);
        analyzer.setProperty(FulltextIndexConstants.ANL_NAME, "german");

        Document searchDefinition = MongotSearchIndexDefinitionBuilder.build(definition(builder));

        assertEquals("lucene.german", searchDefinition.getString("analyzer"));
        assertEquals("lucene.german", searchDefinition.getString("searchAnalyzer"));
    }

    @Test
    public void rejectsUnknownBuiltInAnalyzerName() {
        IndexDefinitionBuilder builder = builder();
        Tree analyzer = builder.getBuilderTree()
                .addChild(FulltextIndexConstants.ANALYZERS)
                .addChild(FulltextIndexConstants.ANL_DEFAULT);
        analyzer.setProperty(FulltextIndexConstants.ANL_NAME, "this_does_not_exist");

        assertThrows(IllegalArgumentException.class,
                () -> MongotSearchIndexDefinitionBuilder.build(definition(builder)));
    }

    @Test
    public void keepsGraphAnalyzerOffAutocompleteField() {
        IndexDefinitionBuilder builder = builder();
        builder.indexRule("nt:base").property("title").useInSuggest();

        Document searchDefinition = MongotSearchIndexDefinitionBuilder.build(definition(builder));
        Document suggest = searchDefinition.get("mappings", Document.class)
                .get("fields", Document.class)
                .get(MongoFieldNames.SUGGEST, Document.class);

        assertEquals("oak_default", searchDefinition.getString("analyzer"));
        assertEquals("lucene.standard", suggest.getString("analyzer"));
    }

    @Test
    public void mapsComposedAnalyzer() {
        IndexDefinitionBuilder builder = builder();
        builder.indexRule("nt:base").property("title").analyzed().nodeScopeIndex();
        Tree analyzer = builder.getBuilderTree()
                .addChild(FulltextIndexConstants.ANALYZERS)
                .addChild(FulltextIndexConstants.ANL_DEFAULT);
        analyzer
                .addChild(FulltextIndexConstants.ANL_TOKENIZER)
                .setProperty(FulltextIndexConstants.ANL_NAME, "Standard");
        Tree charFilters = analyzer.addChild(FulltextIndexConstants.ANL_CHAR_FILTERS);
        charFilters.setOrderableChildren(true);
        charFilters.addChild("HTMLStrip");
        Tree filters = analyzer.addChild(FulltextIndexConstants.ANL_FILTERS);
        filters.setOrderableChildren(true);
        filters.addChild("LowerCase");
        filters.addChild("PorterStem");

        Document searchDefinition = MongotSearchIndexDefinitionBuilder.build(definition(builder));

        assertEquals("oak_default", searchDefinition.getString("analyzer"));
        Document custom = searchDefinition.getList("analyzers", Document.class).get(0);
        assertEquals("oak_default", custom.getString("name"));
        assertEquals(new Document("type", "standard"), custom.get("tokenizer"));
        assertEquals(java.util.List.of(new Document("type", "htmlStrip")
                        .append("ignoredTags", java.util.List.of())),
                custom.getList("charFilters", Document.class));
        assertEquals(java.util.List.of(
                        new Document("type", "lowercase"),
                        new Document("type", "porterStemming")),
                custom.getList("tokenFilters", Document.class));
    }

    @Test
    public void mapsPatternReplaceCharacterFilter() {
        IndexDefinitionBuilder builder = builder();
        Tree analyzer = builder.getBuilderTree()
                .addChild(FulltextIndexConstants.ANALYZERS)
                .addChild(FulltextIndexConstants.ANL_DEFAULT);
        analyzer.addChild(FulltextIndexConstants.ANL_TOKENIZER)
                .setProperty(FulltextIndexConstants.ANL_NAME, "Standard");
        Tree patternReplace = analyzer.addChild(FulltextIndexConstants.ANL_CHAR_FILTERS)
                .addChild("PatternReplace");
        patternReplace.setProperty("pattern", "(\\d+)-(?=\\d)");
        patternReplace.setProperty("replacement", "$1");

        Document custom = MongotSearchIndexDefinitionBuilder.build(definition(builder))
                .getList("analyzers", Document.class).get(0);

        assertEquals(List.of(new Document("type", "patternReplace")
                        .append("pattern", "(\\d+)-(?=\\d)")
                        .append("replacement", "$1")),
                custom.getList("charFilters", Document.class));
    }

    @Test
    public void rejectsUnknownComposedTokenizer() {
        IndexDefinitionBuilder builder = builder();
        Tree analyzer = builder.getBuilderTree()
                .addChild(FulltextIndexConstants.ANALYZERS)
                .addChild(FulltextIndexConstants.ANL_DEFAULT);
        analyzer.addChild(FulltextIndexConstants.ANL_TOKENIZER)
                .setProperty(FulltextIndexConstants.ANL_NAME, "NotARealTokenizer");

        assertThrows(IllegalArgumentException.class,
                () -> MongotSearchIndexDefinitionBuilder.build(definition(builder)));
    }

    @Test
    public void mapsMongotSupportedTokenizerAndTokenFilters() {
        IndexDefinitionBuilder builder = builder();
        Tree analyzer = builder.getBuilderTree()
                .addChild(FulltextIndexConstants.ANALYZERS)
                .addChild(FulltextIndexConstants.ANL_DEFAULT);
        Tree tokenizer = analyzer.addChild(FulltextIndexConstants.ANL_TOKENIZER);
        tokenizer.setProperty(FulltextIndexConstants.ANL_NAME, "Pattern");
        tokenizer.setProperty("pattern", "[^a-z]+");

        Tree filters = analyzer.addChild(FulltextIndexConstants.ANL_FILTERS);
        filters.setOrderableChildren(true);
        Tree ngram = filters.addChild("NGram");
        ngram.setProperty("minGramSize", 2);
        ngram.setProperty("maxGramSize", 3);
        Tree edge = filters.addChild("EdgeNGram");
        edge.setProperty("minGramSize", 1);
        edge.setProperty("maxGramSize", 4);
        Tree shingle = filters.addChild("Shingle");
        shingle.setProperty("minShingleSize", 2);
        shingle.setProperty("maxShingleSize", 3);
        shingle.setProperty("outputUnigrams", false);
        Tree limit = filters.addChild("LimitTokenCount");
        limit.setProperty("maxTokenCount", 10);
        Tree snowball = filters.addChild("SnowballPorter");
        snowball.setProperty("language", "Italian");

        Document custom = MongotSearchIndexDefinitionBuilder.build(definition(builder))
                .getList("analyzers", Document.class).get(0);

        assertEquals(new Document("type", "regexSplit").append("pattern", "[^a-z]+"),
                custom.get("tokenizer"));
        assertEquals(List.of(
                        new Document("type", "nGram").append("minGram", 2).append("maxGram", 3),
                        new Document("type", "edgeGram").append("minGram", 1).append("maxGram", 4),
                        new Document("type", "shingle")
                                .append("minShingleSize", 2).append("maxShingleSize", 3),
                        new Document("type", "limitTokenCount").append("maxTokenCount", 10),
                        new Document("type", "snowballStemming").append("stemmerName", "italian")),
                custom.getList("tokenFilters", Document.class));
    }

    @Test
    public void mapsWordDelimiterOptions() {
        IndexDefinitionBuilder builder = builder();
        Tree analyzer = builder.getBuilderTree()
                .addChild(FulltextIndexConstants.ANALYZERS)
                .addChild(FulltextIndexConstants.ANL_DEFAULT);
        analyzer.addChild(FulltextIndexConstants.ANL_TOKENIZER)
                .setProperty(FulltextIndexConstants.ANL_NAME, "Standard");
        Tree delimiter = analyzer.addChild(FulltextIndexConstants.ANL_FILTERS)
                .addChild("WordDelimiter");
        delimiter.setProperty("preserveOriginal", "1");
        delimiter.setProperty("splitOnCaseChange", "0");
        delimiter.setProperty("splitOnNumerics", "0");

        Document custom = MongotSearchIndexDefinitionBuilder.build(definition(builder))
                .getList("analyzers", Document.class).get(0);

        assertEquals(List.of(new Document("type", "wordDelimiterGraph")
                        .append("delimiterOptions", new Document("preserveOriginal", true)
                                .append("splitOnCaseChange", false)
                                .append("splitOnNumerics", false))),
                custom.getList("tokenFilters", Document.class));
    }

    @Test
    public void mapsWordDelimiterCustomTypes() {
        IndexDefinitionBuilder builder = builder();
        Tree analyzer = builder.getBuilderTree()
                .addChild(FulltextIndexConstants.ANALYZERS)
                .addChild(FulltextIndexConstants.ANL_DEFAULT);
        analyzer.addChild(FulltextIndexConstants.ANL_TOKENIZER)
                .setProperty(FulltextIndexConstants.ANL_NAME, "Keyword");
        Tree delimiter = analyzer.addChild(FulltextIndexConstants.ANL_FILTERS)
                .addChild("WordDelimiter");
        delimiter.setProperty("types", "types.txt");
        delimiter.addChild("types.txt").addChild("jcr:content")
                .setProperty("jcr:data", "_ => ALPHANUM\n- => ALPHANUM");

        Document custom = MongotSearchIndexDefinitionBuilder.build(definition(builder))
                .getList("analyzers", Document.class).get(0);

        assertEquals(List.of(new Document("type", "wordDelimiterGraph")
                        .append("characterTypes", new Document("_", "ALPHANUM")
                                .append("-", "ALPHANUM"))),
                custom.getList("tokenFilters", Document.class));
    }

    @Test
    public void mapsCompoundFingerprintAndMinHashFilters() {
        IndexDefinitionBuilder builder = builder();
        Tree analyzer = builder.getBuilderTree()
                .addChild(FulltextIndexConstants.ANALYZERS)
                .addChild(FulltextIndexConstants.ANL_DEFAULT);
        analyzer.addChild(FulltextIndexConstants.ANL_TOKENIZER)
                .setProperty(FulltextIndexConstants.ANL_NAME, "Standard");
        Tree filters = analyzer.addChild(FulltextIndexConstants.ANL_FILTERS);
        filters.setOrderableChildren(true);

        Tree commonGrams = filters.addChild("CommonGrams");
        commonGrams.setProperty("words", "common.txt");
        commonGrams.addChild("common.txt").addChild("jcr:content")
                .setProperty("jcr:data", "is\nthe");
        Tree compound = filters.addChild("DictionaryCompoundWord");
        compound.setProperty("dictionary", "dictionary.txt");
        compound.addChild("dictionary.txt").addChild("jcr:content")
                .setProperty("jcr:data", "Donau\ndampf\nschiff");
        Tree decompounder = filters.addChild("dictionary_decompounder");
        decompounder.setProperty("word_list", "words.txt");
        decompounder.addChild("words.txt").addChild("jcr:content")
                .setProperty("jcr:data", "meer\nschiff");
        filters.addChild("fingerprint").setProperty("max_output_size", "10");
        Tree minHash = filters.addChild("min_hash");
        minHash.setProperty("hash_count", "1");
        minHash.setProperty("bucket_count", "512");
        minHash.setProperty("hash_set_size", "2");
        minHash.setProperty("with_rotation", "false");

        Document custom = MongotSearchIndexDefinitionBuilder.build(definition(builder))
                .getList("analyzers", Document.class).get(0);

        assertEquals(List.of(
                        new Document("type", "commonGrams")
                                .append("commonWords", List.of("is", "the"))
                                .append("ignoreCase", false),
                        new Document("type", "dictionaryCompoundWord")
                                .append("dictionary", List.of("Donau", "dampf", "schiff"))
                                .append("ignoreCase", false),
                        new Document("type", "dictionaryCompoundWord")
                                .append("dictionary", List.of("meer", "schiff"))
                                .append("ignoreCase", false),
                        new Document("type", "fingerprint")
                                .append("maxOutputSize", 10)
                                .append("separator", " "),
                        new Document("type", "minHash")
                                .append("hashCount", 1)
                                .append("bucketCount", 512)
                                .append("hashSetSize", 2)
                                .append("withRotation", false)),
                custom.getList("tokenFilters", Document.class));
    }

    @Test
    public void mapsHunspellResources() {
        IndexDefinitionBuilder builder = builder();
        Tree analyzer = builder.getBuilderTree()
                .addChild(FulltextIndexConstants.ANALYZERS)
                .addChild(FulltextIndexConstants.ANL_DEFAULT);
        analyzer.addChild(FulltextIndexConstants.ANL_TOKENIZER)
                .setProperty(FulltextIndexConstants.ANL_NAME, "Standard");
        Tree hunspell = analyzer.addChild(FulltextIndexConstants.ANL_FILTERS)
                .addChild("HunspellStem");
        hunspell.setProperty("affix", "fr.aff");
        hunspell.addChild("fr.aff").addChild("jcr:content")
                .setProperty("jcr:data", "SET UTF-8\n");
        hunspell.setProperty("dictionary", "fr.dic");
        hunspell.addChild("fr.dic").addChild("jcr:content")
                .setProperty("jcr:data", "1\nmanger/A\n");

        Document custom = MongotSearchIndexDefinitionBuilder.build(definition(builder))
                .getList("analyzers", Document.class).get(0);

        assertEquals(List.of(new Document("type", "hunspellStem")
                        .append("affix", "SET UTF-8\n")
                        .append("dictionaries", List.of("1\nmanger/A\n"))
                        .append("ignoreCase", false)
                        .append("longestOnly", false)),
                custom.getList("tokenFilters", Document.class));
    }

    @Test
    public void mapsDutchKpStemmer() {
        IndexDefinitionBuilder builder = builder();
        Tree analyzer = builder.getBuilderTree()
                .addChild(FulltextIndexConstants.ANALYZERS)
                .addChild(FulltextIndexConstants.ANL_DEFAULT);
        analyzer.addChild(FulltextIndexConstants.ANL_TOKENIZER)
                .setProperty(FulltextIndexConstants.ANL_NAME, "Standard");
        analyzer.addChild(FulltextIndexConstants.ANL_FILTERS)
                .addChild("stemmer").setProperty("language", "dutch_kp");

        Document custom = MongotSearchIndexDefinitionBuilder.build(definition(builder))
                .getList("analyzers", Document.class).get(0);

        assertEquals(List.of(new Document("type", "snowballStemming")
                        .append("stemmerName", "kp")),
                custom.getList("tokenFilters", Document.class));
    }

    @Test
    public void mapsOakSetAndPatternTokenFilters() {
        IndexDefinitionBuilder builder = builder();
        Tree analyzer = builder.getBuilderTree()
                .addChild(FulltextIndexConstants.ANALYZERS)
                .addChild(FulltextIndexConstants.ANL_DEFAULT);
        analyzer.addChild(FulltextIndexConstants.ANL_TOKENIZER)
                .setProperty(FulltextIndexConstants.ANL_NAME, "Standard");
        Tree filters = analyzer.addChild(FulltextIndexConstants.ANL_FILTERS);
        filters.setOrderableChildren(true);
        Tree marker = filters.addChild("KeywordMarker");
        marker.setProperty("protected", "protected.txt");
        marker.addChild("protected.txt").addChild("jcr:content")
                .setProperty("jcr:data", "running");
        Tree keepWord = filters.addChild("KeepWord");
        keepWord.setProperty("words", "words.txt");
        keepWord.addChild("words.txt").addChild("jcr:content")
                .setProperty("jcr:data", "dog\nfox");
        Tree type = filters.addChild("Type");
        type.setProperty("types", "types.txt");
        type.setProperty("useWhitelist", true);
        type.addChild("types.txt").addChild("jcr:content")
                .setProperty("jcr:data", "<NUM>");
        Tree patternCapture = filters.addChild("PatternCaptureGroup");
        patternCapture.setProperty("pattern", "(([a-z]+)(\\d*))");

        Document custom = MongotSearchIndexDefinitionBuilder.build(definition(builder))
                .getList("analyzers", Document.class).get(0);

        assertEquals(List.of(
                        new Document("type", "keywordMarker")
                                .append("keywords", List.of("running"))
                                .append("ignoreCase", false),
                        new Document("type", "keepWord")
                                .append("words", List.of("dog", "fox"))
                                .append("ignoreCase", false),
                        new Document("type", "type")
                                .append("types", List.of("<NUM>"))
                                .append("keep", true),
                        new Document("type", "patternCaptureGroup")
                                .append("patterns", List.of("(([a-z]+)(\\d*))"))
                                .append("preserveOriginal", true)),
                custom.getList("tokenFilters", Document.class));
    }

    @Test
    public void mapsElasticKeepTypesAlias() {
        IndexDefinitionBuilder builder = builder();
        Tree analyzer = builder.getBuilderTree()
                .addChild(FulltextIndexConstants.ANALYZERS)
                .addChild(FulltextIndexConstants.ANL_DEFAULT);
        analyzer.addChild(FulltextIndexConstants.ANL_TOKENIZER)
                .setProperty(FulltextIndexConstants.ANL_NAME, "Standard");
        analyzer.addChild(FulltextIndexConstants.ANL_FILTERS)
                .addChild("keep_types").setProperty("types", "<NUM>");

        Document custom = MongotSearchIndexDefinitionBuilder.build(definition(builder))
                .getList("analyzers", Document.class).get(0);

        assertEquals(List.of(new Document("type", "type")
                        .append("types", List.of("<NUM>"))
                        .append("keep", true)),
                custom.getList("tokenFilters", Document.class));
    }

    @Test
    public void mapsClassicAnalyzerComponentsToPublicPrimitives() {
        IndexDefinitionBuilder builder = builder();
        Tree analyzer = builder.getBuilderTree()
                .addChild(FulltextIndexConstants.ANALYZERS)
                .addChild(FulltextIndexConstants.ANL_DEFAULT);
        analyzer.addChild(FulltextIndexConstants.ANL_TOKENIZER)
                .setProperty(FulltextIndexConstants.ANL_NAME, "Classic");
        analyzer.addChild(FulltextIndexConstants.ANL_FILTERS).addChild("Classic");

        Document custom = MongotSearchIndexDefinitionBuilder.build(definition(builder))
                .getList("analyzers", Document.class).get(0);

        assertEquals(new Document("type", "uaxUrlEmail"), custom.get("tokenizer"));
        assertEquals(List.of(new Document("type", "regex")
                        .append("pattern", "\\.")
                        .append("replacement", "")
                        .append("matches", "all")),
                custom.getList("tokenFilters", Document.class));
    }

    @Test
    public void mapsOakSynonymsToQueryTimeSource() {
        IndexDefinitionBuilder builder = builder();
        Tree analyzer = builder.getBuilderTree()
                .addChild(FulltextIndexConstants.ANALYZERS)
                .addChild(FulltextIndexConstants.ANL_DEFAULT);
        analyzer.addChild(FulltextIndexConstants.ANL_TOKENIZER)
                .setProperty(FulltextIndexConstants.ANL_NAME, "Standard");
        Tree synonym = analyzer.addChild(FulltextIndexConstants.ANL_FILTERS)
                .addChild("Synonym");
        synonym.setProperty("synonyms", "syn.txt");
        synonym.addChild("syn.txt").addChild("jcr:content")
                .setProperty("jcr:data", "# comment\nplane, airplane, aircraft\nflies => scars");
        MongotIndexDefinition definition = definition(builder);

        Document searchDefinition = MongotSearchIndexDefinitionBuilder.build(definition);

        assertEquals(List.of(new Document("name", "oak_synonyms")
                        .append("source", new Document("collection",
                                definition.getCollectionName() + "_synonyms"))
                        .append("analyzer", "oak_default")),
                searchDefinition.getList("synonyms", Document.class));
        assertEquals(List.of(
                        new Document("_id", 0).append("mappingType", "equivalent")
                                .append("synonyms", List.of("plane", "airplane", "aircraft")),
                        new Document("_id", 1).append("mappingType", "equivalent")
                                .append("synonyms", List.of("flies", "scars"))),
                MongotSearchIndexDefinitionBuilder.synonymDocuments(definition));
    }

    @Test
    public void mapsSimilarityVectorField() {
        IndexDefinitionBuilder builder = builder();
        Tree vector = builder.indexRule("nt:base").property("fv")
                .useInSimilarity(true).similaritySearchDenseVectorSize(3).getBuilderTree();
        vector.setProperty("similarityMetric", "cosine");

        Document fields = MongotSearchIndexDefinitionBuilder.build(definition(builder))
                .get("mappings", Document.class).get("fields", Document.class);
        String fieldName = FieldNames.createSimilarityFieldName(
                MongoFieldNames.encodeProperty("fv"));

        assertNotNull(fields);
        assertEquals(new Document("type", "vector")
                        .append("numDimensions", 3)
                        .append("similarity", "cosine"),
                fields.get(fieldName, Document.class));
    }

    private static IndexDefinitionBuilder builder() {
        return new IndexDefinitionBuilder() {
            @Override
            protected String getIndexType() {
                return MongotIndexDefinition.TYPE_MONGOT;
            }
        };
    }

    private static MongotIndexDefinition definition(IndexDefinitionBuilder builder) {
        return new MongotIndexDefinition(EmptyNodeState.EMPTY_NODE, builder.build(),
                "/oak:index/analyzer");
    }
}
