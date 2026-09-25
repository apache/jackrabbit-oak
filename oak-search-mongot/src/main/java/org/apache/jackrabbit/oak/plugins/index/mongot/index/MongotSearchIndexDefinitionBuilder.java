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

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.index.mongot.MongotIndexDefinition;
import org.apache.jackrabbit.oak.plugins.index.search.FieldNames;
import org.apache.jackrabbit.oak.plugins.index.search.FulltextIndexConstants;
import org.apache.jackrabbit.oak.plugins.index.search.IndexDefinition;
import org.apache.jackrabbit.oak.plugins.index.search.PropertyDefinition;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.bson.Document;

final class MongotSearchIndexDefinitionBuilder {

    private static final String CUSTOM_ANALYZER = "oak_default";
    private static final Pattern MAPPING = Pattern.compile(
            "^\\s*\\\"((?:\\\\.|[^\\\"])*)\\\"\\s*=>\\s*\\\"((?:\\\\.|[^\\\"])*)\\\"\\s*$");
    private static final Pattern WORD_DELIMITER_TYPE = Pattern.compile(
            "^\\s*(\\S)\\s*=>\\s*(LOWER|UPPER|ALPHA|DIGIT|ALPHANUM|SUBWORD_DELIM)\\s*$",
            Pattern.CASE_INSENSITIVE);

    private static final Set<String> STOCK_ANALYZERS = Set.of(
            "standard", "stop", "simple", "whitespace", "keyword", "arabic", "armenian",
            "basque", "bengali", "brazilian", "bulgarian", "catalan", "cjk", "chinese",
            "czech", "danish", "dutch", "english", "finnish", "french", "galician",
            "german", "greek", "hindi", "hungarian", "indonesian", "irish", "italian",
            "japanese", "korean", "kuromoji", "latvian", "lithuanian", "morfologik",
            "nori", "norwegian", "persian", "polish", "portuguese", "romanian", "russian",
            "smartcn", "sorani", "spanish", "swedish", "thai", "turkish", "ukrainian");
    private static final Set<String> SNOWBALL_STEMMERS = Set.of(
            "arabic", "armenian", "basque", "catalan", "danish", "dutch", "english",
            "estonian", "finnish", "french", "german", "german2", "hungarian", "irish",
            "italian", "lithuanian", "norwegian", "porter", "portuguese", "romanian",
            "russian", "spanish", "swedish", "turkish");
    private static final Map<String, String> ANALYZER_ALIASES = Map.of(
            "smartchinese", "smartcn");

    private MongotSearchIndexDefinitionBuilder() {
    }

    static Document build(MongotIndexDefinition definition) {
        Analyzer analyzer = analyzer(definition.getDefinitionNodeState());
        Document fields = new Document();
        fields.append(MongoFieldNames.SYNC_TOKEN, new Document("type", "token"));
        if (definition.isSuggestEnabled()) {
            Document autocomplete = new Document("type", "autocomplete")
                    .append("tokenization", "edgeGram")
                    .append("minGrams", 2)
                    .append("maxGrams", 15)
                    .append("foldDiacritics", true)
                    // Mongot autocomplete fields reject analyzers that produce graph token
                    // streams. Keep suggestion indexing independent of Oak full-text chains.
                    .append("analyzer", "lucene.standard");
            fields.append(MongoFieldNames.SUGGEST, autocomplete);
        }
        if (definition.isSpellcheckEnabled()) {
            fields.append(MongoFieldNames.SPELLCHECK, new Document("type", "string"));
        }
        if (!definition.isFullTextStored()) {
            Document fullText = new Document("type", "string").append("store", false);
            if (analyzer.name() != null) {
                fullText.append("analyzer", analyzer.name())
                        .append("searchAnalyzer", analyzer.searchName());
            }
            fields.append(MongoFieldNames.FULLTEXT, fullText);
        }
        for (IndexDefinition.IndexingRule rule : definition.getDefinedRules()) {
            for (PropertyDefinition property : rule.getSimilarityProperties()) {
                fields.append(FieldNames.createSimilarityFieldName(
                                MongoFieldNames.encodeProperty(property.name)),
                        new Document("type", "vector")
                                .append("numDimensions",
                                        property.getSimilaritySearchDenseVectorSize())
                                .append("similarity",
                                        similarityMetric(definition, rule, property)));
            }
        }

        Document mappings = new Document("dynamic", true);
        if (!fields.isEmpty()) {
            mappings.append("fields", fields);
        }
        Document result = new Document("mappings", mappings);
        if (definition.isStoredSource()) {
            // Mongot reports the include list sorted. Emitting the same order keeps
            // MongotSearchIndexManager from resubmitting, and so rebuilding, the index.
            result.append("storedSource", new Document("include",
                    MongoFieldNames.POST_SEARCH_FIELDS.stream().sorted().toList()));
        }
        if (analyzer.name() != null) {
            result.append("analyzer", analyzer.name())
                    .append("searchAnalyzer", analyzer.searchName());
        }
        if (analyzer.custom() != null) {
            result.append("analyzers", List.of(analyzer.custom()));
        }
        if (definition.hasSynonyms()) {
            result.append("synonyms", List.of(new Document("name",
                            MongotIndexDefinition.SYNONYM_MAPPING_NAME)
                    .append("source", new Document("collection",
                            definition.getSynonymCollectionName()))
                    .append("analyzer", analyzer.name())));
        }
        return result;
    }

    private static String similarityMetric(MongotIndexDefinition definition,
                                           IndexDefinition.IndexingRule rule,
                                           PropertyDefinition property) {
        NodeState propertyState = definition.getDefinitionNodeState()
                .getChildNode(FulltextIndexConstants.INDEX_RULES)
                .getChildNode(rule.getNodeTypeName())
                .getChildNode(FulltextIndexConstants.PROP_NODE)
                .getChildNode(property.nodeName);
        String metric = propertyState.hasProperty("similarityMetric")
                ? propertyState.getString("similarityMetric")
                : "euclidean";
        return switch (metric) {
            case "l2_norm" -> "euclidean";
            case "dot_product" -> "dotProduct";
            case "euclidean", "cosine", "dotProduct" -> metric;
            default -> throw new IllegalArgumentException(
                    "unsupported Mongot vector similarity metric: " + metric);
        };
    }

    private static Analyzer analyzer(NodeState definition) {
        NodeState analyzers = definition.getChildNode(FulltextIndexConstants.ANALYZERS);
        if (!analyzers.exists()) {
            return oakDefaultAnalyzer();
        }
        NodeState analyzer = analyzers.getChildNode(FulltextIndexConstants.ANL_DEFAULT);
        if (!analyzer.exists()) {
            return oakDefaultAnalyzer();
        }
        if (analyzer.hasProperty(FulltextIndexConstants.ANL_CLASS)) {
            if (analyzer.getChildNode("stopwords").exists()) {
                return configuredBuiltInAnalyzer(analyzer);
            }
            String builtIn = builtInAnalyzer(analyzer);
            return new Analyzer(builtIn, builtIn, null);
        }
        if (analyzer.hasProperty(FulltextIndexConstants.ANL_NAME)) {
            String builtIn = builtInAnalyzerName(analyzer.getString(FulltextIndexConstants.ANL_NAME));
            return new Analyzer(builtIn, builtIn, null);
        }
        if (isCjkAnalyzer(analyzer)) {
            return new Analyzer("lucene.cjk", "lucene.cjk", null);
        }
        Document custom = customAnalyzer(analyzer);
        return new Analyzer(CUSTOM_ANALYZER, CUSTOM_ANALYZER, custom);
    }

    private static Analyzer oakDefaultAnalyzer() {
        Document custom = new Document("name", CUSTOM_ANALYZER)
                .append("tokenizer", new Document("type", "standard"))
                .append("tokenFilters", List.of(
                        new Document("type", "lowercase"),
                        new Document("type", "wordDelimiterGraph")
                                .append("delimiterOptions",
                                        new Document("preserveOriginal", true)
                                                .append("splitOnCaseChange", false)
                                                .append("splitOnNumerics", false))));
        return new Analyzer(CUSTOM_ANALYZER, "lucene.standard", custom);
    }

    private static Analyzer configuredBuiltInAnalyzer(NodeState analyzer) {
        String className = analyzer.getString(FulltextIndexConstants.ANL_CLASS);
        if (!"org.apache.lucene.analysis.en.EnglishAnalyzer".equals(className)) {
            throw unsupportedAnalyzer(className + " with custom stopwords");
        }
        List<String> stopwords = resourceLines(analyzer.getChildNode("stopwords"));
        Document custom = new Document("name", CUSTOM_ANALYZER)
                .append("tokenizer", new Document("type", "standard"))
                .append("tokenFilters", List.of(
                        new Document("type", "englishPossessive"),
                        new Document("type", "lowercase"),
                        new Document("type", "stopword")
                                .append("tokens", stopwords).append("ignoreCase", true),
                        new Document("type", "porterStemming")));
        return new Analyzer(CUSTOM_ANALYZER, CUSTOM_ANALYZER, custom);
    }

    private static String builtInAnalyzer(NodeState analyzer) {
        String className = analyzer.getProperty(FulltextIndexConstants.ANL_CLASS).getValue(Type.STRING);
        String simpleName = className.substring(className.lastIndexOf('.') + 1);
        if (!simpleName.endsWith("Analyzer")) {
            throw unsupportedAnalyzer(className);
        }
        String shortName = simpleName.substring(0, simpleName.length() - "Analyzer".length())
                .toLowerCase(Locale.ROOT);
        shortName = ANALYZER_ALIASES.getOrDefault(shortName, shortName);
        if (!className.startsWith("org.apache.lucene.analysis.") || !STOCK_ANALYZERS.contains(shortName)) {
            throw unsupportedAnalyzer(className);
        }
        return "lucene." + shortName;
    }

    private static String builtInAnalyzerName(String name) {
        String shortName = ANALYZER_ALIASES.getOrDefault(
                name.toLowerCase(Locale.ROOT), name.toLowerCase(Locale.ROOT));
        if (!STOCK_ANALYZERS.contains(shortName)) {
            throw unsupportedAnalyzer(name);
        }
        return "lucene." + shortName;
    }

    private static Document customAnalyzer(NodeState analyzer) {
        NodeState tokenizer = analyzer.getChildNode(FulltextIndexConstants.ANL_TOKENIZER);
        if (!tokenizer.exists() || !tokenizer.hasProperty(FulltextIndexConstants.ANL_NAME)) {
            throw new IllegalArgumentException("Mongot composed analyzer requires a tokenizer");
        }
        Document custom = new Document("name", CUSTOM_ANALYZER)
                .append("tokenizer", tokenizer(tokenizer));
        List<Document> charFilters = components(
                analyzer.getChildNode(FulltextIndexConstants.ANL_CHAR_FILTERS), true);
        List<Document> tokenFilters = components(
                analyzer.getChildNode(FulltextIndexConstants.ANL_FILTERS), false);
        if (!charFilters.isEmpty()) {
            custom.append("charFilters", charFilters);
        }
        if (!tokenFilters.isEmpty()) {
            custom.append("tokenFilters", tokenFilters);
        }
        return custom;
    }

    private static Document tokenizer(NodeState tokenizer) {
        String name = componentName(tokenizer, null);
        switch (name) {
            case "standard":
            case "whitespace":
            case "keyword":
                return new Document("type", name);
            case "classic":
                return new Document("type", "uaxUrlEmail");
            case "ngram":
                return grams("nGram", tokenizer);
            case "edgengram":
                return grams("edgeGram", tokenizer);
            case "pattern":
                return patternTokenizer(tokenizer);
            default:
                throw unsupportedComponent("tokenizer", name);
        }
    }

    private static Document patternTokenizer(NodeState tokenizer) {
        String pattern = requiredString(tokenizer, "pattern");
        long group = tokenizer.hasProperty("group") ? tokenizer.getLong("group") : -1;
        if (group < 0) {
            return new Document("type", "regexSplit").append("pattern", pattern);
        }
        return new Document("type", "regexCaptureGroup")
                .append("pattern", pattern)
                .append("group", Math.toIntExact(group));
    }

    private static Document grams(String type, NodeState component) {
        return new Document("type", type)
                .append("minGram", integer(component, "minGramSize", "minGram"))
                .append("maxGram", integer(component, "maxGramSize", "maxGram"));
    }

    private static List<Document> components(NodeState parent, boolean characterFilters) {
        List<Document> result = new ArrayList<>();
        if (!parent.exists()) {
            return result;
        }
        boolean hasSynonyms = !characterFilters && hasComponent(parent, "synonym");
        for (String childName : orderedChildNames(parent)) {
            NodeState component = parent.getChildNode(childName);
            String name = componentName(component, childName);
            if (hasSynonyms && "worddelimiter".equals(name)) {
                continue;
            }
            Document translated = characterFilters
                    ? characterFilter(component, childName)
                    : tokenFilter(component, childName);
            if (translated != null) {
                result.add(translated);
            }
        }
        return result;
    }

    private static boolean hasComponent(NodeState parent, String expected) {
        for (String childName : parent.getChildNodeNames()) {
            if (expected.equals(componentName(parent.getChildNode(childName), childName))) {
                return true;
            }
        }
        return false;
    }

    private static List<String> orderedChildNames(NodeState parent) {
        List<String> result = new ArrayList<>();
        PropertyState order = parent.getProperty(":childOrder");
        if (order != null) {
            order.getValue(Type.NAMES).forEach(result::add);
        }
        for (String name : parent.getChildNodeNames()) {
            if (!result.contains(name)) {
                result.add(name);
            }
        }
        return result;
    }

    private static Document characterFilter(NodeState component, String childName) {
        String name = componentName(component, childName);
        switch (name) {
            case "htmlstrip":
                return new Document("type", "htmlStrip").append("ignoredTags", List.of());
            case "mapping":
                Map<String, String> mappings = mapping(component);
                return mappings.isEmpty() ? null
                        : new Document("type", "mapping").append("mappings", mappings);
            case "persian":
                return new Document("type", "persian");
            case "icunormalize":
                return new Document("type", "icuNormalize");
            case "patternreplace":
                return new Document("type", "patternReplace")
                        .append("pattern", requiredString(component, "pattern"))
                        .append("replacement", requiredString(component, "replacement"));
            default:
                throw unsupportedComponent("character filter", name);
        }
    }

    private static Document tokenFilter(NodeState component, String childName) {
        String name = componentName(component, childName);
        switch (name) {
            case "lowercase":
                return new Document("type", "lowercase");
            case "porterstem":
            case "porterstemming":
                return new Document("type", "porterStemming");
            case "kstem":
            case "kstemming":
                return new Document("type", "kStemming");
            case "asciifolding":
                return new Document("type", "asciiFolding")
                        .append("originalTokens", booleanValue(component, "preserveOriginal", false)
                                ? "include" : "omit");
            case "stop":
            case "stopword":
                return new Document("type", "stopword")
                        .append("tokens", resourceLines(component, "words"))
                        .append("ignoreCase", !component.hasProperty("ignoreCase")
                                || booleanValue(component, "ignoreCase", false));
            case "trim":
            case "reverse":
                return new Document("type", name);
            case "removeduplicates":
                return new Document("type", "removeDuplicates");
            case "keywordrepeat":
                return new Document("type", "keywordRepeat");
            case "keywordmarker":
                return new Document("type", "keywordMarker")
                        .append("keywords", resourceLines(component, "protected"))
                        .append("ignoreCase", booleanValue(component, "ignoreCase", false));
            case "keepword":
                return new Document("type", "keepWord")
                        .append("words", resourceLines(component, "words"))
                        .append("ignoreCase", booleanValue(component, "ignoreCase", false));
            case "type":
                return new Document("type", "type")
                        .append("types", resourceOrValues(component, "types"))
                        .append("keep", booleanValue(component, "useWhitelist", false));
            case "keeptypes":
                return new Document("type", "type")
                        .append("types", resourceOrValues(component, "types"))
                        .append("keep", true);
            case "patterncapturegroup":
                return new Document("type", "patternCaptureGroup")
                        .append("patterns", List.of(requiredString(component, "pattern")))
                        .append("preserveOriginal",
                                booleanValue(component, "preserveOriginal", true));
            case "commongrams":
                return new Document("type", "commonGrams")
                        .append("commonWords", resourceLines(component, "words"))
                        .append("ignoreCase", booleanValue(component, "ignoreCase", false));
            case "dictionarycompoundword":
                return dictionaryCompoundWord(component, "dictionary");
            case "dictionarydecompounder":
                return dictionaryCompoundWord(component, "word_list");
            case "fingerprint":
                return new Document("type", "fingerprint")
                        .append("maxOutputSize", integerValue(component, 1024,
                                "maxOutputSize", "max_output_size"))
                        .append("separator", stringValue(component, " ", "separator"));
            case "minhash":
                return new Document("type", "minHash")
                        .append("hashCount", integerValue(component, 1,
                                "hashCount", "hash_count"))
                        .append("bucketCount", integerValue(component, 512,
                                "bucketCount", "bucket_count"))
                        .append("hashSetSize", integerValue(component, 1,
                                "hashSetSize", "hash_set_size"))
                        .append("withRotation", booleanValueAny(component, true,
                                "withRotation", "with_rotation"));
            case "hunspellstem":
                return new Document("type", "hunspellStem")
                        .append("affix", resourceText(component, "affix"))
                        .append("dictionaries", resourceContents(component, "dictionary"))
                        .append("ignoreCase", booleanValue(component, "ignoreCase", false))
                        .append("longestOnly", booleanValue(component, "longestOnly", false));
            case "standard":
                return null;
            case "synonym":
            case "synonymgraph":
                return null;
            case "worddelimiter":
            case "worddelimitergraph":
                return wordDelimiter(component, name);
            case "spanishlightstem":
                return new Document("type", "snowballStemming")
                        .append("stemmerName", "spanish");
            case "frenchlightstem":
                return new Document("type", "snowballStemming")
                        .append("stemmerName", "french");
            case "germanlightstem":
                return new Document("type", "snowballStemming")
                        .append("stemmerName", "german");
            case "italianlightstem":
                return new Document("type", "snowballStemming")
                        .append("stemmerName", "italian");
            case "germannormalization":
                return new Document("type", "asciiFolding")
                        .append("originalTokens", "omit");
            case "elision":
                return elision(component);
            case "apostrophe":
                return regex("['’].*$", "");
            case "delimitedpayload":
                return regex("\\|.*$", "");
            case "classic":
                return regex("\\.", "", "all");
            case "length":
                return new Document("type", "length")
                        .append("min", integer(component, "min", "min"))
                        .append("max", integer(component, "max", "max"));
            case "ngram":
                return tokenGrams("nGram", component);
            case "edgengram":
                return tokenGrams("edgeGram", component);
            case "shingle":
                if (!hasProperty(component, "outputUnigrams", "output_unigrams")
                        || booleanValueAny(component, true,
                        "outputUnigrams", "output_unigrams")) {
                    throw unsupportedComponent("token filter", name + " with outputUnigrams");
                }
                return new Document("type", "shingle")
                        .append("minShingleSize", requiredInteger(component,
                                "minShingleSize", "minShingleSize", "min_shingle_size"))
                        .append("maxShingleSize", requiredInteger(component,
                                "maxShingleSize", "maxShingleSize", "max_shingle_size"));
            case "limittokencount":
                return new Document("type", "limitTokenCount")
                        .append("maxTokenCount", integer(component,
                                "maxTokenCount", "maxTokenCount"));
            case "snowballporter":
            case "snowballstemming":
                String language = requiredString(component, "language").toLowerCase(Locale.ROOT);
                if (!SNOWBALL_STEMMERS.contains(language)) {
                    throw unsupportedComponent("token filter", name + " language " + language);
                }
                return new Document("type", "snowballStemming")
                        .append("stemmerName", language);
            case "stemmer":
                return stemmer(component);
            default:
                throw unsupportedComponent("token filter", name);
        }
    }

    private static Document wordDelimiter(NodeState component, String name) {
        Document options = new Document();
        appendBoolean(component, options, "generateWordParts", "generateWordParts");
        appendBoolean(component, options, "generateNumberParts", "generateNumberParts");
        appendBoolean(component, options, "catenateWords", "concatenateWords");
        appendBoolean(component, options, "catenateNumbers", "concatenateNumbers");
        appendBoolean(component, options, "catenateAll", "concatenateAll");
        appendBoolean(component, options, "preserveOriginal", "preserveOriginal");
        appendBoolean(component, options, "splitOnCaseChange", "splitOnCaseChange");
        appendBoolean(component, options, "splitOnNumerics", "splitOnNumerics");
        appendBoolean(component, options, "stemEnglishPossessive", "stemEnglishPossessive");
        appendBoolean(component, options, "ignoreKeywords", "ignoreKeywords");

        Document result = new Document("type", "wordDelimiterGraph");
        if (!options.isEmpty()) {
            result.append("delimiterOptions", options);
        }
        if (component.hasProperty("protected")) {
            result.append("protectedWords", new Document("words",
                    resourceLines(component, "protected"))
                    .append("ignoreCase", booleanValue(component, "ignoreCase", true)));
        }
        if (component.hasProperty("types")) {
            result.append("characterTypes", wordDelimiterTypes(component, name));
        }
        return result;
    }

    private static Document dictionaryCompoundWord(NodeState component, String propertyName) {
        return new Document("type", "dictionaryCompoundWord")
                .append("dictionary", resourceLines(component, propertyName))
                .append("ignoreCase", booleanValue(component, "ignoreCase", false));
    }

    private static Document stemmer(NodeState component) {
        String language = requiredString(component, "language").toLowerCase(Locale.ROOT);
        String normalized = language.replace("_", "").replace("-", "");
        if ("dutchkp".equals(normalized) || "kp".equals(normalized)) {
            return new Document("type", "snowballStemming").append("stemmerName", "kp");
        }
        if (!SNOWBALL_STEMMERS.contains(language)) {
            throw unsupportedComponent("token filter", "stemmer language " + language);
        }
        return new Document("type", "snowballStemming").append("stemmerName", language);
    }

    private static Document wordDelimiterTypes(NodeState component, String name) {
        Document types = new Document();
        for (String line : resourceText(component, "types").split("\\R")) {
            String trimmed = line.trim();
            if (trimmed.isEmpty() || trimmed.startsWith("#")) {
                continue;
            }
            Matcher matcher = WORD_DELIMITER_TYPE.matcher(trimmed);
            if (!matcher.matches()) {
                throw unsupportedComponent("token filter", name + " type rule " + trimmed);
            }
            types.append(matcher.group(1), matcher.group(2).toUpperCase(Locale.ROOT));
        }
        if (types.isEmpty()) {
            throw unsupportedComponent("token filter", name + " with empty custom types");
        }
        return types;
    }

    private static void appendBoolean(NodeState source, Document target,
                                      String oakName, String mongoName) {
        if (source.hasProperty(oakName)) {
            target.append(mongoName, booleanValue(source, oakName, false));
        }
    }

    private static Document elision(NodeState component) {
        List<String> articles = resourceLines(component, "articles");
        String alternatives = articles.stream()
                .map(Pattern::quote)
                .reduce((left, right) -> left + "|" + right)
                .orElseThrow();
        return regex("(?i)^(?:" + alternatives + ")['’]", "");
    }

    private static Document regex(String pattern, String replacement) {
        return regex(pattern, replacement, "first");
    }

    private static Document regex(String pattern, String replacement, String matches) {
        return new Document("type", "regex")
                .append("pattern", pattern)
                .append("replacement", replacement)
                .append("matches", matches);
    }

    private static boolean isCjkAnalyzer(NodeState analyzer) {
        NodeState filters = analyzer.getChildNode(FulltextIndexConstants.ANL_FILTERS);
        boolean cjkBigram = false;
        boolean cjkWidth = false;
        for (String childName : filters.getChildNodeNames()) {
            String name = componentName(filters.getChildNode(childName), childName);
            cjkBigram |= "cjkbigram".equals(name);
            cjkWidth |= "cjkwidth".equals(name);
        }
        return cjkBigram && cjkWidth;
    }

    private static Document tokenGrams(String type, NodeState component) {
        Document result = grams(type, component);
        if (component.hasProperty("preserveOriginal")
                && booleanValue(component, "preserveOriginal", false)) {
            result.append("termNotInBounds", "include");
        }
        return result;
    }

    private static Map<String, String> mapping(NodeState component) {
        LinkedHashMap<String, String> result = new LinkedHashMap<>();
        if (!component.hasProperty("mapping")) {
            return result;
        }
        for (String line : resourceText(component, "mapping").split("\\R")) {
            String trimmed = line.trim();
            if (trimmed.isEmpty() || trimmed.startsWith("#")) {
                continue;
            }
            Matcher matcher = MAPPING.matcher(trimmed);
            if (!matcher.matches()) {
                throw new IllegalArgumentException("Unsupported Oak mapping rule: " + trimmed);
            }
            result.put(unescape(matcher.group(1)), unescape(matcher.group(2)));
        }
        return result;
    }

    static List<Document> synonymDocuments(MongotIndexDefinition definition) {
        NodeState filters = definition.getDefinitionNodeState()
                .getChildNode(FulltextIndexConstants.ANALYZERS)
                .getChildNode(FulltextIndexConstants.ANL_DEFAULT)
                .getChildNode(FulltextIndexConstants.ANL_FILTERS);
        List<Document> documents = new ArrayList<>();
        for (String childName : orderedChildNames(filters)) {
            NodeState filter = filters.getChildNode(childName);
            String name = componentName(filter, childName);
            if (!("synonym".equals(name) || "synonymgraph".equals(name))) {
                continue;
            }
            if (filter.hasProperty("format")
                    && !"solr".equalsIgnoreCase(filter.getString("format"))) {
                throw unsupportedComponent("token filter", name + " format "
                        + filter.getString("format"));
            }
            for (String line : resourceText(filter, "synonyms").split("\\R")) {
                String trimmed = line.trim();
                if (trimmed.isEmpty() || trimmed.startsWith("#")) {
                    continue;
                }
                LinkedHashSet<String> terms = new LinkedHashSet<>();
                for (String side : trimmed.split("=>", -1)) {
                    for (String term : side.split(",")) {
                        String value = term.trim();
                        if (value.chars().anyMatch(Character::isLetterOrDigit)) {
                            terms.add(value);
                        }
                    }
                }
                if (terms.size() > 1) {
                    documents.add(new Document("_id", documents.size())
                            .append("mappingType", "equivalent")
                            .append("synonyms", new ArrayList<>(terms)));
                }
            }
        }
        if (documents.isEmpty()) {
            throw new IllegalArgumentException("Mongot synonym filter requires mappings");
        }
        return documents;
    }

    private static List<String> resourceLines(NodeState component, String propertyName) {
        List<String> lines = new ArrayList<>();
        boolean snowball = component.hasProperty("format")
                && "snowball".equalsIgnoreCase(component.getString("format"));
        for (String line : resourceText(component, propertyName).split("\\R")) {
            String value = snowball ? line.split("\\|", 2)[0].trim() : line.trim();
            if (!value.isEmpty() && !value.startsWith("#")) {
                lines.add(value);
            }
        }
        if (lines.isEmpty()) {
            throw new IllegalArgumentException("Mongot stopword filter requires tokens");
        }
        return lines;
    }

    private static List<String> resourceOrValues(NodeState component, String propertyName) {
        String configured = requiredString(component, propertyName);
        boolean resources = true;
        for (String value : configured.split(",")) {
            PropertyState data = component.getChildNode(value.trim())
                    .getChildNode("jcr:content").getProperty("jcr:data");
            resources &= data != null;
        }
        if (resources) {
            return resourceLines(component, propertyName);
        }
        List<String> values = new ArrayList<>();
        for (String value : configured.split("[,\\s]+")) {
            if (!value.isBlank()) {
                values.add(value);
            }
        }
        if (values.isEmpty()) {
            throw new IllegalArgumentException("Missing analyzer values " + propertyName);
        }
        return values;
    }

    private static List<String> resourceLines(NodeState resource) {
        PropertyState data = resource.getChildNode("jcr:content").getProperty("jcr:data");
        if (data == null) {
            throw new IllegalArgumentException("Missing analyzer resource stopwords");
        }
        List<String> lines = new ArrayList<>();
        for (String line : read(data).split("\\R")) {
            String value = line.trim();
            if (!value.isEmpty() && !value.startsWith("#")) {
                lines.add(value);
            }
        }
        if (lines.isEmpty()) {
            throw new IllegalArgumentException("Mongot stopword filter requires tokens");
        }
        return lines;
    }

    private static String resourceText(NodeState component, String propertyName) {
        return String.join("\n", resourceContents(component, propertyName));
    }

    private static List<String> resourceContents(NodeState component, String propertyName) {
        PropertyState names = component.getProperty(propertyName);
        if (names == null) {
            throw new IllegalArgumentException("Missing analyzer resource property " + propertyName);
        }
        List<String> contents = new ArrayList<>();
        for (String resource : names.getValue(Type.STRING).split(",")) {
            NodeState content = component.getChildNode(resource.trim()).getChildNode("jcr:content");
            PropertyState data = content.getProperty("jcr:data");
            if (data == null) {
                throw new IllegalArgumentException("Missing analyzer resource " + resource.trim());
            }
            contents.add(read(data));
        }
        return contents;
    }

    private static String read(PropertyState data) {
        if (data.getType().tag() != Type.BINARY.tag()) {
            return data.getValue(Type.STRING);
        }
        Blob blob = data.getValue(Type.BINARY);
        try (InputStream stream = blob.getNewStream()) {
            return new String(stream.readAllBytes(), StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new IllegalArgumentException("Cannot read analyzer resource", e);
        }
    }

    private static int integer(NodeState component, String oakName, String mongoName) {
        if (!component.hasProperty(oakName)) {
            throw new IllegalArgumentException("Mongot analyzer component requires " + mongoName);
        }
        try {
            return Integer.parseInt(component.getProperty(oakName).getValue(Type.STRING));
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Mongot analyzer component requires integer "
                    + mongoName, e);
        }
    }

    private static int requiredInteger(NodeState component, String displayName, String... names) {
        for (String name : names) {
            if (component.hasProperty(name)) {
                return parseInteger(component, name, displayName);
            }
        }
        throw new IllegalArgumentException("Mongot analyzer component requires "
                + displayName);
    }

    private static int integerValue(NodeState component, int defaultValue, String... names) {
        for (String name : names) {
            if (component.hasProperty(name)) {
                return parseInteger(component, name, name);
            }
        }
        return defaultValue;
    }

    private static int parseInteger(NodeState component, String propertyName, String displayName) {
        try {
            return Integer.parseInt(component.getProperty(propertyName).getValue(Type.STRING));
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Mongot analyzer component requires integer "
                    + displayName, e);
        }
    }

    private static boolean hasProperty(NodeState component, String... names) {
        for (String name : names) {
            if (component.hasProperty(name)) {
                return true;
            }
        }
        return false;
    }

    private static boolean booleanValueAny(NodeState component, boolean defaultValue,
                                           String... names) {
        for (String name : names) {
            if (component.hasProperty(name)) {
                return booleanValue(component, name, defaultValue);
            }
        }
        return defaultValue;
    }

    private static String stringValue(NodeState component, String defaultValue, String... names) {
        for (String name : names) {
            if (component.hasProperty(name)) {
                return component.getString(name);
            }
        }
        return defaultValue;
    }

    private static boolean booleanValue(NodeState component, String name, boolean defaultValue) {
        PropertyState property = component.getProperty(name);
        if (property == null) {
            return defaultValue;
        }
        String value = property.getValue(Type.STRING);
        return "1".equals(value) || Boolean.parseBoolean(value);
    }

    private static String requiredString(NodeState component, String name) {
        if (!component.hasProperty(name)) {
            throw new IllegalArgumentException("Mongot analyzer component requires " + name);
        }
        return component.getString(name);
    }

    private static String componentName(NodeState component, String fallback) {
        String name = component.hasProperty(FulltextIndexConstants.ANL_NAME)
                ? component.getString(FulltextIndexConstants.ANL_NAME)
                : fallback;
        if (name == null) {
            throw new IllegalArgumentException("Analyzer component is missing a name");
        }
        return name.replace("Factory", "").replace("-", "")
                .replace("_", "").toLowerCase(Locale.ROOT);
    }

    private static String unescape(String value) {
        return value.replace("\\\\\"", "\"")
                .replace("\\\\n", "\n")
                .replace("\\\\t", "\t")
                .replace("\\\\\\\\", "\\");
    }

    private static IllegalArgumentException unsupportedComponent(String kind, String name) {
        return new IllegalArgumentException("Mongot cannot faithfully translate "
                + kind + ": " + name);
    }

    private static IllegalArgumentException unsupportedAnalyzer(String className) {
        return new IllegalArgumentException("Mongot cannot faithfully translate analyzer: "
                + className);
    }

    private record Analyzer(String name, String searchName, Document custom) {
    }
}
