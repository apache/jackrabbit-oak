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
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.commons.log.LogSilencer;
import org.apache.jackrabbit.oak.plugins.index.lucene.util.FacetsConfigProvider;
import org.apache.jackrabbit.oak.plugins.index.search.Aggregate;
import org.apache.jackrabbit.oak.plugins.index.search.FieldNames;
import org.apache.jackrabbit.oak.plugins.index.search.IndexDefinition;
import org.apache.jackrabbit.oak.plugins.index.search.IndexFormatVersion;
import org.apache.jackrabbit.oak.plugins.index.search.PropertyDefinition;
import org.apache.jackrabbit.oak.plugins.index.search.spi.binary.FulltextBinaryTextExtractor;
import org.apache.jackrabbit.oak.plugins.index.search.spi.editor.FulltextDocumentMaker;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.lucene.document.*;
import org.apache.lucene.facet.FacetsConfig;
import org.apache.lucene.facet.sortedset.SortedSetDocValuesFacetField;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.util.BytesRef;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.jackrabbit.oak.plugins.index.lucene.FieldFactory.newAncestorsField;
import static org.apache.jackrabbit.oak.plugins.index.lucene.FieldFactory.newDepthField;
import static org.apache.jackrabbit.oak.plugins.index.lucene.FieldFactory.newFulltextField;
import static org.apache.jackrabbit.oak.plugins.index.lucene.FieldFactory.newPathField;
import static org.apache.jackrabbit.oak.plugins.index.lucene.FieldFactory.newPropertyField;

public class LuceneDocumentMaker extends FulltextDocumentMaker<Document> {
    // Lucene doesn't support indexing data larger than 32766 (OAK-9707)
    public static final int STRING_PROPERTY_MAX_LENGTH = 32766;
    private static final Logger log = LoggerFactory.getLogger(LuceneDocumentMaker.class);

    private static final String DYNAMIC_BOOST_SPLIT_REGEX = "[:/]";
    
    private final FacetsConfigProvider facetsConfigProvider;
    private final IndexAugmentorFactory augmentorFactory;
    
    private static final LogSilencer LOG_SILENCER = new LogSilencer(Duration.ofSeconds(10).toMillis(), 10);
    private static final String LOG_KEY_DUPLICATE = "Duplicate value";
    private static final String LOG_KEY_NOT_A_DATE_STRING = "Not a date string";
    private static final String LOG_KEY_UNABLE_TO_PARSE = "Unable to parse the provided date field";
    private static final String LOG_KEY_FOR_INPUT_STRING = "For input string";

    public LuceneDocumentMaker(IndexDefinition definition,
                               IndexDefinition.IndexingRule indexingRule,
                               String path) {
        this(null, null, null, definition, indexingRule, path);
    }

    public LuceneDocumentMaker(@Nullable FulltextBinaryTextExtractor textExtractor,
                               @Nullable FacetsConfigProvider facetsConfigProvider,
                               @Nullable IndexAugmentorFactory augmentorFactory,
                               IndexDefinition definition,
                               IndexDefinition.IndexingRule indexingRule,
                               String path) {
        super(textExtractor, definition, indexingRule, path);
        this.facetsConfigProvider = facetsConfigProvider;
        this.augmentorFactory = augmentorFactory;
    }

    @Override
    protected void indexAnalyzedProperty(Document doc, String pname, String value, PropertyDefinition pd) {
        String analyzedPropName = constructAnalyzedPropertyName(pname);
        doc.add(newPropertyField(analyzedPropName, value, !pd.skipTokenization(pname), pd.stored));
    }

    @Override
    protected void indexSuggestValue(Document doc, String value) {
        doc.add(FieldFactory.newSuggestField(value));
    }

    @Override
    protected void indexSpellcheckValue(Document doc, String value) {
        doc.add(newPropertyField(FieldNames.SPELLCHECK, value, true, false));
    }

    @Override
    protected void indexFulltextValue(Document doc, String value) {
        doc.add(newFulltextField(value));
    }

    @Override
    protected void indexAncestors(Document doc, String path) {
        doc.add(newAncestorsField(PathUtils.getParentPath(path)));
        doc.add(newDepthField(path));
    }

    @Override
    protected void indexTypedProperty(Document doc, PropertyState property, String pname, PropertyDefinition pd, int i) {
        int tag = property.getType().tag();

        Field f;
        if (tag == Type.LONG.tag()) {
            f = new LongField(pname, property.getValue(Type.LONG, i), Field.Store.NO);
        } else if (tag == Type.DATE.tag()) {
            String date = property.getValue(Type.DATE, i);
            f = new LongField(pname, FieldFactory.dateToLong(date), Field.Store.NO);
        } else if (tag == Type.DOUBLE.tag()) {
            f = new DoubleField(pname, property.getValue(Type.DOUBLE, i), Field.Store.NO);
        } else if (tag == Type.BOOLEAN.tag()) {
            f = new StringField(pname, property.getValue(Type.BOOLEAN, i).toString(), Field.Store.NO);
        } else {
            f = new StringField(pname, property.getValue(Type.STRING, i), Field.Store.NO);
        }

        doc.add(f);
    }

    @Override
    protected void indexNotNullProperty(Document doc, PropertyDefinition pd) {
        doc.add(new StringField(FieldNames.NOT_NULL_PROPS, pd.name, Field.Store.NO));
    }

    @Override
    protected void indexNullProperty(Document doc, PropertyDefinition pd) {
        doc.add(new StringField(FieldNames.NULL_PROPS, pd.name, Field.Store.NO));
    }

    private String constructAnalyzedPropertyName(String pname) {
        if (definition.getVersion().isAtLeast(IndexFormatVersion.V2)){
            return FieldNames.createAnalyzedFieldName(pname);
        }
        return pname;
    }

    @Override
    protected boolean addBinary(Document doc, String path, List<String> binaryValues) {
        boolean added = false;
        for (String binaryValue : binaryValues) {
            if (path != null) {
                doc.add(newFulltextField(path, binaryValue, true));
            } else {
                doc.add(newFulltextField(binaryValue, true));
            }

            added = true;
        }

        return added;
    }

    @Override
    protected boolean indexFacetProperty(Document doc, int tag, PropertyState property, String pname) {
        String facetFieldName = FieldNames.createFacetFieldName(pname);
        getFacetsConfig().setIndexFieldName(pname, facetFieldName);

        boolean fieldAdded = false;
        try {
            if (tag == Type.STRINGS.tag() && property.isArray()) {
                getFacetsConfig().setMultiValued(pname, true);
                Iterable<String> values = property.getValue(Type.STRINGS);
                for (String value : values) {
                    if (value != null && value.length() > 0) {
                        doc.add(new SortedSetDocValuesFacetField(pname, value));
                    }
                }
                fieldAdded = true;
            } else if (tag == Type.STRING.tag()) {
                String value = property.getValue(Type.STRING);
                if (value.length() > 0) {
                    doc.add(new SortedSetDocValuesFacetField(pname, value));
                    fieldAdded = true;
                }
            }

        } catch (Throwable e) {
            log.warn("[{}] Ignoring facet property. Could not convert property {} of type {} to type {} for path {}",
                    getIndexName(), pname,
                    Type.fromTag(property.getType().tag(), false),
                    Type.fromTag(tag, false), path, e);
        }
        return fieldAdded;
    }

    @Override
    protected void indexAggregateValue(Document doc, Aggregate.NodeIncludeResult result, String value, PropertyDefinition pd) {
        Field field = result.isRelativeNode() ?
                newFulltextField(result.rootIncludePath, value) : newFulltextField(value) ;
        if (pd != null) {
            field.setBoost(pd.boost);
        }
        doc.add(field);
    }

    @Override
    protected Document initDoc() {
        Document doc = new Document();
        doc.add(newPathField(path));
        return doc;
    }

    @Override
    protected boolean augmentCustomFields(final String path, final Document doc, final NodeState document) {
        boolean dirty = false;

        if (augmentorFactory != null) {
            Iterable<Field> augmentedFields = augmentorFactory
                    .getIndexFieldProvider(indexingRule.getNodeTypeName())
                    .getAugmentedFields(path, document, definition.getDefinitionNodeState());

            for (Field field : augmentedFields) {
                doc.add(field);
                dirty = true;
            }
        }

        return dirty;
    }

    @Override
    protected Document finalizeDoc(Document doc, boolean dirty, boolean facet) throws IOException {
        if (facet && isFacetingEnabled()) {
            doc = getFacetsConfig().build(doc);
        }

        List<IndexableField> fields = doc.getFields();

        // because of LUCENE-5833 we have to merge the suggest fields into a single one
        Field suggestField = null;
        for (IndexableField f : fields) {
            if (FieldNames.SUGGEST.equals(f.name())) {
                if (suggestField == null) {
                    suggestField = FieldFactory.newSuggestField(f.stringValue());
                } else {
                    suggestField = FieldFactory.newSuggestField(suggestField.stringValue(), f.stringValue());
                }
            }
        }

        doc.removeFields(FieldNames.SUGGEST);
        if (suggestField != null) {
            doc.add(suggestField);
        }

        return doc;
    }

    @Override
    protected boolean isFacetingEnabled() {
        return facetsConfigProvider != null;
    }

    @Override
    protected boolean indexTypeOrderedFields(Document doc, String pname, int tag, PropertyState property, PropertyDefinition pd) {
        String name = FieldNames.createDocValFieldName(pname);
        boolean fieldAdded = false;
        Field f = null;
        try {
            if (tag == Type.LONG.tag()) {
                //TODO Distinguish fields which need to be used for search and for sort
                //If a field is only used for Sort then it can be stored with less precision
                f = new NumericDocValuesField(name, property.getValue(Type.LONG));
            } else if (tag == Type.DATE.tag()) {
                String date = property.getValue(Type.DATE);
                f = new NumericDocValuesField(name, FieldFactory.dateToLong(date));
            } else if (tag == Type.DOUBLE.tag()) {
                f = new DoubleDocValuesField(name, property.getValue(Type.DOUBLE));
            } else if (tag == Type.BOOLEAN.tag()) {
                f = new SortedDocValuesField(name,
                        new BytesRef(property.getValue(Type.BOOLEAN).toString()));
            } else if (tag == Type.STRING.tag()) {
                String stringValue = property.getValue(Type.STRING);
                // Truncate the value as lucene limits the length of a SortedDocValueField string to 
                // STRING_PROPERTY_MAX_LENGTH(32766 bytes) and throws exception if over the limit
                f = new SortedDocValuesField(name, getTruncatedBytesRef(name, stringValue, this.path,
                        STRING_PROPERTY_MAX_LENGTH));
            }

            if (f != null && includePropertyValue(property, 0, pd)) {
                if (doc.getField(f.name()) == null) {
                    doc.add(f);
                    fieldAdded = true;
                } else {
                    if (!LOG_SILENCER.silence(LOG_KEY_DUPLICATE)) {
                        log.warn("Duplicate value for ordered field {}; ignoring. Possibly duplicate index definition.", f.name());
                    }
                }
            }
        } catch (Exception e) {
            String message = e.getMessage();
            String key = null;
            // This is a known warning, one of:
            // - IllegalArgumentException: Not a date string
            // - RuntimeException: Unable to parse the provided date field
            // - NumberFormatException: For input string
            // For these we do not log a stack trace, and we only log once every 10 seconds
            // (the location of the code can be found if needed, as it's in Oak)
            if (message.startsWith("Not a date string")) {
                key = LOG_KEY_NOT_A_DATE_STRING;
            } else if (message.startsWith("Unable to parse the provided date field")) {
                key = LOG_KEY_UNABLE_TO_PARSE;
            } else if (message.startsWith("For input string")) {
                key = LOG_KEY_FOR_INPUT_STRING;
            }
            if (key != null) {
                if (!LOG_SILENCER.silence(key)) {
                    // log without stack trace (as it is known)
                    log.warn(
                            "[{}] Ignoring ordered property. Could not convert property {} of type {} to type {} for path {}, message {}",
                            getIndexName(), pname,
                            Type.fromTag(property.getType().tag(), false),
                            Type.fromTag(tag, false), path, e.getMessage());
                }
            } else {
                log.warn(
                        "[{}] Ignoring ordered property. Could not convert property {} of type {} to type {} for path {}",
                        getIndexName(), pname,
                        Type.fromTag(property.getType().tag(), false),
                        Type.fromTag(tag, false), path, e);
            }
        }
        return fieldAdded;
    }

    /**
     * Returns a {@code BytesRef} object constructed from the given {@code String} value and also truncates the length
     * of the {@code BytesRef} object to the specified {@code maxLength}, ensuring that the multi-byte sequences are
     * properly truncated.
     *
     * <p>The {@code BytesRef} object is created from the provided {@code String} value using UTF-8 encoding. As a result, its length
     * can exceed that of the {@code String} value, since Java strings use UTF-16 encoding. This necessitates appropriate truncation.
     *
     * <p>Multi-byte sequences will be of the form {@code 11xxxxxx 10xxxxxx 10xxxxxx 10xxxxxx}.
     * The method first truncates continuation bytes, which start with {@code 10} in binary. It then truncates the head byte, which
     * starts with {@code 11}. Both truncation operations use a binary mask of {@code 11000000}.
     *
     * @param prop      the name of the property
     * @param value     the string property value to convert into a {@code BytesRef} object
     * @param path      the path of the node
     * @param maxLength the maximum length for the {@code BytesRef} object
     * @return the truncated {@code BytesRef} object
     */
    protected static BytesRef getTruncatedBytesRef(String prop, String value, String path, int maxLength) {
        BytesRef ref = new BytesRef(value);
        if (ref.length <= maxLength) {
            return ref;
        }
        
        log.trace("Property {} at path:[{}] has value {}", prop, path, value);
        log.info("Truncating property {} at path:[{}] as length after encoding {} is > {} ",
            prop, path, ref.length, maxLength);
        
        int end = maxLength - 1;
        // skip over tails of utf-8 multi-byte sequences (up to 3 bytes)
        while ((ref.bytes[end] & 0b11000000) == 0b10000000) {
            end--;
        }
        // remove one head of a utf-8 multi-byte sequence (at most 1)
        if ((ref.bytes[end] & 0b11000000) == 0b11000000) {
            end--;
        }
        byte[] truncatedBytes = Arrays.copyOf(ref.bytes, end + 1);
        String truncated = new String(truncatedBytes, StandardCharsets.UTF_8);
        ref = new BytesRef(truncated);
        if (log.isTraceEnabled()) {
            log.trace("Truncated property {} at path:[{}] to {}", prop, path, ref.utf8ToString());
        }

        while (ref.length > maxLength) {
            log.error("Truncation did not work: still {} bytes", ref.length);
            // this may not properly work with unicode surrogates:
            // it is an "emergency" procedure and should never happen
            truncated = truncated.substring(0, truncated.length() - 10);
            ref = new BytesRef(truncated);
        }
        return ref;
    }

    private FacetsConfig getFacetsConfig() {
        return facetsConfigProvider.getFacetsConfig();
    }

    @Override
    protected void indexNodeName(Document doc, String value) {
        doc.add(new StringField(FieldNames.NODE_NAME, value, Field.Store.NO));
    }

    @Override
    protected boolean indexSimilarityTag(Document doc, PropertyState property) {
        doc.add(new TextField(FieldNames.SIMILARITY_TAGS, property.getValue(Type.STRING), Field.Store.YES));
        return true;
    }

    @Override
    protected void indexSimilarityStrings(Document doc, PropertyDefinition pd, String value) {
        for (Field f : FieldFactory.newSimilarityFields(pd.name, value)) {
            doc.add(f);
        }
        if (pd.similarityRerank) {
            for (Field f : FieldFactory.newBinSimilarityFields(pd.name, value)) {
                doc.add(f);
            }
        }
    }

    @Override
    protected void indexSimilarityBinaries(Document doc, PropertyDefinition pd, Blob blob) throws IOException {
        for (Field f : FieldFactory.newSimilarityFields(pd.name, blob)) {
            doc.add(f);
        }
        if (pd.similarityRerank) {
            for (Field f : FieldFactory.newBinSimilarityFields(pd.name, blob)) {
                doc.add(f);
            }
        }
    }

    @Override
    protected boolean indexDynamicBoost(Document doc, String parent, String nodeName, String value, double confidence) {
        List<String> tokens = new ArrayList<>(splitForIndexing(value));
        if (tokens.size() > 1) {
            // Actual name not in tokens
            tokens.add(value);
        }
        boolean added = false;
        for (String token : tokens) {
            if (token.length() > 0) {
                AugmentedField f = new AugmentedField(parent + "/" + token.toLowerCase(), confidence);
                if (doc.getField(f.name()) == null) {
                    doc.add(f);
                    added = true;
                }
            }
        }

        if (added) {
            if (log.isTraceEnabled()) {
                log.trace(
                        "Added augmented fields: {}[{}], {}",
                        parent + "/", String.join(", ", tokens), confidence
                );
            }
        }

        return added;
    }

    private static List<String> splitForIndexing(String tagName) {
        return Arrays.asList(removeBackSlashes(tagName).split(DYNAMIC_BOOST_SPLIT_REGEX));
    }

    private static String removeBackSlashes(String text) {
        return text.replaceAll("\\\\", "");
    }

    private static class AugmentedField extends Field {
        private static final FieldType ft = new FieldType();
        static {
            ft.setIndexed(true);
            ft.setStored(false);
            ft.setTokenized(false);
            ft.setOmitNorms(false);
            ft.setIndexOptions(org.apache.lucene.index.FieldInfo.IndexOptions.DOCS_ONLY);
            ft.freeze();
        }

        AugmentedField(String name, double weight) {
            super(name, "1", ft);
            setBoost((float) weight);
        }
    }

}