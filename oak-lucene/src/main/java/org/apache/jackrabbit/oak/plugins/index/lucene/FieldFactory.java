/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.oak.plugins.index.lucene;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;

import com.google.common.primitives.Ints;
import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.plugins.index.lucene.util.fv.SimSearchUtils;
import org.apache.jackrabbit.oak.plugins.index.search.FieldNames;
import org.apache.jackrabbit.oak.plugins.index.search.spi.binary.BlobByteSource;
import org.apache.jackrabbit.util.ISO8601;
import org.apache.lucene.document.*;

import static org.apache.lucene.document.Field.Store.NO;
import static org.apache.lucene.document.Field.Store.YES;
import static org.apache.lucene.index.FieldInfo.IndexOptions.DOCS_AND_FREQS_AND_POSITIONS;
import static org.apache.lucene.index.FieldInfo.IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS;

/**
 * A factory for Lucene Field instances with frequently used fields.
 */
public final class FieldFactory {

    /**
     * StringField#TYPE_NOT_STORED but tokenized
     */
    private static final FieldType OAK_TYPE = new FieldType();

    private static final FieldType OAK_TYPE_NOT_STORED = new FieldType();

    private static final int[] TYPABLE_TAGS = {
            Type.DATE.tag(),
            Type.BOOLEAN.tag(),
            Type.DOUBLE.tag(),
            Type.LONG.tag(),
    };

    static {
        OAK_TYPE.setIndexed(true);
        OAK_TYPE.setOmitNorms(true);
        OAK_TYPE.setStored(true);
        OAK_TYPE.setIndexOptions(DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS);
        OAK_TYPE.setTokenized(true);
        OAK_TYPE.freeze();

        OAK_TYPE_NOT_STORED.setIndexed(true);
        OAK_TYPE_NOT_STORED.setOmitNorms(true);
        OAK_TYPE_NOT_STORED.setStored(false);
        OAK_TYPE_NOT_STORED.setIndexOptions(DOCS_AND_FREQS_AND_POSITIONS);
        OAK_TYPE_NOT_STORED.setTokenized(true);
        OAK_TYPE_NOT_STORED.freeze();

        Arrays.sort(TYPABLE_TAGS);
    }

    public static boolean canCreateTypedField(Type<?> type) {
        return Ints.contains(TYPABLE_TAGS, type.tag());
    }

    private final static class OakTextField extends Field {

        public OakTextField(String name, String value, boolean stored) {
            super(name, value, stored ? OAK_TYPE : OAK_TYPE_NOT_STORED);
        }
    }

    /**
     * Private constructor.
     */
    private FieldFactory() {
    }

    public static Field newPathField(String path) {
        return new StringField(FieldNames.PATH, path, YES);
    }

    public static Field newPropertyField(String name, String value,
            boolean tokenized, boolean stored) {
        if (tokenized) {
            return new OakTextField(name, value, stored);
        }
        return new StringField(name, value, NO);
    }

    public static Collection<Field> newSimilarityFields(String name, Blob value) throws IOException {
        Collection<Field> fields = new ArrayList<>(1);
        byte[] bytes = new BlobByteSource(value).read();
        fields.add(newSimilarityField(name, bytes));
        return fields;
    }

    public static Collection<Field> newBinSimilarityFields(String name, Blob value) throws IOException {
        Collection<Field> fields = new ArrayList<>(1);
        byte[] bytes = new BlobByteSource(value).read();
        fields.add(newBinarySimilarityField(name, bytes));
        return fields;
    }

    public static Collection<Field> newSimilarityFields(String name, String value) {
        Collection<Field> fields = new ArrayList<>(1);
        fields.add(newSimilarityField(name, value));
        return fields;
    }

    public static Collection<Field> newBinSimilarityFields(String name, String value) {
        Collection<Field> fields = new ArrayList<>(1);
        byte[] bytes = SimSearchUtils.toByteArray(value);
        fields.add(newBinarySimilarityField(name, bytes));
        return fields;
    }

    private static StoredField newBinarySimilarityField(String name, byte[] bytes) {
        return new StoredField(FieldNames.createBinSimilarityFieldName(name), bytes);
    }

    private static Field newSimilarityField(String name, byte[] bytes) {
        return newSimilarityField(name, SimSearchUtils.toDoubleString(bytes));
    }

    private static Field newSimilarityField(String name, String value) {
        return new TextField(FieldNames.createSimilarityFieldName(name), value, Field.Store.YES);
    }

    public static Field newFulltextField(String value) {
        return newFulltextField(value, false);
    }

    public static Field newFulltextField(String name, String value) {
        return newFulltextField(name, value, false);
    }

    public static Field newFulltextField(String value, boolean stored) {
        return new TextField(FieldNames.FULLTEXT, value, stored ? YES : NO);
    }

    public static Field newFulltextField(String name, String value, boolean stored) {
        return new TextField(FieldNames.createFulltextFieldName(name), value, stored ? YES : NO);
    }

    public static Field newAncestorsField(String path){
        return new TextField(FieldNames.ANCESTORS, path, NO);
    }

    public static Field newDepthField(String path){
        return new IntField(FieldNames.PATH_DEPTH, PathUtils.getDepth(path), NO);
    }

    public static Field newSuggestField(String... values) {
        StringBuilder builder = new StringBuilder();
        for (String v : values) {
            if (builder.length() > 0) {
                builder.append('\n');
            }
            builder.append(v);
        }
        return new OakTextField(FieldNames.SUGGEST, builder.toString(), false);
    }

    /**
     * Date values are saved with sec resolution
     * @param date jcr data string
     * @return date value in seconds
     */
    public static Long dateToLong(String date){
        if( date == null){
            return null;
        }
        //TODO OAK-2204 - Should we change the precision to lower resolution
        return ISO8601.parse(date).getTimeInMillis();
    }

}
