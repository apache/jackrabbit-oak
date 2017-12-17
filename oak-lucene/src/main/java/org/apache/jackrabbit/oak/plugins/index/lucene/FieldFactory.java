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

import java.util.Arrays;

import com.google.common.primitives.Ints;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.util.ISO8601;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.IntField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;

import static org.apache.lucene.index.FieldInfo.IndexOptions.DOCS_AND_FREQS_AND_POSITIONS;

import static org.apache.jackrabbit.oak.plugins.index.lucene.FieldNames.PATH;
import static org.apache.jackrabbit.oak.plugins.index.lucene.FieldNames.FULLTEXT;
import static org.apache.lucene.document.Field.Store.NO;
import static org.apache.lucene.document.Field.Store.YES;
import static org.apache.lucene.index.FieldInfo.IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS;

/**
 * {@code FieldFactory} is a factory for <code>Field</code> instances with
 * frequently used fields.
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
        return new StringField(PATH, path, YES);
    }

    public static Field newPropertyField(String name, String value,
            boolean tokenized, boolean stored) {
        if (tokenized) {
            return new OakTextField(name, value, stored);
        }
        return new StringField(name, value, NO);
    }

    public static Field newFulltextField(String value, boolean stored) {
        return new TextField(FULLTEXT, value, stored ? YES : NO);
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
