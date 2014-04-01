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

import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.FieldInfo.IndexOptions;

import static org.apache.jackrabbit.oak.plugins.index.lucene.FieldNames.PATH;
import static org.apache.jackrabbit.oak.plugins.index.lucene.FieldNames.FULLTEXT;
import static org.apache.lucene.document.Field.Store.NO;
import static org.apache.lucene.document.Field.Store.YES;

/**
 * {@code FieldFactory} is a factory for <code>Field</code> instances with
 * frequently used fields.
 */
public final class FieldFactory {

    /**
     * StringField#TYPE_NOT_STORED but tokenized
     */
    private static final FieldType OAK_TYPE = new FieldType();

    static {
        OAK_TYPE.setIndexed(true);
        OAK_TYPE.setOmitNorms(true);
        OAK_TYPE.setStored(true);
        OAK_TYPE.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS);
        OAK_TYPE.setTokenized(true);
        OAK_TYPE.freeze();
    }

    private final static class OakTextField extends Field {

        public OakTextField(String name, String value) {
            super(name, value, OAK_TYPE);
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
            boolean tokenized) {
        if (tokenized) {
            return new OakTextField(name, value);
        }
        return new StringField(name, value, NO);
    }

    public static Field newFulltextField(String value) {
        return new TextField(FULLTEXT, value, NO);
    }

}
