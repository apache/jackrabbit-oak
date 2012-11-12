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
import org.apache.lucene.document.StringField;

import static org.apache.jackrabbit.oak.plugins.index.lucene.FieldNames.PATH;
import static org.apache.lucene.document.Field.Store.NO;
import static org.apache.lucene.document.Field.Store.YES;

/**
 * {@code FieldFactory} is a factory for <code>Field</code> instances with
 * frequently used fields.
 */
public final class FieldFactory {

    /**
     * Private constructor.
     */
    private FieldFactory() {
    }

    public static Field newPathField(String path) {
        return new StringField(PATH, path, YES);
    }

    public static Field newPropertyField(String name, String value) {
        // TODO do we need norms info on the indexed fields ? TextField:StringField
        // return new TextField(name, value, NO);
        return new StringField(name, value, NO);
    }
}
