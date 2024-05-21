/*
 * COPIED FROM APACHE LUCENE 4.7.2
 *
 * Git URL: git@github.com:apache/lucene.git, tag: releases/lucene-solr/4.7.2, path: lucene/core/src/java
 *
 * (see https://issues.apache.org/jira/browse/OAK-10786 for details)
 */

package org.apache.lucene.document;

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

import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.util.BytesRef;

/**
 * <p>
 * Field that stores a per-document {@link BytesRef} value. Here's an example usage:
 *
 * <pre class="prettyprint">
 *   document.add(new DerefBytesDocValuesField(name, new BytesRef("hello")));
 * </pre>
 *
 * <p>
 * If you also need to store the value, you should add a separate {@link StoredField} instance.
 *
 * @see BinaryDocValues
 * @deprecated Use {@link BinaryDocValuesField} instead.
 */
@Deprecated
public class DerefBytesDocValuesField extends BinaryDocValuesField {

    /**
     * Type for bytes DocValues: all with the same length
     */
    public static final FieldType TYPE_FIXED_LEN = BinaryDocValuesField.TYPE;

    /**
     * Type for bytes DocValues: can have variable lengths
     */
    public static final FieldType TYPE_VAR_LEN = BinaryDocValuesField.TYPE;

    /**
     * Create a new fixed or variable-length DocValues field.
     *
     * @param name  field name
     * @param bytes binary content
     * @throws IllegalArgumentException if the field name is null
     */
    public DerefBytesDocValuesField(String name, BytesRef bytes) {
        super(name, bytes);
    }

    /**
     * Create a new fixed or variable length DocValues field.
     * <p>
     *
     * @param name          field name
     * @param bytes         binary content
     * @param isFixedLength (ignored)
     * @throws IllegalArgumentException if the field name is null
     */
    public DerefBytesDocValuesField(String name, BytesRef bytes, boolean isFixedLength) {
        super(name, bytes);
    }
}
