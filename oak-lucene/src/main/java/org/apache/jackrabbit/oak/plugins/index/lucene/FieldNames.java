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
import java.util.HashSet;
import java.util.Set;

/**
 * Defines field names that are used internally to store :path, etc in the
 * search index.
 */
public final class FieldNames {


    /**
     * Private constructor.
     */
    private FieldNames() {
    }

    /**
     * Name of the field that contains the {@value} property of the node.
     */
    public static final String PATH = ":path";

    /**
     * Name of the field that contains all the path hierarchy e.g. for /a/b/c
     * it would contain /a, /a/b, /a/b/c
     */
    public static final String ANCESTORS = ":ancestors";

    /**
     * Name of the field which refers to the depth of path
     */
    public static final String PATH_DEPTH = ":depth";

    /**
     * Name of the field that contains the fulltext index.
     */
    public static final String FULLTEXT = ":fulltext";

    /**
     * Name of the field that contains the suggest index.
     */
    public static final String SUGGEST = ":suggest";

    /**
     * Name of the field that contains the spellcheck index.
     */
    public static final String SPELLCHECK = ":spellcheck";

    /**
     * Prefix for all field names that are fulltext indexed by property name.
     */
    public static final String ANALYZED_FIELD_PREFIX = "full:";

    /**
     * Prefix used for storing fulltext of relative node
     */
    public static final String FULLTEXT_RELATIVE_NODE = "fullnode:";

    /**
     * Name of the field that contains those property names which are not found
     * (or were null) for the given
     */
    public static final String NULL_PROPS = ":nullProps";

    /**
     * Name of the field that contains those property names which are exist i.e. not null
     * for the given NodeState
     */
    public static final String NOT_NULL_PROPS = ":notNullProps";

    /**
     * Name of the field that contains the node name
     */
    public static final String NODE_NAME = ":nodeName";
    
    /**
     * Suffix of the fields that contains function values
     */
    public static final String FUNCTION_PREFIX = "function*";

    /**
     * Used to select only the PATH field from the lucene documents
     */
    public static final Set<String> PATH_SELECTOR = new HashSet<String>(
            Arrays.asList(PATH));

    /**
     * Encodes the field name such that it can be used for storing DocValue
     * This is done such a field if used for both sorting and querying uses
     * a different name for docvalue field
     *
     * @param name name to encode
     * @return encoded field name
     */
    public static String createDocValFieldName(String name){
        return ":dv" + name;
    }

    public static String createAnalyzedFieldName(String pname) {
        return ANALYZED_FIELD_PREFIX + pname;
    }

    public static String createFulltextFieldName(String nodeRelativePath) {
        if (nodeRelativePath == null){
            return FULLTEXT;
        }
        return FULLTEXT_RELATIVE_NODE + nodeRelativePath;
    }

    public static String createFacetFieldName(String pname) {
        return pname + "_facet";
    }

    /**
     * @return if {@code field} represents a field property indexed data
     */
    static boolean isPropertyField(String field) {
        return !field.startsWith(ANALYZED_FIELD_PREFIX)
                && !field.startsWith(FULLTEXT_RELATIVE_NODE)
                && !field.startsWith(":")
                && !field.endsWith("_facet");
    }
}
