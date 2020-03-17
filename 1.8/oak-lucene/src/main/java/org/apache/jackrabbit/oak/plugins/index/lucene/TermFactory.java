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

import org.apache.lucene.index.Term;

/**
 * {@code TermFactory} is a factory for <code>Term</code> instances with
 * frequently used field names.
 */
public final class TermFactory {

    /**
     * Private constructor.
     */
    private TermFactory() {
    }

    /**
     * Creates a Term with the given {@code path} value and with a field
     * name {@link FieldNames#PATH}.
     *
     * @param path
     *            the path.
     * @return the path term.
     */
    public static Term newPathTerm(String path) {
        return new Term(FieldNames.PATH, preparePath(path));
    }

    public static Term newFulltextTerm(String ft, String field) {
        if (field == null || "*".equals(field)) {
            return newFulltextTerm(ft);
        }
        return new Term(field, ft);
    }

    public static Term newAncestorTerm(String path){
        return new Term(FieldNames.ANCESTORS, preparePath(path));
    }

    public static Term newFulltextTerm(String ft) {
        return new Term(FieldNames.FULLTEXT, ft);
    }

    private static String preparePath(String path) {
        if (!"/".equals(path) && !path.startsWith("/")) {
            path = "/" + path;
        }
        return path;
    }
}
