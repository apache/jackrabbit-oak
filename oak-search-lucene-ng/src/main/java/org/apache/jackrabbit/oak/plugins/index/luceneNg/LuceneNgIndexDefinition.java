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
package org.apache.jackrabbit.oak.plugins.index.luceneNg;

import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.plugins.index.search.IndexDefinition;
import org.apache.jackrabbit.oak.plugins.index.search.IndexFormatVersion;
import org.apache.jackrabbit.oak.plugins.index.search.PropertyDefinition;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jcr.PropertyType;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Index definition for Lucene 9 indexes.
 * Extends the base IndexDefinition with Lucene 9 specific configuration.
 */
public class LuceneNgIndexDefinition extends IndexDefinition {

    private static final Logger LOG = LoggerFactory.getLogger(LuceneNgIndexDefinition.class);
    private static final Set<String> REPORTED_CONFLICTS = ConcurrentHashMap.newKeySet();

    /** Fields that rules declare with types of incompatible Lucene field shapes. */
    private final Set<String> conflictingFields;

    /**
     * Creates a new Lucene 9 index definition.
     *
     * @param root the root node state
     * @param defn the index definition node state
     * @param indexPath the path to this index
     */
    public LuceneNgIndexDefinition(@NotNull NodeState root,
                                  @NotNull NodeState defn,
                                  @NotNull String indexPath) {
        super(root, defn, indexPath);
        this.conflictingFields = conflictingFields();
    }

    /**
     * Creates a new Lucene 9 index definition with an explicit format version and unique id.
     * Used by {@link Builder} (and, in turn, by the shared {@code FulltextIndexEditorContext}).
     *
     * @param root the root node state
     * @param defn the index definition node state to use
     * @param version the index format version
     * @param uid the unique id of the index, or {@code null}
     * @param indexPath the path to this index
     */
    public LuceneNgIndexDefinition(@NotNull NodeState root,
                                  @NotNull NodeState defn,
                                  @NotNull IndexFormatVersion version,
                                  @Nullable String uid,
                                  @NotNull String indexPath) {
        super(root, defn, version, uid, indexPath);
        this.conflictingFields = conflictingFields();
    }

    @Override
    protected String getDefaultFunctionName() {
        return LuceneNgIndexConstants.TYPE_LUCENE_NG;
    }

    @Override
    public boolean evaluatePathRestrictions() {
        // PATH and parent fields are always indexed by LuceneNgDocumentMaker.
        return true;
    }

    /**
     * The property type that decides the Lucene field of {@code fieldName}: the declared type, or
     * String when the rules of this index declare the field with incompatible types (Lucene requires
     * one field shape across all documents).
     */
    public int fieldType(String fieldName, PropertyDefinition definition) {
        return conflictingFields.contains(fieldName) ? PropertyType.STRING : definition.getType();
    }

    private Set<String> conflictingFields() {
        Set<String> declaredNames = new HashSet<>();
        for (IndexingRule rule : getDefinedRules()) {
            for (PropertyDefinition pd : rule.getProperties()) {
                if (!pd.isRegexp) {
                    declaredNames.add(pd.name);
                }
            }
        }
        Set<String> conflicting = new HashSet<>();
        for (String name : declaredNames) {
            Map<String, String> shapeByRule = new TreeMap<>();
            for (IndexingRule rule : getDefinedRules()) {
                PropertyDefinition pd = rule.getConfig(name);
                String shape = pd != null && (pd.propertyIndex || pd.ordered) ? fieldShape(pd.getType()) : null;
                if (shape != null) {
                    shapeByRule.put(rule.getNodeTypeName(), shape);
                }
            }
            if (new HashSet<>(shapeByRule.values()).size() > 1) {
                conflicting.add(name);
                if (REPORTED_CONFLICTS.add(getIndexPath() + "|" + name)) {
                    LOG.warn("Index {} declares property {} with incompatible types {}; it is indexed and queried as String",
                            getIndexPath(), name, shapeByRule);
                }
            }
        }
        return conflicting;
    }

    @Nullable
    private static String fieldShape(int type) {
        switch (type) {
            case PropertyType.LONG:
            case PropertyType.DATE:
                return "Long";
            case PropertyType.DOUBLE:
            case PropertyType.DECIMAL:
                return "Double";
            case PropertyType.BINARY:
                return null;
            default:
                return "String";
        }
    }

    /**
     * Gets the index name (last segment of index path).
     *
     * @return the index name
     */
    public String getIndexName() {
        return PathUtils.getName(getIndexPath());
    }

    /**
     * Repository path where Lucene segment files for this index are stored
     * ({@link LuceneNgIndexStorage} child under the definition).
     *
     * @return e.g. {@code /oak:index/myIndex/:luceneNg}
     */
    public String getStoragePath() {
        return LuceneNgIndexStorage.storagePath(getIndexPath());
    }

    /**
     * Builder for {@link LuceneNgIndexDefinition}, mirroring
     * {@code LuceneIndexDefinition.Builder} in {@code oak-lucene}. Required by the shared
     * {@code FulltextIndexEditorContext} (see {@code IndexDefinition.Builder}).
     */
    public static class Builder extends IndexDefinition.Builder<LuceneNgIndexDefinition> {
        @Override
        public LuceneNgIndexDefinition build() {
            return super.build();
        }

        @Override
        public Builder reindex() {
            super.reindex();
            return this;
        }

        @Override
        protected LuceneNgIndexDefinition createInstance(NodeState indexDefnStateToUse) {
            return new LuceneNgIndexDefinition(root, indexDefnStateToUse, version, uid, indexPath);
        }
    }
}
