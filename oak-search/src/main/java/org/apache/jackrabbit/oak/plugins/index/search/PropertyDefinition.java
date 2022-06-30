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
package org.apache.jackrabbit.oak.plugins.index.search;

import javax.jcr.PropertyType;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.plugins.index.property.ValuePattern;
import org.apache.jackrabbit.oak.plugins.index.search.IndexDefinition.IndexingRule;
import org.apache.jackrabbit.oak.plugins.index.search.util.FunctionIndexProcessor;
import org.apache.jackrabbit.oak.plugins.index.search.util.IndexHelper;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.collect.ImmutableList.copyOf;
import static com.google.common.collect.Iterables.toArray;
import static org.apache.jackrabbit.oak.commons.PathUtils.elements;
import static org.apache.jackrabbit.oak.commons.PathUtils.isAbsolute;
import static org.apache.jackrabbit.oak.plugins.index.search.FulltextIndexConstants.FIELD_BOOST;
import static org.apache.jackrabbit.oak.plugins.index.search.FulltextIndexConstants.PROP_IS_REGEX;
import static org.apache.jackrabbit.oak.plugins.index.search.FulltextIndexConstants.PROP_WEIGHT;
import static org.apache.jackrabbit.oak.plugins.index.search.FulltextIndexConstants.PROP_SIMILARITY_SEARCH_DENSE_VECTOR_SIZE;
import static org.apache.jackrabbit.oak.plugins.index.search.spi.query.FulltextIndexPlanner.DEFAULT_PROPERTY_WEIGHT;
import static org.apache.jackrabbit.oak.plugins.index.search.util.ConfigUtil.getOptionalValue;

public class PropertyDefinition {
    private static final Logger log = LoggerFactory.getLogger(PropertyDefinition.class);

    private static final String[] EMPTY_ANCESTORS = new String[0];

    /**
     * The default boost: 1.0f.
     */
    static final float DEFAULT_BOOST = 1.0f;

    static final int SIMILARITY_SEARCH_DENSE_VECTOR_SIZE_DEFAULT = 1024;

    /**
     * Property name. By default derived from the NodeState name which has the
     * property definition. However in case property name is a pattern, relative
     * property etc then it should be defined via 'name' property in NodeState.
     * In such case NodeState name can be set to anything
     */
    public final String name;

    public final String nodeName;

    private final int propertyType;

    /**
     * The boost value for a property.
     */
    public final float boost;

    public final boolean isRegexp;

    public final boolean index;

    public final boolean stored;

    public final boolean nodeScopeIndex;

    public final boolean propertyIndex;

    public final boolean analyzed;

    public final boolean ordered;

    public final boolean nullCheckEnabled;

    public final boolean notNullCheckEnabled;

    final int includedPropertyTypes;

    public final boolean relative;

    public final boolean useInSuggest;

    public final boolean useInSpellcheck;

    public final boolean facet;

    public final String[] ancestors;

    public final boolean excludeFromAggregate;

    public final int weight;

    public final boolean dynamicBoost;

    /**
     * Property name excluding the relativePath. For regular expression based definition
     * its set to null
     */
    @Nullable
    public final String nonRelativeName;

    /**
     * For function-based indexes: the function name, in Polish notation.
     */
    public final String function;

    /**
     * For function-based indexes: the function code, as tokens.
     */
    public final String[] functionCode;

    public final ValuePattern valuePattern;

    public final boolean sync;

    public final boolean unique;

    public final boolean useInSimilarity;

    public final boolean similarityRerank;
    public final boolean similarityTags;
    private final int similaritySearchDVS;

    public PropertyDefinition(IndexingRule idxDefn, String nodeName, NodeState defn) {
        this.nodeName = nodeName;
        this.isRegexp = getOptionalValue(defn, PROP_IS_REGEX, false);
        this.name = getName(defn, nodeName);
        this.relative = isRelativeProperty(name);
        this.boost = getOptionalValue(defn, FIELD_BOOST, DEFAULT_BOOST);
        this.weight = getOptionalValue(defn, PROP_WEIGHT, DEFAULT_PROPERTY_WEIGHT);
        this.dynamicBoost = getOptionalValue(defn, FulltextIndexConstants.PROP_DYNAMIC_BOOST, false);

        //By default if a property is defined it is indexed
        this.index = getOptionalValue(defn, FulltextIndexConstants.PROP_INDEX, true);
        this.stored = getOptionalValueIfIndexed(defn, FulltextIndexConstants.PROP_USE_IN_EXCERPT, false);
        this.nodeScopeIndex = getOptionalValueIfIndexed(defn, FulltextIndexConstants.PROP_NODE_SCOPE_INDEX, false);

        //If boost is specified then that field MUST be analyzed
        if (defn.hasProperty(FIELD_BOOST)) {
            this.analyzed = true;
        } else {
            this.analyzed = getOptionalValueIfIndexed(defn, FulltextIndexConstants.PROP_ANALYZED, false);
        }

        this.ordered = getOptionalValueIfIndexed(defn, FulltextIndexConstants.PROP_ORDERED, false);
        this.includedPropertyTypes = IndexDefinition.getSupportedTypes(defn, FulltextIndexConstants.PROP_INCLUDED_TYPE,
                IndexDefinition.TYPES_ALLOW_ALL);

        //TODO Add test case for above cases

        this.propertyType = getPropertyType(idxDefn, nodeName, defn);
        this.useInSuggest = getOptionalValueIfIndexed(defn, FulltextIndexConstants.PROP_USE_IN_SUGGEST, false);
        this.useInSpellcheck = getOptionalValueIfIndexed(defn, FulltextIndexConstants.PROP_USE_IN_SPELLCHECK, false);
        this.useInSimilarity = getOptionalValueIfIndexed(defn, FulltextIndexConstants.PROP_USE_IN_SIMILARITY, false);
        this.similarityRerank = getOptionalValueIfIndexed(defn, FulltextIndexConstants.PROP_SIMILARITY_RERANK, true);
        this.similarityTags = getOptionalValueIfIndexed(defn, FulltextIndexConstants.PROP_SIMILARITY_TAGS, false);
        this.nullCheckEnabled = getOptionalValueIfIndexed(defn, FulltextIndexConstants.PROP_NULL_CHECK_ENABLED, false);
        this.notNullCheckEnabled = getOptionalValueIfIndexed(defn, FulltextIndexConstants.PROP_NOT_NULL_CHECK_ENABLED, false);
        this.excludeFromAggregate = getOptionalValueIfIndexed(defn, FulltextIndexConstants.PROP_EXCLUDE_FROM_AGGREGATE, false);
        this.similaritySearchDVS = getOptionalValue(defn, PROP_SIMILARITY_SEARCH_DENSE_VECTOR_SIZE,
                SIMILARITY_SEARCH_DENSE_VECTOR_SIZE_DEFAULT);
        this.nonRelativeName = determineNonRelativeName();
        this.ancestors = computeAncestors(name);
        this.facet = getOptionalValueIfIndexed(defn, FulltextIndexConstants.PROP_FACETS, false);
        this.function = FunctionIndexProcessor.convertToPolishNotation(
                getOptionalValue(defn, FulltextIndexConstants.PROP_FUNCTION, null));
        this.functionCode = FunctionIndexProcessor.getFunctionCode(this.function);
        this.valuePattern = new ValuePattern(defn);
        this.unique = getOptionalValueIfIndexed(defn, FulltextIndexConstants.PROP_UNIQUE, false);
        this.sync = unique || getOptionalValueIfIndexed(defn, FulltextIndexConstants.PROP_SYNC, false);

        //If some property is set to sync then propertyIndex mode is always enabled
        this.propertyIndex = sync || getOptionalValueIfIndexed(defn, FulltextIndexConstants.PROP_PROPERTY_INDEX, false);
        validate();
    }


    /**
     * If 'analyzed' is enabled then property value would be used to evaluate the
     * contains clause related to those properties. In such mode also some properties
     * would be skipped from analysis
     *
     * @param propertyName name of the property to check. As property definition might
     *                     be regEx based this is required to be passed explicitly
     * @return true if the property value should be tokenized/analyzed
     */
    public boolean skipTokenization(String propertyName) {
        //For regEx case check against a whitelist
        if (isRegexp && IndexHelper.skipTokenization(propertyName)) {
            return true;
        }
        return !analyzed;
    }

    public boolean fulltextEnabled() {
        return index && (analyzed || nodeScopeIndex);
    }

    public boolean propertyIndexEnabled() {
        return index && propertyIndex;
    }

    public boolean isTypeDefined() {
        return propertyType != PropertyType.UNDEFINED;
    }

    /**
     * Returns size of dense vector used for similarity search using this index.
     *
     * @return dense vector size
     */
    public int getSimilaritySearchDenseVectorSize() {
        return similaritySearchDVS;
    }


    /**
     * Returns the property type. If no explicit type is defined the default is assumed
     * to be {@link PropertyType#STRING}
     *
     * @return propertyType as per javax.jcr.PropertyType
     */
    public int getType() {
        //If no explicit type is defined we assume it to be string
        return isTypeDefined() ? propertyType : PropertyType.STRING;
    }

    public boolean includePropertyType(int type) {
        return IndexDefinition.includePropertyType(includedPropertyTypes, type);
    }

    @Override
    public String toString() {
        return "PropertyDefinition{" +
                "name='" + name + '\'' +
                ", propertyType=" + propertyType +
                ", boost=" + boost +
                ", isRegexp=" + isRegexp +
                ", index=" + index +
                ", stored=" + stored +
                ", nodeScopeIndex=" + nodeScopeIndex +
                ", propertyIndex=" + propertyIndex +
                ", analyzed=" + analyzed +
                ", ordered=" + ordered +
                ", useInSuggest=" + useInSuggest +
                ", useInSimilarity=" + useInSimilarity +
                ", nullCheckEnabled=" + nullCheckEnabled +
                ", notNullCheckEnabled=" + notNullCheckEnabled +
                ", function=" + function +
                '}';
    }

    static boolean isRelativeProperty(String propertyName) {
        return !isAbsolute(propertyName)
                && !FulltextIndexConstants.REGEX_ALL_PROPS.equals(propertyName)
                && PathUtils.getNextSlash(propertyName, 0) > 0;
    }

    //~---------------------------------------------< internal >

    private boolean getOptionalValueIfIndexed(NodeState definition, String propName, boolean defaultVal) {
        //If property is not to be indexed then all other config would be
        //set to false ignoring whatever is defined in config for them
        if (!index) {
            return false;
        }
        return getOptionalValue(definition, propName, defaultVal);
    }

    private void validate() {
        if (nullCheckEnabled && isRegexp) {
            throw new IllegalStateException(String.format("%s can be set to true for property definition using " +
                    "regular expression", FulltextIndexConstants.PROP_NULL_CHECK_ENABLED));
        }
    }

    private String determineNonRelativeName() {
        if (isRegexp) {
            return null;
        }

        if (!relative) {
            return name;
        }

        return PathUtils.getName(name);
    }

    private static String[] computeAncestors(String path) {
        if (FulltextIndexConstants.REGEX_ALL_PROPS.equals(path)) {
            return EMPTY_ANCESTORS;
        } else {
            return toArray(copyOf(elements(PathUtils.getParentPath(path))), String.class);
        }
    }


    private static String getName(NodeState definition, String defaultName) {
        PropertyState ps = definition.getProperty(FulltextIndexConstants.PROP_NAME);
        return ps == null ? defaultName : ps.getValue(Type.STRING);
    }

    private static int getPropertyType(IndexingRule idxDefn, String name, NodeState defn) {
        int type = PropertyType.UNDEFINED;
        if (defn.hasProperty(FulltextIndexConstants.PROP_TYPE)) {
            String typeName = defn.getString(FulltextIndexConstants.PROP_TYPE);
            try {
                type = PropertyType.valueFromName(typeName);
            } catch (IllegalArgumentException e) {
                log.warn("Invalid property type {} for property {} in Index {}", typeName, name, idxDefn);
            }
        }
        return type;
    }
}
