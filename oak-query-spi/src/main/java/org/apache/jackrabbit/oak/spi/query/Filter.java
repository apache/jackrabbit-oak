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
package org.apache.jackrabbit.oak.spi.query;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.jcr.PropertyType;

import org.apache.jackrabbit.oak.api.PropertyValue;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.spi.query.fulltext.FullTextExpression;

/**
 * The filter for an index lookup that contains a number of restrictions that
 * are combined with AND. Possible restrictions are a property restriction, a
 * path restriction, a node type restriction, and a fulltext restriction.
 * <p>
 * A property restriction could be that the property must exist, or that the
 * property value has to be within a certain range.
 * <p>
 * A path restriction could be a restriction to a certain subtree, a parent of a
 * certain path, or equality to a certain path.
 */
public interface Filter {

    /**
     * Get the list of property restrictions, if any. Each property may contain
     * multiple restrictions, for example x=1 and x=2. For this case, only
     * multi-valued properties match that contain both 1 and 2.
     *
     * @return the conditions (an empty collection if not used)
     */
    Collection<PropertyRestriction> getPropertyRestrictions();

    /**
     * Get the fulltext search conditions, if any.
     *
     * @return the conditions (an empty collection if not used)
     */
    @Deprecated
    Collection<String> getFulltextConditions();

    /**
     * Get the fulltext search condition expression, if any.
     *
     * @return the condition (null if none)
     */
    FullTextExpression getFullTextConstraint();

    QueryLimits getQueryLimits();

    /**
     * check whether a certain (valid) path is accessible (can be read) from the user associated with the query Session
     * @param path a valid JCR path
     * @return <code>true</code> if path can be read by the calling user, <code>false</code> otherwise.
     */
    boolean isAccessible(String path);

    /**
     * Whether the filter contains a native condition.
     *
     * @return true if it does
     */
    boolean containsNativeConstraint();

    /**
     * Get the most restrictive property restriction for the given property, if
     * any.
     *
     * @param propertyName the property name
     * @return the first restriction, or null if there is no restriction for
     *         this property
     */
    PropertyRestriction getPropertyRestriction(String propertyName);

    /**
     * Get the all property restriction for the given property.
     *
     * @param propertyName the property name
     * @return the list of restrictions (possibly empty, never null)
     */
    List<PropertyRestriction> getPropertyRestrictions(String propertyName);

    /**
     * Get the path restriction type.
     *
     * @return the path restriction type
     */
    PathRestriction getPathRestriction();

    /**
     * Get the path, or "/" if there is no path restriction set.
     *
     * @return the path
     */
    String getPath();

    /**
     * Get the plan for the path.
     *
     * @return the plan
     */
    String getPathPlan();

    /**
     * Returns the name of the filter node type.
     *
     * @return nodetype name
     */
    @Nullable
    String getNodeType();

    /**
     * Checks whether nodes of all types can match this filter.
     *
     * @return {@code true} iff there are no type restrictions
     */
    boolean matchesAllTypes();

    /**
     * Returns the names of the filter node type and all its supertypes.
     *
     * @return supertype name
     */
    @Nonnull
    Set<String> getSupertypes();

    /**
     * Returns the names of all matching primary node types.
     *
     * @return primary node type names
     */
    @Nonnull
    Set<String> getPrimaryTypes();

    /**
     * Returns the names of all matching mixin node types.
     *
     * @return mixin node type names
     */
    @Nonnull
    Set<String> getMixinTypes();

    /**
     * Get the complete query statement. The statement should only be used for
     * logging purposes.
     *
     * @return the query statement (possibly null)
     */
    @Nullable
    String getQueryStatement();

    /**
     * If the filter condition can not possibly match any row, due to a
     * contradiction in the query (for example "x=1 and x=2").
     *
     * @return true if the filter condition can not match any row
     */
    boolean isAlwaysFalse();

    /**
     * A restriction for a property.
     */
    class PropertyRestriction {

        /**
         * The name of the property.
         */
        public String propertyName;

        /**
         * The first value to read, or null to read from the beginning.
         */
        public PropertyValue first;

        /**
         * Whether values that match the first should be returned.
         */
        public boolean firstIncluding;

        /**
         * The last value to read, or null to read until the end.
         */
        public PropertyValue last;

        /**
         * Whether values that match the last should be returned.
         */
        public boolean lastIncluding;

        /**
         * Whether this is a like constraint. in this case only the 'first'
         * value should be taken into consideration
         */
        public boolean isLike;

        /**
         * A list of possible values, for conditions of the type
         * "x=1 or x=2 or x=3".
         */
        public List<PropertyValue> list;

        /**
         * The property type, if restricted.
         * If not restricted, this field is set to PropertyType.UNDEFINED.
         */
        public int propertyType = PropertyType.UNDEFINED;

        public boolean isNullRestriction() {
            return first == null && last == null && list == null && lastIncluding && firstIncluding;
        }

        public boolean isNotNullRestriction() {
            return first == null && last == null && list == null && !lastIncluding && !firstIncluding;
        }

        @Override
        public String toString() {
            return (toStringFromTo() + " " + toStringList()).trim();
        }

        private String toStringList() {
            if (list == null) {
                return "";
            }
            StringBuilder buff = new StringBuilder("in(");
            int i = 0;
            for (PropertyValue p : list) {
                if (i++ > 0) {
                    buff.append(", ");
                }
                buff.append(p.toString());
            }
            buff.append(')');
            return buff.toString();
        }

        private String toStringFromTo() {
            if (isNullRestriction()) {
                return "is null";
            } else if (isNotNullRestriction()) {
                return "is not null";
            }
            String f = first == null ? "" : first.toString();
            String l = last == null ? "" : last.toString();
            if (f.equals(l)) {
                return f;
            }
            String fi = first == null ? "" : (firstIncluding ? "[" : "(");
            String li = last == null ? "" : (lastIncluding ? "]" : ")");
            return fi + f + ".." + l + li;
        }

        /**
         * How restrictive a condition is.
         *
         * @return 0 for "is not null", 10 for equality, and 5 for everything
         *         else
         */
        public int sortOrder() {
            if (first == null && last == null) {
                if (list == null) {
                    return 0;
                }
                return 5;
            }
            if (first == last) {
                return 10;
            }
            return 5;
        }

        @Override
        public int hashCode() {
            // generated code (Eclipse)
            final int prime = 31;
            int result = 1;
            result = prime * result + ((first == null) ? 0 : first.hashCode());
            result = prime * result + (firstIncluding ? 1231 : 1237);
            result = prime * result + (isLike ? 1231 : 1237);
            result = prime * result + ((last == null) ? 0 : last.hashCode());
            result = prime * result + (lastIncluding ? 1231 : 1237);
            result = prime * result + ((list == null) ? 0 : list.hashCode());
            result = prime * result +
                    ((propertyName == null) ? 0 : propertyName.hashCode());
            result = prime * result + propertyType;
            return result;
        }

        @Override
        public boolean equals(Object obj) {
            // generated code (Eclipse)
            if (this == obj) {
                return true;
            }
            if (obj == null) {
                return false;
            }
            if (getClass() != obj.getClass()) {
                return false;
            }
            PropertyRestriction other = (PropertyRestriction) obj;
            if (first == null) {
                if (other.first != null) {
                    return false;
                }
            } else if (!first.equals(other.first)) {
                return false;
            }
            if (firstIncluding != other.firstIncluding) {
                return false;
            }
            if (isLike != other.isLike) {
                return false;
            }
            if (last == null) {
                if (other.last != null) {
                    return false;
                }
            } else if (!last.equals(other.last)) {
                return false;
            }
            if (lastIncluding != other.lastIncluding) {
                return false;
            }
            if (list == null) {
                if (other.list != null) {
                    return false;
                }
            } else if (!list.equals(other.list)) {
                return false;
            }
            if (propertyName == null) {
                if (other.propertyName != null) {
                    return false;
                }
            } else if (!propertyName.equals(other.propertyName)) {
                return false;
            }
            if (propertyType != other.propertyType) {
                return false;
            }
            return true;
        }

    }

    /**
     * The path restriction type.
     */
    enum PathRestriction {

        /**
         * All nodes.
         */
        NO_RESTRICTION("*"),

        /**
         * A parent of this node.
         */
        PARENT("/.."),

        /**
         * This exact node only.
         */
        EXACT(""),

        /**
         * All direct child nodes.
         */
        DIRECT_CHILDREN("/*"),

        /**
         * All direct and indirect child nodes (excluding the node with the
         * given path).
         */
        ALL_CHILDREN("//*");

        private final String name;

        PathRestriction(String name) {
            this.name = name;
        }

        @Override
        public String toString() {
            return name;
        }

    }

    Filter EMPTY_FILTER = new Filter() {

        private final QueryLimits EMPTY_LIMITS = new QueryLimits() {

            @Override
            public long getLimitInMemory() {
                return Long.MAX_VALUE;
            }

            @Override
            public long getLimitReads() {
                return Long.MAX_VALUE;
            }

            @Override
            public boolean getFullTextComparisonWithoutIndex() {
                return false;
            }

            @Override
            public boolean getFailTraversal() {
                return false;
            }

        };

        @Override
        public Collection<PropertyRestriction> getPropertyRestrictions() {
            return Collections.emptyList();
        }

        @Override
        public Collection<String> getFulltextConditions() {
            return Collections.emptyList();
        }

        @Override
        public FullTextExpression getFullTextConstraint() {
            return null;
        }

        @Override
        public QueryLimits getQueryLimits() {
            return EMPTY_LIMITS;
        }

        @Override
        public boolean isAccessible(String path) {
            return false;
        }

        @Override
        public boolean containsNativeConstraint() {
            return false;
        }

        @Override
        public PropertyRestriction getPropertyRestriction(String propertyName) {
            return null;
        }

        @Override
        public List<PropertyRestriction> getPropertyRestrictions(String propertyName) {
            return Collections.emptyList();
        }

        @Override
        public PathRestriction getPathRestriction() {
            return PathRestriction.NO_RESTRICTION;
        }

        @Override
        public String getPath() {
            return PathUtils.ROOT_PATH;
        }

        @Override
        public String getPathPlan() {
            return PathRestriction.NO_RESTRICTION.toString();
        }

        @Nullable
        @Override
        public String getNodeType() {
            return null;
        }

        @Override
        public boolean matchesAllTypes() {
            return false;
        }

        @Nonnull
        @Override
        public Set<String> getSupertypes() {
            return Collections.emptySet();
        }

        @Nonnull
        @Override
        public Set<String> getPrimaryTypes() {
            return Collections.emptySet();
        }

        @Nonnull
        @Override
        public Set<String> getMixinTypes() {
            return Collections.emptySet();
        }

        @Nullable
        @Override
        public String getQueryStatement() {
            return null;
        }

        @Override
        public boolean isAlwaysFalse() {
            return false;
        }
    };

}
