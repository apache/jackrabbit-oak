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
import javax.jcr.PropertyType;

import org.apache.jackrabbit.oak.api.CoreValue;

/**
 * The filter for an index lookup.
 */
public interface Filter {

    /**
     * Get the list of property restrictions, if any.
     *
     * @return the conditions (an empty collection if not used)
     */
    Collection<PropertyRestriction> getPropertyRestrictions();

    /**
     * Get the fulltext search conditions, if any.
     *
     * @return the conditions (an empty collection if not used)
     */
    Collection<String> getFulltextConditions();

    /**
     * Get the property restriction for the given property, if any.
     *
     * @param propertyName the property name
     * @return the restriction, or null if there is no restriction for this property
     */
    PropertyRestriction getPropertyRestriction(String propertyName);

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

    String getNodeType();

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
        public CoreValue first;

        /**
         * Whether values that match the first should be returned.
         */
        public boolean firstIncluding;

        /**
         * The last value to read, or null to read until the end.
         */
        public CoreValue last;

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
         * The property type, if restricted.
         * If not restricted, this field is set to PropertyType.UNDEFINED.
         */
        public int propertyType = PropertyType.UNDEFINED;

        @Override
        public String toString() {
            return (first == null ? "" : ((firstIncluding ? "[" : "(") + first)) + ".." +
                    (last == null ? "" : last + (lastIncluding ? "]" : ")"));
        }

    }

    /**
     * The path restriction type.
     */
    enum PathRestriction {

        /**
         * A parent of this node
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
         * All direct and indirect child nodes.
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

}
