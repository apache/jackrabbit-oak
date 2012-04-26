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
package org.apache.jackrabbit.oak.spi;

import org.apache.jackrabbit.oak.api.CoreValue;

/**
 * The filter for an index lookup.
 */
public interface Filter {

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

    /**
     * A restriction for a property.
     */
    static class PropertyRestriction {

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

        @Override
        public String toString() {
            return (first == null ? "" : ((firstIncluding ? "[" : "(") + first)) + ".." +
                    (last == null ? "" : last + (lastIncluding ? "]" : ")"));
        }

    }

    /**
     * The path restriction type.
     */
    public enum PathRestriction {

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
