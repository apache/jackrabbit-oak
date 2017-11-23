/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.jackrabbit.oak.plugins.index.property;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.jackrabbit.oak.spi.state.NodeState;

/**
 * interface for shared constants around different actors: QueryIndex, IndexEditors,
 * IndexEditorProviders, ...
 */
public interface OrderedIndex {
    /**
     * Deprecation message tracked when using the ordered index.
     */
    String DEPRECATION_MESSAGE =
        "Ordered Index has been deprecated since Oak 1.1.8. " +
        "Please replace the index definition ({}) with Lucene Property index " +
        "and remove the index providers from the repository. " +
        "See docs at http://jackrabbit.apache.org/oak/docs";
    /**
     * the deprecation message will be tracked every N times
     */
    int TRACK_DEPRECATION_EVERY = 10000;
    
    /**
     * enum for easing the order direction of the index
     */
    enum OrderDirection {
        /**
         * ascending order configuration (default)
         */
        ASC("ascending"), 
        
        /**
         * descending order configuration
         */
        DESC("descending");
        
        private final String direction;
        private OrderDirection(String direction) {
            this.direction = direction;
        }
        public String getDirection() {
            return direction;
        }
        
        /**
         * retrieve an {@code OrderDirection} from a provided String. Will return null in case of
         * no-match
         * 
         * @param direction the direction of the sorting: ascending or descending
         * @return the direction
         */
        @Nullable
        @CheckForNull
        public static OrderDirection fromString(@Nonnull final String direction) {
            for (OrderDirection d : OrderDirection.values()) {
                if (d.getDirection().equalsIgnoreCase(direction)) {
                    return d;
                }
            }
            return null;
        }
        
        /**
         * tells whether the provided index definition is ascending or descending
         * 
         * @param indexMeta
         * @return the direction
         */
        public static OrderDirection fromIndexMeta(final NodeState indexMeta) {
            OrderDirection direction = ASC;
            if (indexMeta != null && DESC.getDirection().equals(indexMeta.getString(DIRECTION))) {
                direction = DESC;
            }
            return direction;
        }
        
        /**
         * convenience method that tells if the provided index definition is descending
         * 
         * @param indexMeta
         * @return true if descending
         */
        public static boolean isDescending(NodeState indexMeta) {
            return DESC.equals(fromIndexMeta(indexMeta));
        }
        
        public boolean isAscending() {
            return ASC.equals(this);
        }
        
        public boolean isDescending() {
            return DESC.equals(this);
        }

        /**
         * convenience method that tells if the provided index definition is ascending
         * 
         * @param indexMeta
         * @return true if ascending
         */
        public static boolean isAscending(NodeState indexMeta) {
            return ASC.equals(fromIndexMeta(indexMeta));
        }
    };
    
    String TYPE = "ordered";
    
    /**
     * the 'key' used for specifying the direction of the index when providing the configuration
     * 
     * {@code  { "propertyNames"="foobar", "direction"="ascending" } }
     */
    String DIRECTION = "direction";
    
    /**
     * the default direction for sorting the index
     */
    OrderDirection DEFAULT_DIRECTION = OrderDirection.ASC;    
}
