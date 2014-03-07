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

/**
 * interface for shared constants around different actors: QueryIndex, IndexEditors,
 * IndexEditorProviders, ...
 */
public interface OrderedIndex {
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
        private OrderDirection(String direction){
            this.direction = direction;
        }
        public String getDirection(){
            return direction;
        }
        
        /**
         * retrieve an {@code OrderDirection} from a provided String. Will return null in case of no-match
         * 
         * @param direction the direction of the sorting: ascending or descending
         * @return
         */
        public static @Nullable @CheckForNull OrderDirection fromString(@Nonnull final String direction){
            for(OrderDirection d : OrderDirection.values()){
                if(d.getDirection().equalsIgnoreCase(direction)){
                    return d;
                }
            }
            return null;
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
