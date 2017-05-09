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
package org.apache.jackrabbit.oak.query.ast;

import java.util.Set;

/**
 * A nodetype info mechanism.
 */
public interface NodeTypeInfo {
    
    /**
     * Check whether the nodetype exists.
     * 
     * @return true if it exists
     */
    boolean exists();

    /**
     * Get the name of the nodetype.
     * 
     * @return the fully qualified name
     */
    String getNodeTypeName();

    /**
     * Get the set of supertypes.
     * 
     * @return the set
     */
    Set<String> getSuperTypes();

    /**
     * Get the set of primary subtypes.
     * 
     * @return the set
     */
    Set<String> getPrimarySubTypes();

    /**
     * Get the set of mixin subtypes.
     * 
     * @return the set
     */
    Set<String> getMixinSubTypes();

    /**
     * Check whether this is a mixin.
     * 
     * @return true if it is a mixin, false if it is a primary type
     */
    boolean isMixin();

    /**
     * Get the names of all single-valued properties.
     * 
     * @return the names
     */
    Iterable<String> getNamesSingleValuesProperties();

}