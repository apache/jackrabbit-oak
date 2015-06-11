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

package org.apache.jackrabbit.oak.remote;

import java.util.Set;

import static com.google.common.collect.Sets.newHashSet;

/**
 * Represents a set of filters that can be applied when a subtree is read from
 * the repository.
 */
public class RemoteTreeFilters {

    /**
     * Return the depth of the tree to read. This method returns {@code 0} by
     * default, meaning that only the root of the subtree will be returned. The
     * default value makes read operation for a subtree look like the read
     * operation for a single node.
     */
    public int getDepth() {
        return 0;
    }

    /**
     * Return the property filters. This method returns {@code {"*"}} by
     * default, meaning that every property will be returned for every node
     * included in the subtree.
     */
    public Set<String> getPropertyFilters() {
        return newHashSet("*");
    }

    /**
     * Return the node filters. This method returns a value of {@code {"*"}} by
     * default, meaning that every descendant node of the root of the tree wil
     * be returned.
     */
    public Set<String> getNodeFilters() {
        return newHashSet("*");
    }

    /**
     * Return the binary threshold. This method returns {@code 0} by default,
     * meaning that by default binary properties will be returned as references
     * to binary objects, instead of being returned as proper binary objects.
     */
    public long getBinaryThreshold() {
        return 0;
    }

    /**
     * Return the start index for children. This method returns {@code 0} by
     * default, meaning that children will be read from the beginning when
     * reading the root node and every descendant.
     */
    public int getChildrenStart() {
        return 0;
    }

    /**
     * Return the maximum number of children to return. This method returns
     * {@code -1} by default, meaning that every children will be returned when
     * reading the root node and every descendant.
     */
    public int getChildrenCount() {
        return -1;
    }

}
