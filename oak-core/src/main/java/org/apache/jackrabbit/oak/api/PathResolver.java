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

package org.apache.jackrabbit.oak.api;

import com.google.common.base.Function;

/**
 * A path resolver is responsible for resolving {@link TreeLocation}s for the elements
 * in a path given its respective parent tree location.
 * <p/>
 * Each element in the iterator corresponds to an element of a path from the root
 * to leaf. Each element is a {@link Function} mapping from the current tree location
 * to the next one. The particulars of the mapping is determined on how implementations
 * of this interface interpret the corresponding path element.
 */
public interface PathResolver extends Iterable<Function<TreeLocation, TreeLocation>> { }
