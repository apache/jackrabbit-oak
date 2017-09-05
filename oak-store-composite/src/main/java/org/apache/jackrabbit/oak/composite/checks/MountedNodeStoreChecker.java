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

package org.apache.jackrabbit.oak.composite.checks;

import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.composite.MountedNodeStore;
import org.apache.jackrabbit.oak.spi.mount.MountInfoProvider;
import org.apache.jackrabbit.oak.spi.state.NodeStore;

/**
 * Applies a category of consistence checks specific to <tt>NodeStore</tt> mounts
 * 
 * <p>Checks are only performed on non-default mounts.</p>
 * 
 * <p>Named 'Checker' to clarify that it is not a Validator in the Oak sense.</p> 
 *
 */
public interface MountedNodeStoreChecker<T> {
    
    public T createContext(NodeStore globalStore, MountInfoProvider mip);
    
    boolean check(MountedNodeStore mountedStore, Tree tree, ErrorHolder errorHolder, T context);

}
