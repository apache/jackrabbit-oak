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
package org.apache.jackrabbit.oak.spi.lifecycle;

import org.apache.jackrabbit.mk.api.MicroKernel;
import org.apache.jackrabbit.oak.spi.state.NodeStore;

/**
 * This interface is mainly used in an OSGi environment where various components
 * of Oak are started by container and one would like to plug in some code that
 * is executed when the micro kernel becomes available in the system.
 */
public interface MicroKernelTracker {

    /**
     * This method is called when both the {@link MicroKernel} service and this
     * tracker become available in the system.
     * @param mk the {@link MicroKernel} instance.
     */
    public void available(NodeStore store);

}
