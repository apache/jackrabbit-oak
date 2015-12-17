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
package org.apache.jackrabbit.oak.spi.state;

import javax.annotation.Nonnull;

/**
 * Interface for bearing cluster node specific information.
 */
public interface Clusterable {
    /**
     * <p>
     * Will return a unique number per instance across the cluster. It will only make its best
     * effort to preserve the same number across restarts but it must be unique across the cluster.
     * </p>
     * 
     * @return Cannot be null or empty.
     */
    @Nonnull
    String getInstanceId();
}
