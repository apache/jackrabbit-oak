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

package org.apache.jackrabbit.oak.api.jmx;

import org.osgi.annotation.versioning.ProviderType;

import javax.management.openmbean.TabularData;

/**
 * MBean for managing the copy-on-write node store
 */
@ProviderType
public interface CopyOnWriteStoreMBean {
    String TYPE = "CopyOnWriteStoreManager";

    /**
     * Enabled the temporary, copy-on-write store
     * @return the operation status
     */
    String enableCopyOnWrite();

    /**
     * Disables the temporary store and switched the repository back to the "normal" mode.
     * @return the operation status
     */
    String disableCopyOnWrite();

    /**
     * Returns the copy-on-write status
     * @return status of the copy-on-write mode
     */
    String getStatus();
}
