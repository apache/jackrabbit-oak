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

package org.apache.jackrabbit.oak.plugins.blob.migration;

import javax.management.openmbean.CompositeData;
import javax.management.openmbean.OpenDataException;

import org.apache.jackrabbit.oak.api.jmx.Description;
import org.apache.jackrabbit.oak.api.jmx.Name;
import org.jetbrains.annotations.NotNull;

public interface BlobMigrationMBean {

    String TYPE = "BlobMigration";

    @NotNull
    @Description("Start or resume the blob migration")
    String startBlobMigration(
            @Name("resume") @Description("true to resume stopped migration or false to start it from scratch") boolean resume);

    @NotNull
    @Description("Stop the blob migration")
    String stopBlobMigration();

    @NotNull
    CompositeData getBlobMigrationStatus() throws OpenDataException;

}
