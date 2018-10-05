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
package org.apache.jackrabbit.oak.segment.azure;

import org.osgi.service.metatype.annotations.AttributeDefinition;
import org.osgi.service.metatype.annotations.ObjectClassDefinition;

import static org.apache.jackrabbit.oak.segment.azure.Configuration.PID;

@ObjectClassDefinition(
        pid = {PID},
        name = "Apache Jackrabbit Oak Azure Segment Store Service",
        description = "Azure backend for the Oak Segment Node Store")
@interface Configuration {

    String PID = "org.apache.jackrabbit.oak.segment.azure.AzureSegmentStoreService";

    @AttributeDefinition(
            name = "Azure account name",
            description = "Name of the Azure Storage account to use.")
    String accountName();

    @AttributeDefinition(
            name = "Azure container name",
            description = "Name of the container to use. If it doesn't exists, it'll be created.")
    String containerName() default AzureSegmentStoreService.DEFAULT_CONTAINER_NAME;

    @AttributeDefinition(
            name = "Azure account access key",
            description = "Access key which should be used to authenticate on the account")
    String accessKey();

    @AttributeDefinition(
            name = "Root path",
            description = "Names of all the created blobs will be prefixed with this path")
    String rootPath() default AzureSegmentStoreService.DEFAULT_ROOT_PATH;

    @AttributeDefinition(
            name = "Azure connection string (optional)",
            description = "Connection string to be used to connect to the Azure Storage. " +
                    "Setting it will override the accountName and accessKey properties.")
    String connectionURL() default "";
}