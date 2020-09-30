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
package org.apache.jackrabbit.oak.segment.aws;

import static org.apache.jackrabbit.oak.segment.aws.Configuration.PID;

import org.osgi.service.metatype.annotations.AttributeDefinition;
import org.osgi.service.metatype.annotations.ObjectClassDefinition;

@ObjectClassDefinition(
        pid = {PID},
        name = "Apache Jackrabbit Oak AWS Segment Store Service",
        description = "AWS backend for the Oak Segment Node Store")
public @interface Configuration {

    String PID = "org.apache.jackrabbit.oak.segment.aws.AwsSegmentStoreService";

    @AttributeDefinition(
            name = "AWS account access key",
            description = "Access key which should be used to authenticate on the account")
    String accessKey();

    @AttributeDefinition(
            name = "AWS bucket name",
            description = "Name of the bucket to use. If it doesn't exists, it'll be created.")
    String bucketName();

    @AttributeDefinition(
            name = "Root directory",
            description = "Names of all the created blobs will be prefixed with this path")
    String journalTableName() default AwsSegmentStoreService.DEFAULT_JOURNALTABLE_NAME;

    @AttributeDefinition(
            name = "Root directory",
            description = "Names of all the created blobs will be prefixed with this path")
    String lockTableName() default AwsSegmentStoreService.DEFAULT_LOCKTABLE_NAME;

    @AttributeDefinition(
            name = "AWS region",
            description = "AWS region in which the resources are created or expected to be present")
    String region() default AwsSegmentStoreService.DEFAULT_REGION_NAME;

    @AttributeDefinition(
            name = "Root directory",
            description = "Names of all the created blobs will be prefixed with this path")
    String rootDirectory() default AwsSegmentStoreService.DEFAULT_ROOT_DIRECTORY;

    @AttributeDefinition(
            name = "AWS account secret key",
            description = "Secret key which should be used to authenticate on the account")
    String secretKey();

    @AttributeDefinition(
            name = "AWS session token",
            description = "Session token which should be used to authenticate on the account")
    String sessionToken() default "";
}