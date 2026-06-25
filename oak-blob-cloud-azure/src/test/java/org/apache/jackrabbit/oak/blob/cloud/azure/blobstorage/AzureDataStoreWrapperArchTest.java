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
package org.apache.jackrabbit.oak.blob.cloud.azure.blobstorage;

import com.tngtech.archunit.junit.AnalyzeClasses;
import com.tngtech.archunit.junit.ArchTest;
import com.tngtech.archunit.junit.ArchUnitRunner;
import com.tngtech.archunit.lang.ArchRule;
import org.junit.runner.RunWith;

import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.noClasses;

@RunWith(ArchUnitRunner.class)
@AnalyzeClasses(packages = "org.apache.jackrabbit.oak.blob.cloud.azure.blobstorage")
public class AzureDataStoreWrapperArchTest {

    // v8 classes must not reference v12 — except AzureDataStoreWrapper (the intentional bridge).
    // Test classes (ending in Test/IT) are excluded: test infrastructure routinely crosses
    // package boundaries to access helpers like AzuriteDockerRule and AzureDataStoreV12.
    @ArchTest
    static final ArchRule v8MustNotReferenceV12 = noClasses()
            .that().resideInAPackage("..azure.blobstorage")
            .and().areNotAssignableTo(AzureDataStoreWrapper.class)
            .and().areNotAssignableTo(AzureDataStoreWrapper.DelegatingDataStore.class)
            .and().haveSimpleNameNotEndingWith("Test")
            .and().haveSimpleNameNotEndingWith("IT")
            .should().dependOnClassesThat()
            .resideInAPackage("..azure.blobstorage.v12..");

    // v12 classes must not reference v8 — AzureDataStoreWrapper owns the one-way bridge.
    // Test classes (ending in Test/IT) are excluded for the same reason as above.
    @ArchTest
    static final ArchRule v12MustNotReferenceV8 = noClasses()
            .that().resideInAPackage("..azure.blobstorage.v12..")
            .and().haveSimpleNameNotEndingWith("Test")
            .and().haveSimpleNameNotEndingWith("IT")
            .should().dependOnClassesThat()
            .resideInAPackage("..azure.blobstorage");
}
