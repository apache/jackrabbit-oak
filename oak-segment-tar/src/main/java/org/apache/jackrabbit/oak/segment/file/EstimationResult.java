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
<<<<<<< HEAD:oak-jcr/src/test/java/org/apache/jackrabbit/oak/jcr/security/authorization/MixReferenceableTest.java
package org.apache.jackrabbit.oak.jcr.security.authorization;

/**
 * Implementation of {@code AbstractAutoCreatedPropertyTest} for mix:referenceable
 * nodes.
 */
public class MixReferenceableTest extends AbstractAutoCreatedPropertyTest {

    String getNodeName() {
        return "referenceable";
    }

    String getMixinName() {
        return mixReferenceable;
    }
=======

package org.apache.jackrabbit.oak.segment.file;

class EstimationResult {

    private final boolean gcNeeded;

    private final String gcLog;

    EstimationResult(boolean gcNeeded, String gcLog) {
        this.gcNeeded = gcNeeded;
        this.gcLog = gcLog;
    }

    boolean isGcNeeded() {
        return gcNeeded;
    }

    String getGcLog() {
        return gcLog;
    }

>>>>>>> upstream/trunk:oak-segment-tar/src/main/java/org/apache/jackrabbit/oak/segment/file/EstimationResult.java
}
