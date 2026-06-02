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
package org.apache.jackrabbit.oak.run.cli;

import org.junit.Test;

import static org.junit.Assert.assertThrows;

/**
 * Verifies that tika-core is not transitively available in oak-run-commons.
 *
 * oak-run-commons depends on jackrabbit-core, which declares tika-core as a
 * transitive dependency. The exclusion in the jackrabbit-core dependency block
 * in pom.xml is required to prevent tika-core from leaking onto the classpath
 * (and ultimately into the assembled oak-run and oak-run-elastic JARs at a
 * version that may differ from the one declared in oak-parent).
 *
 * If this test fails it means tika-core is reachable via jackrabbit-core's
 * transitive dependencies. Restore the tika-core exclusion in the
 * jackrabbit-core dependency in oak-run-commons/pom.xml.
 */
public class TikaExclusionTest {

    @Test
    public void tikaCoreNotTransitivelyAvailable() {
        assertThrows(ClassNotFoundException.class, () -> Class.forName("org.apache.tika.Tika"));
}
}
