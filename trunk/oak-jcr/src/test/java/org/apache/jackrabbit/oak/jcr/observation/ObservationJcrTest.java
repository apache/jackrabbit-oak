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
package org.apache.jackrabbit.oak.jcr.observation;

import junit.framework.Test;
import junit.framework.TestSuite;
import org.apache.jackrabbit.core.observation.MixinTest;
import org.apache.jackrabbit.core.observation.MoveInPlaceTest;
import org.apache.jackrabbit.core.observation.ReorderTest;
import org.apache.jackrabbit.core.observation.ShareableNodesTest;
import org.apache.jackrabbit.oak.jcr.tck.TCKBase;
import org.apache.jackrabbit.test.ConcurrentTestSuite;

public class ObservationJcrTest extends TCKBase {

    public ObservationJcrTest() {
        super("Jackrabbit Observation tests");
    }

    public static Test suite() {
        return new ObservationJcrTest();
    }

    @Override
    protected void addTests() {
        TestSuite tests = new ConcurrentTestSuite("Jackrabbit Observation tests");
        tests.addTestSuite(ReorderTest.class);
        tests.addTestSuite(MoveInPlaceTest.class);
        tests.addTestSuite(MixinTest.class);
        tests.addTestSuite(ShareableNodesTest.class);
        addTest(tests);
    }
}
