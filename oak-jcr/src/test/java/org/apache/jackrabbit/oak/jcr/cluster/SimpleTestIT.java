/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.oak.jcr.cluster;

import org.junit.Test;

/**
 * A simple cluster test.
 */
public class SimpleTestIT extends AbstractClusterTest {
    
    @SuppressWarnings("unused")
    @Test 
    public void test() throws Exception {
        if (s1 == null) {
            return;
        }
        if (s1.getRootNode().hasNode("test")) {
            s1.getRootNode().getNode("test").remove();
        }
        s1.getRootNode().addNode("test");
        s1.save();
        for (int x : seconds(5)) {
            s2.refresh(false);
            if (s2.getRootNode().hasNode("test")) {
                break;
            }
        }
        s2.getRootNode().getNode("test").remove();
        s2.save();
        for (int x : seconds(5)) {
            s1.refresh(false);
            if (!s1.getRootNode().hasNode("test")) {
                break;
            }
        }
        s1.getRootNode().addNode("test");
        s1.save();
        for (int x : seconds(5)) {
            s2.refresh(false);
            if (s2.getRootNode().hasNode("test")) {
                break;
            }
        }
        s2.getRootNode().getNode("test").remove();
        s2.save();
    }
    
}
