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
package org.apache.jackrabbit.oak.plugins.index.property;

import static org.apache.jackrabbit.oak.query.QueryEngineImpl.ForceOptimised.OPTIMISED;

import org.junit.Before;

/**
 * should be executing the {@link MultiPropertyOrTest} by forcing the optimisation in place.
 */
public class MultiPropertyOrTestOptimisation extends MultiPropertyOrTest {
    
    @Override
    @Before
    public void before() throws Exception {
        super.before();
        setForceOptimised(OPTIMISED);
        setTraversalEnabled(false);
    }    
}
