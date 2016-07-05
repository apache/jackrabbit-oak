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
package org.apache.jackrabbit.oak.commons;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * helper class that return the list of available fixtures based on the {@code ns-fixtures} system
 * property ({@code -Dns-fixtures=SEGMENT_MK}).
 * 
 * See {@link FixturesHelper#Fixture} for a list of available fixtures
 */
public class FixturesHelper {
    /**
     * splitter for specifying multiple fixtures
     */

    private static final String SPLIT_ON = ",";
    /**
     * System property to be used.
     */
    public static final String NS_FIXTURES = "ns-fixtures";

    /**
     * default fixtures when no {@code ns-fixtures} is provided
     */
    public static enum Fixture {
       DOCUMENT_MK, DOCUMENT_NS, SEGMENT_MK, DOCUMENT_RDB
    };

    private static final Set<Fixture> FIXTURES;
    static {
        String raw = System.getProperty(NS_FIXTURES, "");
        if (raw.trim().isEmpty()) {
            FIXTURES = Collections.unmodifiableSet(new HashSet<Fixture>(Arrays.asList(Fixture
                .values())));
        } else {
            String[] fs = raw.split(SPLIT_ON);
            Set<Fixture> tmp = new HashSet<Fixture>();
            boolean unknownFixture = false;
            for (String f : fs) {
                String x = f.trim().toUpperCase();
                try {
                    Fixture fx = Fixture.valueOf(x);
                    tmp.add(fx);
                } catch (IllegalArgumentException e){
                    //This fixture is not present in branches
                    //so would need to be ignored
                    if (!"SEGMENT_TAR".equals(x)){
                        throw e;
                    } else {
                        unknownFixture = true;
                    }
                }
            }

            //If SEGMENT_TAR is missing (true for branches) then
            //ensure that tmp maps to SEGMENT_MK to avoid running all fixture
            if (tmp.isEmpty() && unknownFixture){
                tmp.add(Fixture.SEGMENT_MK);
            }

            if (tmp.isEmpty()) {
                FIXTURES = Collections.unmodifiableSet(new HashSet<Fixture>(Arrays.asList(Fixture
                    .values())));
            } else {
                FIXTURES = Collections.unmodifiableSet(tmp);
            }
            
        }
    }

    public static Set<Fixture> getFixtures() {
        return FIXTURES;
    }
}
