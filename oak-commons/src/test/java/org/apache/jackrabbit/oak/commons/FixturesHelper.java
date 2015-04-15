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

import static java.util.Collections.unmodifiableSet;

import java.util.EnumSet;
import java.util.Set;

/**
 * helper class that return the list of available fixtures based on the {@code nsfixtures} system
 * property ({@code -Dnsfixtures=SEGMENT_MK}).
 * 
 * See {@link FixturesHelper.Fixture} for a list of available fixtures
 */
public final class FixturesHelper {
    /**
     * splitter for specifying multiple fixtures
     */
    private static final String SPLIT_ON = ","; 
    /**
     * System property to be used.
     */
    public static final String NS_FIXTURES = "nsfixtures";

    private FixturesHelper() { }

    /**
     * default fixtures when no {@code nsfixtures} is provided
     */
    public enum Fixture {
       DOCUMENT_NS, SEGMENT_MK, DOCUMENT_RDB, MEMORY_NS
    }

    private static final Set<Fixture> FIXTURES;
    static {
        String raw = System.getProperty(NS_FIXTURES, "");
        if (raw.trim().isEmpty()) {
            FIXTURES = unmodifiableSet(EnumSet.allOf(Fixture.class));
        } else {
            Set<Fixture> tmp = EnumSet.noneOf(Fixture.class);
            for (String f : raw.split(SPLIT_ON)) {
                String x = f.trim();
                Fixture fx = Fixture.valueOf(x.toUpperCase());
                if (fx != null) {
                    tmp.add(fx);
                }
            }
            
            if (tmp.isEmpty()) {
                FIXTURES = unmodifiableSet(EnumSet.allOf(Fixture.class));
            } else {
                FIXTURES = unmodifiableSet(tmp);
            }
            
        }
    }

    public static Set<Fixture> getFixtures() {
        return FIXTURES;
    }
}
