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

import java.util.Collections;
import java.util.EnumSet;
import java.util.Locale;
import java.util.Set;

import org.apache.jackrabbit.oak.commons.properties.SystemPropertySupplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * helper class that return the list of available fixtures based on the {@code nsfixtures} system
 * property ({@code -Dnsfixtures=SEGMENT_TAR}).
 * 
 * See {@link FixturesHelper.Fixture} for a list of available fixtures
 */
public final class FixturesHelper {

    /**
     * splitter for specifying multiple fixtures
     */
    private static final String SPLIT_ON = ","; 

    private static final Logger LOG = LoggerFactory.getLogger(FixturesHelper.class);

    private static final String PROPNAME = "nsfixtures";

    /**
     * System property to be used.
     */
    public static final String RAW_FIXTURES = SystemPropertySupplier.create(PROPNAME, "").loggingTo(LOG).get().trim();

    private FixturesHelper() { }

    /**
     * default fixtures when no {@code nsfixtures} system property is provided
     */
    public enum Fixture {
       DOCUMENT_NS, @Deprecated SEGMENT_MK, DOCUMENT_RDB, MEMORY_NS, DOCUMENT_MEM, SEGMENT_TAR, SEGMENT_AWS, SEGMENT_AZURE_V8, SEGMENT_AZURE, COMPOSITE_SEGMENT, COMPOSITE_MEM, COW_DOCUMENT
    }

    private static final Set<Fixture> FIXTURES;

    private static final Set<Fixture> ALL_FIXTURES = Collections.unmodifiableSet(EnumSet.allOf(Fixture.class));

    static {
        if (RAW_FIXTURES.isEmpty()) {
            FIXTURES = ALL_FIXTURES;
        } else {
            Set<Fixture> tmp = EnumSet.noneOf(Fixture.class);
            boolean unknownFixture = false;
            for (String f : RAW_FIXTURES.split(SPLIT_ON)) {
                String x = f.trim().toUpperCase(Locale.ENGLISH);
                try {
                    Fixture fx = Fixture.valueOf(x);
                    tmp.add(fx);
                } catch (IllegalArgumentException e) {
                    // This fixture is not present in branches
                    // so would need to be ignored
                    if (!"SEGMENT_TAR".equals(x)) {
                        throw e;
                    } else {
                        unknownFixture = true;
                    }
                }
            }

            // If SEGMENT_TAR is missing (true for branches) then
            // ensure that tmp contains at least MEMORY_NS to avoid running all
            // fixtures
            if (tmp.isEmpty() && unknownFixture) {
                tmp.add(Fixture.MEMORY_NS);
            }

            FIXTURES = tmp.isEmpty() ? ALL_FIXTURES : Collections.unmodifiableSet(tmp);
        }

        LOG.info("Fixtures are {} (based on value of system property '{}' value '{}').", FIXTURES, PROPNAME, RAW_FIXTURES);
    }

    public static Set<Fixture> getFixtures() {
        return FIXTURES;
    }
}
