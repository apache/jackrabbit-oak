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
package org.apache.jackrabbit.oak.spi.xml;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility class defining specific, configurable import behavior. A given
 * implementation of the {@link ProtectedItemImporter} may support this
 * as part of the overall import configuration.
 */
public final class ImportBehavior {

    private static final Logger log = LoggerFactory.getLogger(ImportBehavior.class);

    /**
     * If a value cannot be set due to constraints
     * enforced by the API implementation, the failure is logged as
     * warning and the value is ignored.
     */
    public static final int IGNORE = 1;
    /**
     * Same as {@link #IGNORE} but in addition tries to circumvent the
     * problem. This option should only be used with validated and trusted
     * XML passed to the {@code Session} and {@code Workspace} import API.
     */
    public static final int BESTEFFORT = 2;
    /**
     * Aborts the import as soon as invalid values are detected throwing
     * a {@code ConstraintViolationException}.
     */
    public static final int ABORT = 3;

    public static final String NAME_IGNORE = "ignore";
    public static final String NAME_BESTEFFORT = "besteffort";
    public static final String NAME_ABORT = "abort";

    /**
     * Private constructor to avoid instantiation.
     */
    private ImportBehavior() {}

    public static int valueFromString(String behaviorString) {
        if (NAME_IGNORE.equalsIgnoreCase(behaviorString)) {
            return IGNORE;
        } else if (NAME_BESTEFFORT.equalsIgnoreCase(behaviorString)) {
            return BESTEFFORT;
        } else if (NAME_ABORT.equalsIgnoreCase(behaviorString)) {
            return ABORT;
        } else {
            log.error("Invalid behavior " + behaviorString + " -> Using default: ABORT.");
            return ABORT;
        }
    }

    public static String nameFromValue(int importBehavior) {
        switch (importBehavior) {
            case ImportBehavior.IGNORE:
                return NAME_IGNORE;
            case ImportBehavior.ABORT:
                return NAME_ABORT;
            case ImportBehavior.BESTEFFORT:
                return NAME_BESTEFFORT;
            default:
                throw new IllegalArgumentException("Invalid import behavior: " + importBehavior);
        }
    }
}