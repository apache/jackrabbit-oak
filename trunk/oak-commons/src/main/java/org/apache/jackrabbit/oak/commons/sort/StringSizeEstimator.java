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
/**
 * 
 */
package org.apache.jackrabbit.oak.commons.sort;

/**
 * Source copied from a publicly available library.
 * @see <a
 *  href="https://code.google.com/p/externalsortinginjava/">https://code.google.com/p/externalsortinginjava</a>
 * 
 * @author Eleftherios Chetzakis
 * 
 */
public final class StringSizeEstimator {

    private static final int OBJ_HEADER;
    private static final int ARR_HEADER;
    private static final int INT_FIELDS = 12;
    private static final int OBJ_REF;
    private static final int OBJ_OVERHEAD;
    private static final boolean IS_64_BIT_JVM;

    /**
     * Private constructor to prevent instantiation.
     */
    private StringSizeEstimator() {
    }

    /**
     * Class initializations.
     */
    static {
        // By default we assume 64 bit JVM
        // (defensive approach since we will get
        // larger estimations in case we are not sure)
        boolean is64Bit = true;
        // check the system property "sun.arch.data.model"
        // not very safe, as it might not work for all JVM implementations
        // nevertheless the worst thing that might happen is that the JVM is 32bit
        // but we assume its 64bit, so we will be counting a few extra bytes per string object
        // no harm done here since this is just an approximation.
        String arch = System.getProperty("sun.arch.data.model");
        if (arch != null) {
            if (arch.indexOf("32") != -1) {
                // If exists and is 32 bit then we assume a 32bit JVM
                is64Bit = false;
            }
        }
        IS_64_BIT_JVM = is64Bit;
        // The sizes below are a bit rough as we don't take into account 
        // advanced JVM options such as compressed oops
        // however if our calculation is not accurate it'll be a bit over
        // so there is no danger of an out of memory error because of this.
        OBJ_HEADER = IS_64_BIT_JVM ? 16 : 8;
        ARR_HEADER = IS_64_BIT_JVM ? 24 : 12;
        OBJ_REF = IS_64_BIT_JVM ? 8 : 4;
        OBJ_OVERHEAD = OBJ_HEADER + INT_FIELDS + OBJ_REF + ARR_HEADER;

    }

    /**
     * Estimates the size of a {@link String} object in bytes.
     * 
     * @param s The string to estimate memory footprint.
     * @return The <strong>estimated</strong> size in bytes.
     */
    public static long estimatedSizeOf(String s) {
        return (s.length() * 2) + OBJ_OVERHEAD;
    }

}
