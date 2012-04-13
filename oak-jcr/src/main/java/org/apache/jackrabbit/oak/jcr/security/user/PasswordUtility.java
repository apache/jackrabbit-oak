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
package org.apache.jackrabbit.oak.jcr.security.user;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.UnsupportedEncodingException;
import java.security.NoSuchAlgorithmException;

/**
 * PasswordUtility...
 */
public class PasswordUtility {

    /**
     * logger instance
     */
    private static final Logger log = LoggerFactory.getLogger(PasswordUtility.class);

    public static final String DEFAULT_ALGORITHM = "SHA-256";
    public static final int DEFAULT_SALT_SIZE = 8;
    public static final int DEFAULT_ITERATIONS = 10000;

    /**
     * Avoid instantiation
     */
    private PasswordUtility() {}

    public static boolean isSame(String passwordHash, String toTest) {
        // TODO
        return false;
    }

    public static String buildPasswordHash(String password, String algorithm,
                                           int defaultSaltSize, int iterations)
            throws NoSuchAlgorithmException, UnsupportedEncodingException {
        // TODO
        return null;
    }

    public static boolean isPlainTextPassword(String password) {
        // TODO
        return false;
    }
}