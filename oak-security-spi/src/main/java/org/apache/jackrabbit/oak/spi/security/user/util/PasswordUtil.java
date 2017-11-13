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
package org.apache.jackrabbit.oak.spi.security.user.util;

import java.io.UnsupportedEncodingException;
import java.security.Key;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.KeySpec;
import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.PBEKeySpec;

import org.apache.jackrabbit.oak.spi.security.ConfigurationParameters;
import org.apache.jackrabbit.oak.spi.security.user.UserConstants;
import org.apache.jackrabbit.util.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Utility to generate and compare password hashes.
 */
public final class PasswordUtil {

    private static final Logger log = LoggerFactory.getLogger(PasswordUtil.class);

    private static final char DELIMITER = '-';
    private static final int NO_ITERATIONS = 1;
    private static final String ENCODING = "UTF-8";

    /**
     * @since OAK 1.0
     */
    private static final String PBKDF2_PREFIX = "PBKDF2";
    
    public static final String DEFAULT_ALGORITHM = "SHA-256";
    public static final int DEFAULT_SALT_SIZE = 8;
    public static final int DEFAULT_ITERATIONS = 1000;

    /**
     * Avoid instantiation
     */
    private PasswordUtil() {}

    /**
     * Generates a hash of the specified password with the default values
     * for algorithm, salt-size and number of iterations.
     *
     * @param password The password to be hashed.
     * @return The password hash.
     * @throws NoSuchAlgorithmException If {@link #DEFAULT_ALGORITHM} is not supported.
     * @throws UnsupportedEncodingException If utf-8 is not supported.
     */
    public static String buildPasswordHash(@Nonnull String password) throws NoSuchAlgorithmException, UnsupportedEncodingException {
        return buildPasswordHash(password, DEFAULT_ALGORITHM, DEFAULT_SALT_SIZE, DEFAULT_ITERATIONS);
    }

    /**
     * Generates a hash of the specified password using the specified algorithm,
     * salt size and number of iterations into account.
     *
     * @param password The password to be hashed.
     * @param algorithm The desired hash algorithm. If the algorith is
     * {@code null} the {@link #DEFAULT_ALGORITHM} will be used.
     * @param saltSize The desired salt size. If the specified integer is lower
     * that {@link #DEFAULT_SALT_SIZE} the default is used.
     * @param iterations The desired number of iterations. If the specified
     * integer is lower than 1 the {@link #DEFAULT_ITERATIONS default} value is used.
     * @return  The password hash.
     * @throws NoSuchAlgorithmException If the specified algorithm is not supported.
     * @throws UnsupportedEncodingException If utf-8 is not supported.
     */
    public static String buildPasswordHash(@Nonnull String password,
                                           @Nullable String algorithm,
                                           int saltSize, int iterations) throws NoSuchAlgorithmException, UnsupportedEncodingException {
        checkNotNull(password);
        if (iterations < NO_ITERATIONS) {
            iterations = DEFAULT_ITERATIONS;
        }
        if (saltSize < DEFAULT_SALT_SIZE) {
            saltSize = DEFAULT_SALT_SIZE;
        }
        String salt = generateSalt(saltSize);
        String alg = (algorithm == null) ? DEFAULT_ALGORITHM : algorithm;
        return generateHash(password, alg, salt, iterations);
    }

    /**
     * Same as {@link #buildPasswordHash(String, String, int, int)} but retrieving
     * the parameters for hash generation from the specified configuration.
     *
     * @param password The password to be hashed.
     * @param config The configuration defining the details of the hash generation.
     * @return The password hash.
     * @throws NoSuchAlgorithmException If the specified algorithm is not supported.
     * @throws UnsupportedEncodingException If utf-8 is not supported.
     */
    public static String buildPasswordHash(@Nonnull String password,
                                           @Nonnull ConfigurationParameters config) throws NoSuchAlgorithmException, UnsupportedEncodingException {
        checkNotNull(config);
        String algorithm = config.getConfigValue(UserConstants.PARAM_PASSWORD_HASH_ALGORITHM, DEFAULT_ALGORITHM);
        int iterations = config.getConfigValue(UserConstants.PARAM_PASSWORD_HASH_ITERATIONS, DEFAULT_ITERATIONS);
        int saltSize = config.getConfigValue(UserConstants.PARAM_PASSWORD_SALT_SIZE, DEFAULT_SALT_SIZE);

        return buildPasswordHash(password, algorithm, saltSize, iterations);
    }

    /**
     * Returns {@code true} if the specified string doesn't start with a
     * valid algorithm name in curly brackets.
     *
     * @param password The string to be tested.
     * @return {@code true} if the specified string doesn't start with a
     * valid algorithm name in curly brackets.
     */
    public static boolean isPlainTextPassword(@Nullable String password) {
        return extractAlgorithm(password) == null;
    }

    /**
     * Returns {@code true} if hash of the specified {@code password} equals the
     * given hashed password.
     *
     * @param hashedPassword Password hash.
     * @param password The password to compare.
     * @return If the hash created from the specified {@code password} equals
     * the given {@code hashedPassword} string.
     */
    public static boolean isSame(@Nullable String hashedPassword, @Nonnull char[] password) {
        return isSame(hashedPassword, String.valueOf(password));
    }

    /**
     * Returns {@code true} if hash of the specified {@code password} equals the
     * given hashed password.
     *
     * @param hashedPassword Password hash.
     * @param password The password to compare.
     * @return If the hash created from the specified {@code password} equals
     * the given {@code hashedPassword} string.
     */
    public static boolean isSame(@Nullable String hashedPassword, @Nonnull String password) {
        if (hashedPassword == null) {
            return false;
        }
        try {
            String algorithm = extractAlgorithm(hashedPassword);
            if (algorithm != null) {
                int startPos = algorithm.length()+2;
                String salt = extractSalt(hashedPassword, startPos);
                int iterations = NO_ITERATIONS;
                if (salt != null) {
                    startPos += salt.length()+1;
                    iterations = extractIterations(hashedPassword, startPos);
                }

                String hash = generateHash(password, algorithm, salt, iterations);
                return compareSecure(hashedPassword, hash);
            } // hashedPassword is plaintext -> return false
        } catch (NoSuchAlgorithmException | UnsupportedEncodingException e) {
            log.warn(e.getMessage());
        }
        return false;
    }
    
    //------------------------------------------------------------< private >---
    /**
     * Compare two strings. The comparison is constant time: it will always loop
     * over all characters and doesn't use conditional operations in the loop to
     * make sure an attacker can not use a timing attack.
     *
     * @param a
     * @param b
     * @return true if both parameters contain the same data.
     */
    private static boolean compareSecure(@Nonnull String a, @Nonnull String b) {
        int len = a.length();
        if (len != b.length()) {
            return false;
        }
        if (len == 0) {
            return true;
        }
        // don't use conditional operations inside the loop
        int bits = 0;
        for (int i = 0; i < len; i++) {
            // this will never reset any bits
            bits |= a.charAt(i) ^ b.charAt(i);
        }
        return bits == 0;
    }

    @Nonnull
    private static String generateHash(@Nonnull String pwd, @Nonnull String algorithm,
                                       @Nullable String salt, int iterations) throws NoSuchAlgorithmException, UnsupportedEncodingException {
        StringBuilder passwordHash = new StringBuilder();
        passwordHash.append('{').append(algorithm).append('}');
        if (salt != null && !salt.isEmpty()) {
            StringBuilder data = new StringBuilder();
            data.append(salt).append(pwd);

            passwordHash.append(salt).append(DELIMITER);
            if (iterations > NO_ITERATIONS) {
                passwordHash.append(iterations).append(DELIMITER);
            }
            String digest;
            if (algorithm.startsWith(PBKDF2_PREFIX)) {
                digest = generatePBKDF2(pwd, salt, algorithm, iterations, 128);
            } else {
                digest = generateDigest(data.toString(), algorithm, iterations);
            }
            passwordHash.append(digest);
        } else {
            // backwards compatible to jr 2.0: no salt, no iterations
            passwordHash.append(Text.digest(algorithm, pwd.getBytes(ENCODING)));
        }
        return passwordHash.toString();
    }

    @Nonnull
    private static String generateSalt(int saltSize) {
        SecureRandom random = new SecureRandom();
        byte[] salt = new byte[saltSize];
        random.nextBytes(salt);

        return convertBytesToHex(salt);
    }
    
    /**
     * Convert a byte array to a hex encoded string.
     *
     * @param bytes the byte array
     * @return the hex encoded string
     */
    @Nonnull
    private static String convertBytesToHex(byte[] bytes) {
        StringBuilder res = new StringBuilder(bytes.length * 2);
        for (byte b : bytes) {
            res.append(Text.hexTable[(b >> 4) & 15]);
            res.append(Text.hexTable[b & 15]);
        }
        return res.toString();
    }
    
    /**
     * Convert a hex encoded string to a byte array.
     *
     * @param s the hex encoded string
     * @return the byte array
     */
    @Nonnull
    private static byte[] convertHexToBytes(String s) {
        int len = s.length();
        if (len % 2 != 0) {
            throw new IllegalArgumentException("Not a hex encoded byte array: " + s);
        }
        byte[] bytes = new byte[len / 2];
        for (int i = 0; i < bytes.length; i++) {
            bytes[i] = (byte) (
                    (Character.digit(s.charAt(i + i), 16) << 4) + 
                    Character.digit(s.charAt(i + i + 1), 16));
        }
        return bytes;
    }

    @Nonnull
    private static String generatePBKDF2(@Nonnull String pwd, @Nonnull String salt,
                                         @Nonnull String algorithm, int iterations, int keyLength) throws NoSuchAlgorithmException {
        // for example PBKDF2WithHmacSHA1
        SecretKeyFactory factory = SecretKeyFactory.getInstance(algorithm);
        byte[] saltBytes = convertHexToBytes(salt);
        KeySpec keyspec = new PBEKeySpec(pwd.toCharArray(), saltBytes, iterations, keyLength);
        try {
            Key key = factory.generateSecret(keyspec);
            byte[] bytes = key.getEncoded();
            return convertBytesToHex(bytes);
        } catch (InvalidKeySpecException e) {
            throw new NoSuchAlgorithmException(algorithm, e);
        }  
    }

    @Nonnull
    private static String generateDigest(@Nonnull String data, @Nonnull String algorithm, int iterations) throws UnsupportedEncodingException, NoSuchAlgorithmException {
        byte[] bytes = data.getBytes(ENCODING);
        MessageDigest md = MessageDigest.getInstance(algorithm);

        for (int i = 0; i < iterations; i++) {
            md.reset();
            bytes = md.digest(bytes);
        }

        return convertBytesToHex(bytes);
    }

    /**
     * Extract the algorithm from the given crypted password string. Returns the
     * algorithm or {@code null} if the given string doesn't have a
     * leading {@code algorithm} such as created by {@code buildPasswordHash}
     * or if the extracted string doesn't represent an available algorithm.
     *
     * @param hashedPwd The password hash.
     * @return The algorithm or {@code null} if the given string doesn't have a
     * leading {@code algorithm} such as created by {@code buildPasswordHash}
     * or if the extracted string isn't a supported algorithm.
     */
    @CheckForNull
    private static String extractAlgorithm(@Nullable String hashedPwd) {
        if (hashedPwd != null && !hashedPwd.isEmpty()) {
            int end = hashedPwd.indexOf('}');
            if (hashedPwd.charAt(0) == '{' && end > 0 && end < hashedPwd.length()-1) {
                String algorithm = hashedPwd.substring(1, end);
                try {
                    MessageDigest.getInstance(algorithm);
                    return algorithm;
                } catch (NoSuchAlgorithmException e) {
                    log.debug("Invalid algorithm detected " + algorithm, e);
                }
            }
        }

        // not starting with {} or invalid algorithm
        return null;
    }

    @CheckForNull
    private static String extractSalt(@Nullable String hashedPwd, int start) {
        if (hashedPwd != null) {
            int end = hashedPwd.indexOf(DELIMITER, start);
            if (end > -1) {
                return hashedPwd.substring(start, end);
            }
        }
        // no salt
        return null;
    }

    private static int extractIterations(@Nullable String hashedPwd, int start) {
        if (hashedPwd != null) {
            int end = hashedPwd.indexOf(DELIMITER, start);
            if (end > -1) {
                String str = hashedPwd.substring(start, end);
                try {
                    return Integer.parseInt(str);
                } catch (NumberFormatException e) {
                    log.debug("Expected number of iterations. Found: " + str, e);
                }
            }
        }

        // no extra iterations
        return NO_ITERATIONS;
    }
}