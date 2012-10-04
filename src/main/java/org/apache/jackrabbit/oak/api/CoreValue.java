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
package org.apache.jackrabbit.oak.api;

import java.io.InputStream;
import java.math.BigDecimal;

import javax.annotation.Nonnull;

/**
 * {@code CoreValue} is the internal representation of a {@link javax.jcr.Value
 * JCR value}. It is therefore isolated from session-specific namespace mappings
 * and relies on the internal representation of JCR names and paths.
 */
@Deprecated
public interface CoreValue extends Comparable<CoreValue> {

    /**
     * Returns the type of this value object which any of the following property
     * types defined by the JCR specification:
     *
     * <ul>
     *     <li>{@link javax.jcr.PropertyType#BINARY BINARY}</li>
     *     <li>{@link javax.jcr.PropertyType#BOOLEAN BOOLEAN}</li>
     *     <li>{@link javax.jcr.PropertyType#DATE DATE}</li>
     *     <li>{@link javax.jcr.PropertyType#DECIMAL DECIMAL}</li>
     *     <li>{@link javax.jcr.PropertyType#DOUBLE DOUBLE}</li>
     *     <li>{@link javax.jcr.PropertyType#LONG LONG}</li>
     *     <li>{@link javax.jcr.PropertyType#NAME NAME}</li>
     *     <li>{@link javax.jcr.PropertyType#PATH PATH}</li>
     *     <li>{@link javax.jcr.PropertyType#REFERENCE REFERENCE}</li>
     *     <li>{@link javax.jcr.PropertyType#STRING STRING}</li>
     *     <li>{@link javax.jcr.PropertyType#URI URI}</li>
     *     <li>{@link javax.jcr.PropertyType#WEAKREFERENCE WEAKREFERENCE}</li>
     * </ul>
     *
     * @return The type of this value instance. The return value is any of
     * the types defined by {@link javax.jcr.PropertyType} except for
     * {@link javax.jcr.PropertyType#UNDEFINED UNDEFINED}.
     */
    int getType();

    /**
     * Returns a {@code String} representation of this value. Note that the
     * string reflects the internal state and doesn't respect any session level
     * namespace remapping.
     *
     * @return The string representation of this value.
     */
    @Nonnull
    String getString();

    /**
     * Returns a {@code long} representation of this value.
     *
     * @return A {@code long} representation of this value based on an internal
     * conversion.
     * @throws NumberFormatException If the conversion fails.
     */
    long getLong();

    /**
     * Returns a {@code double} representation of this value.
     *
     * @return A {@code double} representation of this value based on an internal
     * conversion.
     * @throws NumberFormatException If the conversion fails.
     */
    double getDouble();

    /**
     * Returns a {@code boolean} representation of this value.
     *
     * @return A {@code boolean} representation of this value based on an internal
     * conversion.
     * @throws {@code UnsupportedOperationException} If the value cannot be
     * converted a {@code boolean}.
     */
    boolean getBoolean();

    /**
     * Returns a {@code BigDecimal} representation of this value.
     *
     * @return A {@code BigDecimal} representation of this value based on an
     * internal conversion.
     * @throws {@code NumberFormatException} If the value cannot be converted
     * a {@code BigDecimal}.
     */
    @Nonnull
    BigDecimal getDecimal();

    /**
     * Returns a new stream for this value object.
     *
     * @return a new stream for this value based on an internal conversion.
     * @throws //TODO define exceptions
     */
    @Nonnull
    InputStream getNewStream();

    /**
     * Returns the length of this value.
     *
     * @return the length of this value.
     */
    long length();
}
