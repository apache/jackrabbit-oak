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

import javax.annotation.Nonnull;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;

/**
 * {@code CoreValueFactory} defines methods to create new instances of
 * {@code CoreValue}.
 */
public interface CoreValueFactory {

    /**
     * Creates a new value of type {@link javax.jcr.PropertyType#STRING}.
     *
     * @param value A non-null {@code String} defining the new value.
     * @return a new value instance.
     * @throws IllegalArgumentException if the specified {@code String}
     * is {@code null}.
     */
    @Nonnull
    CoreValue createValue(String value);

    /**
     * Creates a new value of type {@link javax.jcr.PropertyType#DOUBLE}.
     *
     * @param value The {@code double} that defines the new value.
     * @return a new value instance.
     */
    @Nonnull
    CoreValue createValue(double value);

    /**
     * Creates a new value of type {@link javax.jcr.PropertyType#DOUBLE}.
     *
     * @param value The {@code double} that defines the new value.
     * @return a new value instance.
     */
    @Nonnull
    CoreValue createValue(long value);

    /**
     * Creates a new value of type {@link javax.jcr.PropertyType#BOOLEAN}.
     *
     * @param value The {@code boolean} that defines the new value.
     * @return a new value instance.
     */
    @Nonnull
    CoreValue createValue(boolean value);

    /**
     * Creates a new value of type {@link javax.jcr.PropertyType#DECIMAL}.
     *
     * @param value A non-null {@code BigDecimal} that defines the new value.
     * @return a new value instance.
     * @throws IllegalArgumentException if the specified {@code BigDecimal} is {@code null}.
     */
    @Nonnull
    CoreValue createValue(BigDecimal value);

    /**
     * Creates a new value of type {@link javax.jcr.PropertyType#BINARY}.
     *
     * @param value A non-null {@code InputStream} that defines the new value.
     * @return a new value instance.
     * @throws IllegalArgumentException if the specified {@code InputStream} is {@code null}.
     * @throws IOException If an error occurs while processing the stream.
     * @throws //TODO define exceptions (currently impl. throws MicrokernelException)
     */
    @Nonnull
    CoreValue createValue(InputStream value) throws IOException;

    /**
     * Creates a new value of the specified type.
     *
     * @param value A non-null {@code String} that defines the new value.
     * @param type The desired target type of the new value.
     * @return a new value instance.
     * @throws IllegalArgumentException if the specified {@code value} is {@code null}
     * or if the given type is not supported.
     * @throws NumberFormatException If the specified {@code type} requires
     * conversion to any of the number types and the conversion fails.
     * @throws //TODO define and consolidate exceptions
     */
    @Nonnull
    CoreValue createValue(@Nonnull String value, int type);
}