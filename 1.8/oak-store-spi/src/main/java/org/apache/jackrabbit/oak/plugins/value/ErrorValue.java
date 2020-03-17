/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.jackrabbit.oak.plugins.value;

import java.io.InputStream;
import java.math.BigDecimal;
import java.util.Calendar;

import javax.jcr.Binary;
import javax.jcr.PropertyType;
import javax.jcr.RepositoryException;
import javax.jcr.Value;

/**
 * Instances of this class represent a {@code Value} which couldn't be retrieved.
 * All its accessors throw a {@code RepositoryException}.
 */
public class ErrorValue implements Value {
    private final Exception exception;
    private final int type;

    public ErrorValue(Exception exception, int type) {
        this.exception = exception;
        this.type = type;
    }

    public ErrorValue(RepositoryException e) {
        this(e, PropertyType.UNDEFINED);
    }

    @Override
    public String getString() throws RepositoryException {
        throw createException();
    }

    @Override
    public InputStream getStream() throws RepositoryException {
        throw createException();
    }

    @Override
    public Binary getBinary() throws RepositoryException {
        throw createException();
    }

    @Override
    public long getLong() throws RepositoryException {
        throw createException();
    }

    @Override
    public double getDouble() throws RepositoryException {
        throw createException();
    }

    @Override
    public BigDecimal getDecimal() throws RepositoryException {
        throw createException();
    }

    @Override
    public Calendar getDate() throws RepositoryException {
        throw createException();
    }

    @Override
    public boolean getBoolean() throws RepositoryException {
        throw createException();
    }

    @Override
    public int getType() {
        return type;
    }

    private RepositoryException createException() {
        return new RepositoryException("Inaccessible value", exception);
    }

    @Override
    public String toString() {
        return "Inaccessible value: " + exception.getMessage();
    }
}
