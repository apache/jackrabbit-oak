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
package org.apache.jackrabbit.oak.plugins.nodetype.write;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.jcr.nodetype.ConstraintViolationException;

import org.apache.jackrabbit.oak.namepath.JcrNameParser;
import org.apache.jackrabbit.oak.namepath.NameMapper;

/**
 * Abstract base class for the template implementations in this package.
 * Keeps track of the Oak name of this template and provides utility methods
 * for mapping between JCR and Oak names.
 */
abstract class NamedTemplate {

    private final NameMapper mapper;

    private String oakName = null; // not defined by default

    protected NamedTemplate(@Nonnull NameMapper mapper) {
        this.mapper = mapper;
    }

    protected NamedTemplate(@Nonnull NameMapper mapper, @Nullable String jcrName)
            throws ConstraintViolationException {
        this(mapper);
        if (jcrName != null) {
            setName(jcrName);
        }
    }

    /**
     * Returns the Oak name of this template, or {@code null} if the name
     * has not yet been set.
     *
     * @return Oak name, or {@code null}
     */
    @CheckForNull
    protected String getOakName() {
        return oakName;
    }

    //------------------------------------------------------------< public >--

    /**
     * Returns the JCR name of this template, or {@code null} if the name
     * has not yet been set.
     *
     * @return JCR name, or {@code null}
     */
    @CheckForNull
    public String getName() {
        return getJcrNameAllowNull(oakName);
    }

    /**
     * Sets the name of this template.
     *
     * @param jcrName JCR name
     * @throws ConstraintViolationException if the name is invalid
     */
    public void setName(@Nonnull String jcrName)
            throws ConstraintViolationException {
        this.oakName = getOakNameOrThrowConstraintViolation(jcrName);
    }

    //-------------------------------------------< name handling utilities >--

    /**
     * Like {@link NameMapper#getJcrName(String)}, but allows the given Oak
     * name to be {@code null}, in which case the return value is also
     * {@code null}. Useful for the template implementations where
     * {@code null} values are used to indicate undefined attributes.
     *
     * @param oakName Oak name, or {@code null}
     * @return JCR name, or {@code null}
     */
    @CheckForNull
    protected String getJcrNameAllowNull(@CheckForNull String oakName) {
        if (oakName != null) {
            return mapper.getJcrName(oakName);
        } else {
            return null;
        }
    }

    /**
     * Converts the given Oak names to corresponding JCR names. If the given
     * array is {@code null} (signifying an undefined set of names), then the
     * return value is also {@code null}.
     *
     * @param oakNames Oak names, or {@code null}
     * @return JCR names, or {@code null}
     */
    @CheckForNull
    protected String[] getJcrNamesAllowNull(@CheckForNull String[] oakNames) {
        String[] jcrNames = null;
        if (oakNames != null) {
            jcrNames = new String[oakNames.length];
            for (int i = 0; i < oakNames.length; i++) {
                jcrNames[i] = mapper.getJcrName(oakNames[i]);
            }
        }
        return jcrNames;
    }

    /**
     * Converts the given JCR name to the corresponding Oak name. Throws
     * a {@link ConstraintViolationException} if the name is {@code null}
     * or otherwise invalid.
     *
     * @param jcrName JCR name
     * @return Oak name
     * @throws ConstraintViolationException if name is invalid or {@code null}
     */
    @Nonnull
    protected String getOakNameOrThrowConstraintViolation(@CheckForNull String jcrName)
            throws ConstraintViolationException {
        if (jcrName == null) {
            throw new ConstraintViolationException("Missing JCR name");
        }
        String oakName = mapper.getOakNameOrNull(jcrName);
        if (oakName == null || !JcrNameParser.validate(jcrName)) {
            throw new ConstraintViolationException(
                    "Invalid name: " + jcrName);
        }
        return oakName;
    }

    /**
     * Like {@link #getOakNameOrThrowConstraintViolation(String)} but allows
     * the given JCR name to be {@code null}, in which case the return value
     * is also {@code null}.
     *
     * @param jcrName JCR name, or {@code null}
     * @return Oak name, or {@code null}
     * @throws ConstraintViolationException if the name is invalid
     */
    @CheckForNull
    protected String getOakNameAllowNullOrThrowConstraintViolation(@CheckForNull String jcrName)
            throws ConstraintViolationException {
        if (jcrName == null) {
            return null;
        } else {
            return getOakNameOrThrowConstraintViolation(jcrName);
        }
    }

    /**
     * Converts the given JCR names to corresponding Oak names. Throws
     * a {@link ConstraintViolationException} if the given array is
     * {@code null} or one of the contained JCR names is {@code null}
     * or otherwise invalid.
     *
     * @param jcrNames JCR names
     * @return Oak names
     * @throws ConstraintViolationException if names are invalid or {@code null}
     */
    @Nonnull
    protected String[] getOakNamesOrThrowConstraintViolation(@CheckForNull String[] jcrNames)
            throws ConstraintViolationException {
        if (jcrNames != null) {
            String[] oakNames = new String[jcrNames.length];
            for (int i = 0; i < jcrNames.length; i++) {
                oakNames[i] = getOakNameOrThrowConstraintViolation(jcrNames[i]);
            }
            return oakNames;
        } else {
            throw new ConstraintViolationException("Missing JCR names");
        }
    }

}
