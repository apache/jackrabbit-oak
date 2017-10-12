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

import java.util.List;
import javax.annotation.Nonnull;
import javax.jcr.RepositoryException;
import javax.jcr.nodetype.ConstraintViolationException;

import org.apache.jackrabbit.oak.api.Tree;

/**
 * {@code ProtectedNodeImporter} provides means to import protected
 * {@code Node}s and the subtree defined below such nodes.
 * <p>
 * The import of a protected tree is started by the {@code Importer} by
 * calling {@link #start(Tree)}. If the {@code ProtectedNodeImporter}
 * is able to deal with that type of protected node, it is in charge of dealing
 * with all subsequent child {@code NodeInfo}s present below the protected
 * parent until {@link #end(Tree)} is called. The latter resets this importer
 * and makes it available for another protected import.
 */
public interface ProtectedNodeImporter extends ProtectedItemImporter {


    /**
     * Notifies this importer about the existence of a protected node that
     * has either been created (NEW) or has been found to be existing.
     * This importer implementation is in charge of evaluating the nature of
     * that protected node in order to determine, if it is able to handle
     * subsequent protected or non-protected child nodes in the tree below
     * that parent.
     *
     * @param protectedParent A protected node that has either been created
     * during the current XML import or that has been found to be existing
     * without allowing same-name siblings.
     * @return {@code true} If this importer is able to deal with the
     * tree that may be present below the given protected Node.
     * @throws IllegalStateException If this method is called on
     * this importer without having reached {@link #end(Tree)}.
     * @throws javax.jcr.RepositoryException If an error occurs.
     */
    boolean start(@Nonnull Tree protectedParent) throws IllegalStateException,
            RepositoryException;

    /**
     * Informs this importer that the tree to be imported below
     * {@code protectedParent} has bee completed. This allows the importer
     * to be reset in order to be able to deal with another call to
     * {@link #start(Tree)}.<p>
     * If {@link #start(Tree)} hasn't been called before, this method returns
     * silently.
     *
     * @param protectedParent The protected parent tree.
     * @throws IllegalStateException If end is called in an illegal state.
     * @throws javax.jcr.nodetype.ConstraintViolationException If the tree
     * that was imported is incomplete.
     * @throws RepositoryException If another error occurs.
     */
    void end(@Nonnull Tree protectedParent) throws IllegalStateException,
            ConstraintViolationException, RepositoryException;

    /**
     * Informs this importer about a new {@code childInfo} and it's properties.
     * If the importer is able to successfully import the given information
     * this method returns silently. Otherwise
     * {@code ConstraintViolationException} is thrown, in which case the
     * whole import fails.<p>
     * In case this importer deals with multiple levels of nodes, it is in
     * charge of maintaining the hierarchical structure (see also {#link endChildInfo()}.
     * <p>
     * If {@link #start(Tree)} hasn't been called before, this method returns
     * silently.
     *
     * @param childInfo
     * @param propInfos
     * @throws IllegalStateException If called in an illegal state.
     * @throws javax.jcr.nodetype.ConstraintViolationException If the given
     * infos contain invalid or incomplete data and therefore cannot be properly
     * handled by this importer.
     * @throws RepositoryException If another error occurs.
     */
    void startChildInfo(@Nonnull NodeInfo childInfo, @Nonnull List<PropInfo> propInfos)
            throws IllegalStateException, ConstraintViolationException, RepositoryException;

    /**
     * Informs this importer about the end of a child info.
     * <p>
     * If {@link #start(Tree)} hasn't been called before, this method returns
     * silently.
     *
     * @throws IllegalStateException If end is called in an illegal state.
     * @throws javax.jcr.nodetype.ConstraintViolationException If this method
     * is called before all required child information has been imported.
     * @throws RepositoryException If another error occurs.
     */
    void endChildInfo() throws RepositoryException;
}