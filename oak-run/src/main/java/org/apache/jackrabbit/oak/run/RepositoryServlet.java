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
package org.apache.jackrabbit.oak.run;

import org.apache.jackrabbit.mk.api.MicroKernel;
import org.apache.jackrabbit.mk.core.MicroKernelImpl;
import org.apache.jackrabbit.oak.jcr.RepositoryImpl;
import org.apache.jackrabbit.webdav.jcr.JCRWebdavServerServlet;

import javax.jcr.Repository;
import javax.jcr.RepositoryException;
import javax.servlet.ServletException;

class RepositoryServlet extends JCRWebdavServerServlet {

    private final String path;

    private MicroKernel kernel;

    private Repository repository;

    public RepositoryServlet(String path) {
        this.path = path;
    }

    @Override
    public void init() throws ServletException {
        if (path != null) {
            kernel = new MicroKernelImpl(path);
        } else {
            kernel = new MicroKernelImpl();
        }

        try {
            repository = new RepositoryImpl();
        } catch (RepositoryException e) {
            throw new ServletException("Could not start a repository", e);
        }
        super.init();
    }

    @Override
    protected Repository getRepository() {
        return repository;
    }

    @Override
    public void destroy() {
        super.destroy();

        kernel.dispose();
    }

}