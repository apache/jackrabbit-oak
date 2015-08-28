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
package org.apache.jackrabbit.oak.upgrade.cli.node;

import java.io.BufferedReader;
import java.io.Closeable;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

import javax.jcr.RepositoryException;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.jackrabbit.core.RepositoryContext;
import org.apache.jackrabbit.core.config.RepositoryConfig;

import com.google.common.io.Closer;

public class Crx2Factory {

    private final File repositoryDir;

    private final File repositoryFile;

    public Crx2Factory(String repositoryDir, String repositoryFile) {
        if (!isCrx2Repository(repositoryDir)) {
            throw new IllegalArgumentException("Repository directory not found: " + repositoryDir);
        }
        this.repositoryDir = new File(repositoryDir);
        this.repositoryFile = new File(repositoryFile);
        if (!this.repositoryFile.isFile()) {
            throw new IllegalArgumentException("Repository configuration not found: " + repositoryFile);
        }
    }

    public RepositoryContext create(Closer closer) throws IOException, RepositoryException {
        RepositoryContext source = RepositoryContext.create(RepositoryConfig.create(repositoryFile, repositoryDir));
        closer.register(asCloseable(source));
        return source;
    }

    public File getRepositoryDir() {
        return repositoryDir;
    }

    private Closeable asCloseable(final RepositoryContext context) {
        return new Closeable() {
            @Override
            public void close() throws IOException {
                context.getRepository().shutdown();
            }
        };
    }

    public static boolean isRepositoryXml(String path) {
        final File file = new File(path);
        if (file.isFile()) {
            BufferedReader reader = null;
            try {
                reader = new BufferedReader(new FileReader(file));
                String line;
                while ((line = reader.readLine()) != null) {
                    if (StringUtils.containsIgnoreCase(line, "<Repository>")) {
                        return true;
                    }
                }
            } catch (FileNotFoundException e) {
                return false;
            } catch (IOException e) {
                return false;
            } finally {
                IOUtils.closeQuietly(reader);
            }
        }
        return false;
    }

    public static boolean isCrx2Repository(String directory) {
        final File dir = new File(directory);
        if (!dir.isDirectory()) {
            return false;
        }
        final File systemId = new File(dir, "system.id");
        return systemId.exists();
    }

    @Override
    public String toString() {
        return String.format("CRX2[%s, %s]", repositoryDir, repositoryFile);
    }
}
