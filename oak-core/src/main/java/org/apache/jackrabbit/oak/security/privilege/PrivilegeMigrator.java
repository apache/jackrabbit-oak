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
package org.apache.jackrabbit.oak.security.privilege;

import java.io.IOException;
import java.io.InputStream;
import javax.jcr.NamespaceRegistry;
import javax.jcr.RepositoryException;

import org.apache.jackrabbit.oak.api.ContentSession;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeDefinition;
import org.apache.jackrabbit.oak.util.TODO;

/**
 * PrivilegeMigrator is a utility to migrate custom privilege definitions from
 * a jackrabbit 2 project to oak.
 */
public class PrivilegeMigrator {

    private final ContentSession contentSession;

    public PrivilegeMigrator(ContentSession contentSession) {
        this.contentSession = contentSession;
    }

    /**
     *
     * @throws RepositoryException
     */
    public void migrateCustomPrivileges() throws RepositoryException {
        PrivilegeRegistry pr = new PrivilegeRegistry(contentSession);
        InputStream stream = null;
        // TODO: order custom privileges such that validation succeeds.
        // FIXME: user proper path to jr2 custom privileges stored in fs
        // jr2 used to be:
        // new FileSystemResource(fs, "/privileges/custom_privileges.xml").getInputStream()
        if (stream != null) {
            try {
                // TODO: should get a proper namespace registry from somewhere
                NamespaceRegistry nsRegistry =
                        TODO.dummyImplementation().returnValue(null);
                PrivilegeDefinition[] custom = PrivilegeDefinitionReader.readCustomDefinitons(stream, nsRegistry);

                for (PrivilegeDefinition def : custom) {
                    pr.registerDefinition(def.getName(), def.isAbstract(), def.getDeclaredAggregateNames());
                }
            } catch (IOException e) {
                throw new RepositoryException(e);
            } finally {
                try {
                    stream.close();
                } catch (IOException e) {
                    // ignore.
                }
            }
        }
    }
}