/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.oak.jcr;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Map;
import javax.jcr.Repository;
import javax.jcr.RepositoryException;
import javax.jcr.RepositoryFactory;

public class OakRepositoryFactory implements RepositoryFactory {

    private static final String REPOSITORY_URI = "org.apache.jackrabbit.repository.uri";

    @SuppressWarnings({"rawtypes", "unchecked"})
    @Override
    public Repository getRepository(Map parameters) throws RepositoryException {
        Object value = parameters == null ? null : parameters.get(REPOSITORY_URI);
        if (value != null) {
            try {
                URI uri = new URI(value.toString());
                if (uri.getScheme().equalsIgnoreCase("jcr-oak")) {
                    return getRepository(uri, parameters);
                }
            } catch (URISyntaxException ignore) {
            }
        } else {
            return getRepository(null, null);
        }
        return null;
    }

    private static Repository getRepository(
            URI uri, Map<String, String> parameters)
            throws RepositoryException {
        // TODO correctly interpret uri
        return new Jcr().createRepository();
    }

}
