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
package org.apache.jackrabbit.oak.scalability.benchmarks.search;

import org.apache.jackrabbit.oak.scalability.suites.ScalabilityAbstractSuite;

import javax.jcr.Credentials;
import javax.jcr.Repository;
import java.util.UUID;

/**
 * Writes random paths concurrently with multiple readers/writers configured with {#WRITERS} and {#READERS}.
 */
public class ConcurrentWriter extends ConcurrentReader {
    @Override
    public void execute(Repository repository, Credentials credentials, ScalabilityAbstractSuite.ExecutionContext context)
        throws Exception {
        Writer writer = new Writer(this.getClass().getSimpleName() + UUID.randomUUID(),
            100, repository.login(credentials), context);
        writer.process();
    }
}
