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
package org.apache.jackrabbit.oak.fixture;

import java.io.File;

import javax.jcr.Repository;

import org.apache.commons.io.FileUtils;
import org.apache.jackrabbit.core.RepositoryImpl;
import org.apache.jackrabbit.core.config.RepositoryConfig;

public class JackrabbitRepositoryFixture implements RepositoryFixture {

    private RepositoryImpl[] cluster;

    @Override
    public boolean isAvailable(int n) {
        return n == 1;
    }

    @Override
    public Repository[] setUpCluster(int n) throws Exception {
        if (n == 1) {
            Repository[] cluster = new Repository[n];
            File directory = new File("jackrabbit-repository");
            RepositoryConfig config = RepositoryConfig.install(directory);
            this.cluster[0] = RepositoryImpl.create(config);
            cluster[0] = this.cluster[0];
            return cluster;
        } else {
            throw new UnsupportedOperationException("TODO");
        }
    }

    @Override
    public void syncRepositoryCluster(Repository... nodes) {
        throw new UnsupportedOperationException("TODO");
    }

    @Override
    public void tearDownCluster() {
        for (RepositoryImpl repository : cluster) {
            File directory = new File(repository.getConfig().getHomeDir());
            repository.shutdown();
            FileUtils.deleteQuietly(directory);
        }
    }

}
