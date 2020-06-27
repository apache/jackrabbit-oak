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
package org.apache.jackrabbit.oak.plugins.index.lucene;

import org.apache.jackrabbit.oak.Oak;
import org.apache.jackrabbit.oak.jcr.Jcr;
import org.apache.jackrabbit.oak.plugins.index.LuceneIndexOptions;
import org.apache.jackrabbit.oak.plugins.index.SpellcheckTest1;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jcr.Repository;
import javax.jcr.RepositoryException;
import java.io.File;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

//TODO Spellckeck tests are failing for lucene index
@Ignore
public class LuceneSpellcheckTest1 extends SpellcheckTest1 {

    private static final Logger LOG = LoggerFactory.getLogger(LuceneSpellcheckTest1.class);

    private ExecutorService executorService = Executors.newFixedThreadPool(2);
    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder(new File("target"));

    protected Repository createJcrRepository() throws RepositoryException {
        indexOptions = new LuceneIndexOptions();
        repositoryOptionsUtil = new LuceneTestRepositoryBuilder(executorService, temporaryFolder).build();
        Oak oak = repositoryOptionsUtil.getOak();
        Jcr jcr = new Jcr(oak);
        Repository repository = jcr.createRepository();
        return repository;
    }
}
