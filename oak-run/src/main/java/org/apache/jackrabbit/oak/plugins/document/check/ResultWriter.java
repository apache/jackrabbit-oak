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
package org.apache.jackrabbit.oak.plugins.document.check;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.function.Consumer;

import static org.apache.jackrabbit.oak.plugins.document.check.Result.END;

/**
 * <code>ResultWriter</code>...
 */
public final class ResultWriter implements Callable<Void> {

    private final BlockingQueue<Result> results;

    private final Consumer<String> resultConsumer;

    public ResultWriter(BlockingQueue<Result> results,
                        Consumer<String> resultConsumer) {
        this.results = results;
        this.resultConsumer = resultConsumer;
    }

    @Override
    public Void call() throws Exception {
        Result r;
        while ((r = results.take()) != END) {
            resultConsumer.accept(r.toJson());
        }
        return null;
    }
}
