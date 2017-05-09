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

package org.apache.jackrabbit.oak.segment.tool;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.jackrabbit.oak.segment.tool.Utils.readRevisions;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.List;

/**
 * Collect and print the revisions of a segment store.
 */
public class Revisions implements Runnable {

    /**
     * Create a builder for the {@link Revisions} command.
     *
     * @return an instance of {@link Builder}.
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Collect options for the {@link Revisions} command.
     */
    public static class Builder {

        private File path;

        private File out;

        private Builder() {
            // Prevent external instantiation.
        }

        /**
         * The path to an existing segment store. This parameter is required.
         *
         * @param path the path to an existing segment store.
         * @return this builder.
         */
        public Builder withPath(File path) {
            this.path = checkNotNull(path);
            return this;
        }

        /**
         * The file where the output of this command is stored. this parameter
         * is mandatory.
         *
         * @param out the output file.
         * @return this builder.
         */
        public Builder withOutput(File out) {
            this.out = checkNotNull(out);
            return this;
        }

        /**
         * Create an executable version of the {@link Revisions} command.
         *
         * @return an instance of {@link Runnable}.
         */
        public Runnable build() {
            checkNotNull(path);
            checkNotNull(out);
            return new Revisions(this);
        }

    }

    private final File path;

    private final File out;

    private Revisions(Builder builder) {
        this.path = builder.path;
        this.out = builder.out;
    }

    @Override
    public void run() {
        try {
            listRevisions();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void listRevisions() throws IOException {
        System.out.println("Store " + path);
        System.out.println("Writing revisions to " + out);

        List<String> revs = readRevisions(path);

        if (revs.isEmpty()) {
            System.out.println("No revisions found.");
            return;
        }

        try (PrintWriter pw = new PrintWriter(out)) {
            for (String r : revs) {
                pw.println(r);
            }
        }
    }

}
