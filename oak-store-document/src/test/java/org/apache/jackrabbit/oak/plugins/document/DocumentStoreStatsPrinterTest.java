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
package org.apache.jackrabbit.oak.plugins.document;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.List;

import org.apache.felix.inventory.Format;
import org.junit.Rule;
import org.junit.Test;

import static java.util.Arrays.asList;
import static org.apache.commons.lang.StringUtils.split;
import static org.hamcrest.Matchers.hasItem;
import static org.junit.Assert.assertThat;

public class DocumentStoreStatsPrinterTest {

    @Rule
    public DocumentMKBuilderProvider builderProvider = new DocumentMKBuilderProvider();

    @Test
    public void printStats() {
        DocumentNodeStore ns = builderProvider.newBuilder().getNodeStore();
        DocumentStoreStatsPrinter printer = new DocumentStoreStatsPrinter();
        printer.nodeStore = ns;

        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter(sw);
        printer.print(pw, Format.TEXT, false);

        List<String> lines = asList(split(sw.toString(),"\r\n"));
        assertThat(lines, hasItem("type=memory"));
        assertThat(lines, hasItem("nodes=1"));
        assertThat(lines, hasItem("clusterNodes=1"));
    }
}
