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
package org.apache.jackrabbit.oak.plugins.index;

import org.junit.Test;

import java.text.DecimalFormatSymbols;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class ConsoleIndexingReporterTest {

    private static final char DELIM = new DecimalFormatSymbols().getDecimalSeparator();

    @Test
    public void emptyReport() {
        String expected = "Indexes: \n" +
                "Date: <date>\n" +
                "OAK Version: SNAPSHOT\n" +
                "Configuration:\n" +
                "\n" +
                "Environment Variables:\n" +
                "\n" +
                "Information:\n" +
                "\n" +
                "Timings:\n" +
                "\n" +
                "Metrics:\n";

        ConsoleIndexingReporter consoleIndexingReporter = new ConsoleIndexingReporter();
        String report = consoleIndexingReporter.generateReport();
        report = replaceDate(report);
        assertEquals(expected, report);
    }

    @Test
    public void fullReport() {
        String expected = "Indexes: index1, index2\n" +
                "Date: <date>\n" +
                "OAK Version: SNAPSHOT\n" +
                "Configuration:\n" +
                "  config1: value1\n" +
                "  config2: 12\n" +
                "Environment Variables:\n" +
                "  ENV_VAR1: <value>\n" +
                "  ENV_VAR2: <value>\n" +
                "Information:\n" +
                "  A message\n" +
                "  Foo Bar\n" +
                "Timings:\n" +
                "  Mongo dump: 00:02:44\n" +
                "  Merge sort: 00:00:06\n" +
                "  Build FFS (Dump+Merge): 00:02:59\n" +
                "  Build Lucene Index: 00:01:14\n" +
                "  Merge node store: 00:00:00\n" +
                "  Total time: 00:04:15\n" +
                "Metrics:\n" +
                "  metric1: 1\n" +
                "  metric2: 123\n" +
                "  metric3: 123456 (120" + DELIM + "56 KiB)\n" +
                "  metric4: 123456789 (117" + DELIM +"74 MiB)\n" +
                "  metric5: 1234567890123456 (1" + DELIM + "10 PiB)";

        ConsoleIndexingReporter consoleIndexingReporter = new ConsoleIndexingReporter(List.of("ENV_VAR1", "ENV_VAR2"));

        consoleIndexingReporter.setIndexNames(List.of("index1", "index2"));

        // Should be printed in alphabetic order
        consoleIndexingReporter.addMetric("metric1", 1);
        consoleIndexingReporter.addMetricByteSize("metric2", 123);
        consoleIndexingReporter.addMetricByteSize("metric3", 123456);
        consoleIndexingReporter.addMetricByteSize("metric4", 123456789);
        consoleIndexingReporter.addMetricByteSize("metric5", 1234567890123456L);

        // Should be printed in alphabetic order
        consoleIndexingReporter.addConfig("config1", "value1");
        consoleIndexingReporter.addConfig("config2", 12);
        consoleIndexingReporter.addInformation("Foo Bar");
        consoleIndexingReporter.addInformation("A message");

        // These should be printed by the order they were added
        consoleIndexingReporter.addTiming("Mongo dump", "00:02:44");
        consoleIndexingReporter.addTiming("Merge sort", "00:00:06");
        consoleIndexingReporter.addTiming("Build FFS (Dump+Merge)", "00:02:59");
        consoleIndexingReporter.addTiming("Build Lucene Index", "00:01:14");
        consoleIndexingReporter.addTiming("Merge node store", "00:00:00");
        consoleIndexingReporter.addTiming("Total time", "00:04:15");

        String report = consoleIndexingReporter.generateReport();

        report = replaceDate(report);
        report = replaceVariable(report, "ENV_VAR1");
        report = replaceVariable(report, "ENV_VAR2");

        assertEquals(expected, report);
    }

    private String replaceDate(String report) {
        return report.replaceAll("Date: .*", "Date: <date>");
    }

    private String replaceVariable(String report, String varName) {
        return report.replaceAll(" {2}" + varName + ": .*", "  " + varName + ": <value>");
    }

}