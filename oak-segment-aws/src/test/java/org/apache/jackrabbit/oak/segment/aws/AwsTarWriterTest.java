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
package org.apache.jackrabbit.oak.segment.aws;

import java.io.IOException;
import java.util.Date;

import com.amazonaws.services.s3.AmazonS3;

import org.apache.jackrabbit.oak.segment.file.tar.TarWriterTest;
import org.apache.jackrabbit.oak.segment.spi.monitor.IOMonitorAdapter;
import org.apache.jackrabbit.oak.segment.spi.persistence.SegmentArchiveManager;
import org.apache.jackrabbit.oak.segment.spi.persistence.SegmentArchiveWriter;
import org.junit.Before;
import org.junit.ClassRule;

public class AwsTarWriterTest extends TarWriterTest {

    @ClassRule
    public static final S3MockRule s3Mock = new S3MockRule();

    private S3Directory directory;

    @Before
    public void setUp() throws IOException {
        AmazonS3 s3 = s3Mock.createClient();
        long time = new Date().getTime();
        directory = new S3Directory(s3, "bucket-" + time, "oak");
        directory.ensureBucket();
    }

    @Override
    protected SegmentArchiveManager getSegmentArchiveManager() {
        return new AwsArchiveManager(directory, new IOMonitorAdapter(), monitor);
    }

    @Override
    protected SegmentArchiveManager getFailingSegmentArchiveManager() {
        IOMonitorAdapter ioMonitor = new IOMonitorAdapter();
        return new AwsArchiveManager(directory, ioMonitor, monitor) {
            @Override
            public SegmentArchiveWriter create(String archiveName) {
                return new AwsSegmentArchiveWriter(directory.withDirectory(archiveName), archiveName, ioMonitor, monitor) {
                    @Override
                    public void writeGraph(byte[] data) throws IOException {
                        throw new IOException("test");
                    }
                };
            }
        };
    }
}
