/*
 * COPIED FROM APACHE LUCENE 4.7.2
 *
 * Git URL: git@github.com:apache/lucene.git, tag: releases/lucene-solr/4.7.2, path: lucene/core/src/java
 *
 * (see https://issues.apache.org/jira/browse/OAK-10786 for details)
 */

package org.apache.lucene.store;

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

import java.io.IOException;
import java.util.zip.CRC32;
import java.util.zip.Checksum;

/** Reads bytes through to a primary IndexInput, computing
 *  checksum as it goes. Note that you cannot use seek().
 *
 * @lucene.internal
 */
public class ChecksumIndexInput extends IndexInput {
  IndexInput main;
  Checksum digest;

  public ChecksumIndexInput(IndexInput main) {
    super("ChecksumIndexInput(" + main + ")");
    this.main = main;
    digest = new CRC32();
  }

  @Override
  public byte readByte() throws IOException {
    final byte b = main.readByte();
    digest.update(b);
    return b;
  }

  @Override
  public void readBytes(byte[] b, int offset, int len)
    throws IOException {
    main.readBytes(b, offset, len);
    digest.update(b, offset, len);
  }

  
  public long getChecksum() {
    return digest.getValue();
  }

  @Override
  public void close() throws IOException {
    main.close();
  }

  @Override
  public long getFilePointer() {
    return main.getFilePointer();
  }

  @Override
  public void seek(long pos) {
    throw new UnsupportedOperationException();
  }

  @Override
  public long length() {
    return main.length();
  }
}
