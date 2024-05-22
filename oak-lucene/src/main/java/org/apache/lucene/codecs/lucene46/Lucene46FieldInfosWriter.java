/*
 * COPIED FROM APACHE LUCENE 4.7.2
 *
 * Git URL: git@github.com:apache/lucene.git, tag: releases/lucene-solr/4.7.2, path: lucene/core/src/java
 *
 * (see https://issues.apache.org/jira/browse/OAK-10786 for details)
 */

package org.apache.lucene.codecs.lucene46;

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

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.FieldInfosWriter;
import org.apache.lucene.index.FieldInfo.DocValuesType;
import org.apache.lucene.index.FieldInfo.IndexOptions;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.IOUtils;

/**
 * Lucene 4.6 FieldInfos writer.
 * 
 * @see Lucene46FieldInfosFormat
 * @lucene.experimental
 */
final class Lucene46FieldInfosWriter extends FieldInfosWriter {
  
  /** Sole constructor. */
  public Lucene46FieldInfosWriter() {
  }
  
  @Override
  public void write(Directory directory, String segmentName, String segmentSuffix, FieldInfos infos, IOContext context) throws IOException {
    final String fileName = IndexFileNames.segmentFileName(segmentName, segmentSuffix, Lucene46FieldInfosFormat.EXTENSION);
    IndexOutput output = directory.createOutput(fileName, context);
    boolean success = false;
    try {
      CodecUtil.writeHeader(output, Lucene46FieldInfosFormat.CODEC_NAME, Lucene46FieldInfosFormat.FORMAT_CURRENT);
      output.writeVInt(infos.size());
      for (FieldInfo fi : infos) {
        IndexOptions indexOptions = fi.getIndexOptions();
        byte bits = 0x0;
        if (fi.hasVectors()) bits |= Lucene46FieldInfosFormat.STORE_TERMVECTOR;
        if (fi.omitsNorms()) bits |= Lucene46FieldInfosFormat.OMIT_NORMS;
        if (fi.hasPayloads()) bits |= Lucene46FieldInfosFormat.STORE_PAYLOADS;
        if (fi.isIndexed()) {
          bits |= Lucene46FieldInfosFormat.IS_INDEXED;
          assert indexOptions.compareTo(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS) >= 0 || !fi.hasPayloads();
          if (indexOptions == IndexOptions.DOCS_ONLY) {
            bits |= Lucene46FieldInfosFormat.OMIT_TERM_FREQ_AND_POSITIONS;
          } else if (indexOptions == IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS) {
            bits |= Lucene46FieldInfosFormat.STORE_OFFSETS_IN_POSTINGS;
          } else if (indexOptions == IndexOptions.DOCS_AND_FREQS) {
            bits |= Lucene46FieldInfosFormat.OMIT_POSITIONS;
          }
        }
        output.writeString(fi.name);
        output.writeVInt(fi.number);
        output.writeByte(bits);

        // pack the DV types in one byte
        final byte dv = docValuesByte(fi.getDocValuesType());
        final byte nrm = docValuesByte(fi.getNormType());
        assert (dv & (~0xF)) == 0 && (nrm & (~0x0F)) == 0;
        byte val = (byte) (0xff & ((nrm << 4) | dv));
        output.writeByte(val);
        output.writeLong(fi.getDocValuesGen());
        output.writeStringStringMap(fi.attributes());
      }
      success = true;
    } finally {
      if (success) {
        output.close();
      } else {
        IOUtils.closeWhileHandlingException(output);
      }
    }
  }
  
  private static byte docValuesByte(DocValuesType type) {
    if (type == null) {
      return 0;
    } else if (type == DocValuesType.NUMERIC) {
      return 1;
    } else if (type == DocValuesType.BINARY) {
      return 2;
    } else if (type == DocValuesType.SORTED) {
      return 3;
    } else if (type == DocValuesType.SORTED_SET) {
      return 4;
    } else {
      throw new AssertionError();
    }
  }  
}
