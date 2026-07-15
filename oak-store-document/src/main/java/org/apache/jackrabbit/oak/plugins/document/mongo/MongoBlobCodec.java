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
package org.apache.jackrabbit.oak.plugins.document.mongo;

import org.bson.BsonReader;
import org.bson.BsonString;
import org.bson.BsonValue;
import org.bson.BsonWriter;
import org.bson.Document;
import org.bson.codecs.Codec;
import org.bson.codecs.CollectibleCodec;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.DocumentCodec;
import org.bson.codecs.EncoderContext;

class MongoBlobCodec implements CollectibleCodec<MongoBlob> {

    private final Codec<Document> documentCodec = new DocumentCodec();

    MongoBlobCodec() {
    }

    @Override
    public MongoBlob generateIdIfAbsentFromDocument(MongoBlob document) {
        if (!documentHasId(document)) {
            throw new IllegalStateException("MongoBlob must not have generated id");
        }
        return document;
    }

    @Override
    public boolean documentHasId(MongoBlob document) {
        return document.getId() != null;
    }

    @Override
    public BsonValue getDocumentId(MongoBlob document) {
        if (!documentHasId(document)) {
            throw new IllegalStateException("MongoBlob does not have an id");
        }
        return new BsonString(document.getId());
    }

    @Override
    public MongoBlob decode(BsonReader reader, DecoderContext decoderContext) {
        Document doc = documentCodec.decode(reader, decoderContext);
        return MongoBlob.fromDocument(doc);
    }

    @Override
    public void encode(BsonWriter writer,
                       MongoBlob value,
                       EncoderContext encoderContext) {
        Document doc = value.asDocument();
        documentCodec.encode(writer, doc, encoderContext);
    }

    @Override
    public Class<MongoBlob> getEncoderClass() {
        return MongoBlob.class;
    }
}
