<!--
   Licensed to the Apache Software Foundation (ASF) under one or more
   contributor license agreements.  See the NOTICE file distributed with
   this work for additional information regarding copyright ownership.
   The ASF licenses this file to You under the Apache License, Version 2.0
   (the "License"); you may not use this file except in compliance with
   the License.  You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
  -->

# Direct Binary Access upload file process

The direct binary upload process is split into 3 phases:

1. **Initialize:** A remote client makes request to the Jackrabbit-based application to request an upload, which calls `initiateBinaryUpload(long, int)` and returns the resulting information to the remote client.
2. **Upload:** The remote client performs the actual binary upload directly to the binary storage provider. The BinaryUpload returned from the previous call to `initiateBinaryUpload(long, int)` contains detailed instructions on how to complete the upload successfully. For more information, see the `BinaryUpload` documentation.
3. **Complete:** The remote client notifies the Jackrabbit-based application that step 2 is complete. The upload token returned in the first step (obtained by calling `BinaryUpload.getUploadToken()`) is passed by the client to `completeBinaryUpload(String)`. This will provide the application with a regular JCR Binary that can then be used to write JCR content including the binary (such as an `nt:file` structure) and persist it.

Example A: Here’s how to generate a pre-signed PUT URL using SSE-KMS:
```
String myKmsCmkId = ...;
GeneratePresignedUrlRequest genreq = new GeneratePresignedUrlRequest(
    myExistingBucket, myKey, HttpMethod.PUT)
    .withSSEAlgorithm(SSEAlgorithm.KMS.getAlgorithm())
    // Explicitly specifying your KMS customer master key id
    .withKmsCmkId(myKmsCmkId);
URL puturl = s3.generatePresignedUrl(genreq);
System.out.println("Presigned PUT URL using SSE-KMS with explicit CMK ID: "
    + puturl);
```

Here’s how to make use of the generated pre-signed PUT URL from (Example A) via the Apache HttpClient:
```
File fileToUpload = ...;
HttpPut putreq = new HttpPut(URI.create(puturl.toExternalForm()));
putreq.addHeader(new BasicHeader(Headers.SERVER_SIDE_ENCRYPTION,
    SSEAlgorithm.KMS.getAlgorithm()));
putreq.addHeader(new BasicHeader(Headers.SERVER_SIDE_ENCRYPTION_AWS_KMS_KEYID,
    myKmsCmkId)); // Explicitly specifying your KMS customer master key id
putreq.setEntity(new FileEntity(fileToUpload));
CloseableHttpClient httpclient = HttpClients.createDefault();
httpclient.execute(putreq);
```

Here is an example of a [test case](https://github.com/apache/jackrabbit-oak/blob/5f89d905e96de6f9bb9314a08529e262607ba406/oak-blob-cloud/src/test/java/org/apache/jackrabbit/oak/blob/cloud/s3/TestS3Ds.java#L180) where initiate, upload and complete binary upload phases are shown.
