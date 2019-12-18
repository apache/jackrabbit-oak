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

# Direct Binary Access upload file process using SSE Encryption

The direct binary upload process is split into [3 phases](direct-binary-access.html)

The remote client performs the actual binary upload directly to the binary storage provider. The BinaryUpload returned  to `initiateBinaryUpload(long, int)` contains detailed instructions on how to complete the upload successfully. For more information, see the `BinaryUpload` documentation.

Example A: Here’s how to initiateHttpUpload:
```
long ONE_GB = 1048576000;
/*Set all the properties for SSE*/
if(properties != null){
    setProperties(properties);
}
DataRecordUpload uploadContext = initiateHttpUpload(ONE_GB, 1);
String uploadToken = uploadContext.getUploadToken();
/*resultHttpStatusCode should be returned as 200 */
int resultHttpStatusCode =  httpPut(uploadContext);
```

Here’s how to make use of the context returned by the `initiateHttpUpload` in Example A to upload a file using different SSE Encryption:
```
int httpPut(@Nullable DataRecordUpload uploadContext) throws IOException  {
    File fileToUpload = ...;
    String keyId = null;
    URI puturl = uploadContext.getUploadURIs().iterator().next();
    HttpPut putreq = new HttpPut(puturl);

    String encryptionType = props.getProperty(s3Encryption);

    if (encryptionType.equals(SSE_KMS)) {
        keyId = props.getProperty(kmsKeyId);
        putreq.addHeader(new BasicHeader(Headers.SERVER_SIDE_ENCRYPTION,
               SSEAlgorithm.KMS.getAlgorithm()));
        if(keyId != null) {
            putreq.addHeader(new BasicHeader(Headers.SERVER_SIDE_ENCRYPTION_AWS_KMS_KEYID,
               keyId));
        }
    } else {
        putreq.addHeader(new BasicHeader(Headers.SERVER_SIDE_ENCRYPTION,
            SSEAlgorithm.AES256.getAlgorithm()));
    }

    // Explicitly specifying your KMS customer master key id
    putreq.setEntity(new FileEntity(fileToUpload));
    CloseableHttpClient httpclient = HttpClients.createDefault();
    CloseableHttpResponse response  = httpclient.execute(putreq);
    return response.getStatusLine().getStatusCode();
}
```

Here is an example of a [test case](https://github.com/apache/jackrabbit-oak/blob/5f89d905e96de6f9bb9314a08529e262607ba406/oak-blob-cloud/src/test/java/org/apache/jackrabbit/oak/blob/cloud/s3/TestS3Ds.java#L180) where initiate, upload and complete binary upload phases are shown.
