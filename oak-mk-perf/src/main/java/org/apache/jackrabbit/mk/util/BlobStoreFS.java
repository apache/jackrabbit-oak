package org.apache.jackrabbit.mk.util;

import java.io.File;
import java.io.InputStream;

import org.apache.jackrabbit.mongomk.api.BlobStore;



public class BlobStoreFS implements BlobStore {


   public BlobStoreFS(String rootPath) {
       File rootDir = new File(rootPath);
       if (!rootDir.isDirectory()) {
           rootDir.mkdirs();
       }


   }

   public long getBlobLength(String blobId) throws Exception {
       return 0;
   }

   public int readBlob(String blobId, long blobOffset, byte[] buffer, int bufferOffset, int length) throws Exception {
       return 0;
   }

   public String writeBlob(InputStream is) throws Exception {
       return null;
   }
   
}