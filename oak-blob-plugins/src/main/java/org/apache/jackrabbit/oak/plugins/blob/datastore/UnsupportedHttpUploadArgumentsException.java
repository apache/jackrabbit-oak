package org.apache.jackrabbit.oak.plugins.blob.datastore;

public class UnsupportedHttpUploadArgumentsException extends Exception {
    public UnsupportedHttpUploadArgumentsException() {
        super();
    }
    public UnsupportedHttpUploadArgumentsException(final String message) {
        super(message);
    }
}
