package org.apache.jackrabbit.oak.spi.blob;

public class DirectBinaryAccessException extends Exception {
    public DirectBinaryAccessException() {}
    public DirectBinaryAccessException(String message) {
        super(message);
    }
}
