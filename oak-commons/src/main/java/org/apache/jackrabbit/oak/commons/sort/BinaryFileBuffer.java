package org.apache.jackrabbit.oak.commons.sort;

import java.io.BufferedReader;
import java.io.EOFException;
import java.io.IOException;
import java.util.function.Function;

public class BinaryFileBuffer<T> {
    public final BufferedReader fbr;
    private final Function<String, T> stringToType;
    private T cache;
    private boolean empty;

    public BinaryFileBuffer(BufferedReader r, Function<String, T> stringToType)
            throws IOException {
        this.fbr = r;
        this.stringToType = stringToType;
        reload();
    }

    public boolean empty() {
        return this.empty;
    }

    private void reload() throws IOException {
        try {
            if ((this.cache = stringToType.apply(fbr.readLine())) == null) {
                this.empty = true;
                this.cache = null;
            } else {
                this.empty = false;
            }
        } catch (EOFException oef) {
            this.empty = true;
            this.cache = null;
        }
    }

    public void close() throws IOException {
        this.fbr.close();
    }

    public T peek() {
        if (empty()) {
            return null;
        }
        return this.cache;
    }

    public T pop() throws IOException {
        T answer = peek();
        reload();
        return answer;
    }

}
