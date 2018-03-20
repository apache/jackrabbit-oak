package org.apache.jackrabbit.oak.segment.spi.persistence;

import java.io.Closeable;
import java.io.IOException;

public interface JournalFileReader extends Closeable {

    String readLine() throws IOException;

}
