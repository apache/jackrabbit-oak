package org.apache.jackrabbit.oak.segment.spi.persistence;

import java.io.IOException;

public interface JournalFile {

    JournalFileReader openJournalReader() throws IOException;

    JournalFileWriter openJournalWriter() throws IOException;

    String getName();

    boolean exists();
}
