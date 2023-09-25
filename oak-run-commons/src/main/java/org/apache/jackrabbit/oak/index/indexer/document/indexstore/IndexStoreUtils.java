package org.apache.jackrabbit.oak.index.indexer.document.indexstore;

import org.apache.jackrabbit.oak.commons.Compression;
import org.jetbrains.annotations.NotNull;

import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.util.List;

import static org.apache.jackrabbit.guava.common.base.Preconditions.checkState;

public class IndexStoreUtils {
    public static final String METADATA_SUFFIX = ".metadata";

    /**
     * This function by default uses GNU zip as compression algorithm for backward compatibility.
     */
    public static BufferedReader createReader(File file, boolean compressionEnabled) {
        return createReader(file, compressionEnabled ? Compression.GZIP : Compression.NONE);
    }

    public static BufferedReader createReader(File file, Compression algorithm) {
        try {
            InputStream in = new FileInputStream(file);
            return new BufferedReader(new InputStreamReader(algorithm.getInputStream(in)));
        } catch (IOException e) {
            throw new RuntimeException("Error opening file " + file, e);
        }
    }

    /**
     * This function by default uses GNU zip as compression algorithm for backward compatibility.
     */
    public static BufferedWriter createWriter(File file, boolean compressionEnabled) throws IOException {
        return createWriter(file, compressionEnabled ? Compression.GZIP : Compression.NONE);
    }

    public static BufferedWriter createWriter(File file, Compression algorithm) throws IOException {
        OutputStream out = new FileOutputStream(file);
        return new BufferedWriter(new OutputStreamWriter(algorithm.getOutputStream(out)));
    }

    public static BufferedOutputStream createOutputStream(File file, Compression algorithm) throws IOException {
        OutputStream out = new FileOutputStream(file);
        return new BufferedOutputStream(algorithm.getOutputStream(out));
    }

    public static long sizeOf(List<File> sortedFiles) {
        return sortedFiles.stream().mapToLong(File::length).sum();
    }

    /**
     * This function by default uses GNU zip as compression algorithm for backward compatibility.
     */
    public static String getSortedStoreFileName(boolean compressionEnabled) {
        return getSortedStoreFileName(compressionEnabled ? Compression.GZIP : Compression.NONE);
    }

    public static String getSortedStoreFileName(Compression algorithm) {
        return algorithm.addSuffix("store-sorted.json");
    }

    public static String getMetadataFileName(Compression algorithm) {
        return algorithm.addSuffix("store-sorted.json.metadata");
    }

    /*
        Metadata file is placed in same folder as IndexStore file with following naming convention.
        e.g. <filename.json>.<compression suffix> then metadata file is stored as <filename.json>.metadata.<compression suffix>
     */
    public static File getMetadataFile(File indexStoreFile, Compression algorithm) {
        File metadataFile;
        if (algorithm.equals(Compression.NONE)) {
            metadataFile = new File(indexStoreFile.getAbsolutePath() + METADATA_SUFFIX);
        } else {
            String fileName = indexStoreFile.getName();
            String compressionSuffix = getCompressionSuffix(indexStoreFile);
            checkState(algorithm.addSuffix("").equals(compressionSuffix));
            String fileNameWithoutCompressionSuffix = fileName.substring(0, fileName.lastIndexOf("."));
            metadataFile = new File(algorithm.addSuffix(indexStoreFile.getParent() + "/"
                    + fileNameWithoutCompressionSuffix + METADATA_SUFFIX));
        }
        return metadataFile;
    }

    private static String getCompressionSuffix(File file) {
        return file.getName().substring(file.getName().lastIndexOf("."));
    }

    /**
     * This method validates the compression suffix is in correspondence with compression algorithm.
     */
    public static void validateFlatFileStoreFileName(File file, @NotNull Compression algorithm) {
        if (!algorithm.equals(Compression.NONE)) {
            checkState(algorithm.addSuffix("")
                            .equals(getCompressionSuffix(file)),
                    "File suffix should be in correspondence with compression algorithm. Filename:{}, Compression suffix:{} ",
                    file.getAbsolutePath(), algorithm.addSuffix(""));
        }
    }

}
