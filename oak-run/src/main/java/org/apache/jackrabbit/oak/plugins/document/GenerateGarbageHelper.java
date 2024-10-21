package org.apache.jackrabbit.oak.plugins.document;

/**
 * Helper class to generate garbage for testing purposes.
 */
public class GenerateGarbageHelper {

    public static boolean isInvalidGarbageGenerationMode(int fullGCMode) {
        return fullGCMode == 0;
    }

    public static boolean includesEmptyProps(int fullGCMode) {
        return fullGCMode == 1;
    }

    public static boolean includesGapOrphans(int fullGCMode) {
        return fullGCMode == 2;
    }
}
