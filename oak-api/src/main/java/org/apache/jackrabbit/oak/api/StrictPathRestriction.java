package org.apache.jackrabbit.oak.api;

public enum StrictPathRestriction {
    ENABLE("enable"),
    WARN("warn"),
    DISABLE("disable");
    private String stringValue;

    StrictPathRestriction(String pathRestriction) {
        stringValue = pathRestriction;
    }

    public static StrictPathRestriction stringToEnum(String strictPathRestrictionInString) {
        return StrictPathRestriction.valueOf(strictPathRestrictionInString.toUpperCase());
    }

}
