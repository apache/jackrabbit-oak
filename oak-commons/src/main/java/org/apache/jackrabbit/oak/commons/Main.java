package org.apache.jackrabbit.oak.commons;

import org.apache.jackrabbit.guava.common.base.Strings;

public class Main {

    public static void main(String[] args) {
//        System.out.println(Strings.isNullOrEmpty(""));
//        System.out.println(isNullOrEmpty(""));
//
//
//        System.out.println(Strings.isNullOrEmpty("asas"));
//        System.out.println(isNullOrEmpty("asas"));
//
//        System.out.println(Strings.isNullOrEmpty(" "));
//        System.out.println(isNullOrEmpty(" "));


        System.out.println(isNullOrBlank(""));
        System.out.println(isNullOrBlank(" "));
        System.out.println(isNullOrBlank("as"));


    }

    static boolean isNullOrEmpty(String str) {
        return str == null || str.isEmpty();
    }

    static boolean isNullOrBlank(String str) {
        return str == null || str.isBlank();
    }
}
