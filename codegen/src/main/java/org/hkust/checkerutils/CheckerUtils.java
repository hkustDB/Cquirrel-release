package org.hkust.checkerutils;

import java.util.Collection;

public class CheckerUtils {
    private static final String NULL_OR_EMPTY_MESSAGE = " cannot be null or empty";

    //TODO: could these functions be made generic?
    public static void checkNullOrEmpty(String string, String name) {
        if (string == null || string.isEmpty()) {
            throw new RuntimeException(name + NULL_OR_EMPTY_MESSAGE);
        }
    }

    public static void checkNullOrEmpty(Collection collection, String name) {
        if (collection == null || collection.isEmpty()) {
            throw new RuntimeException(name + NULL_OR_EMPTY_MESSAGE);
        }
    }

    public static void validateNonNullNonEmpty(Collection collection, String name) {
        if (collection != null && collection.isEmpty()) {
            throw new RuntimeException(name + " cannot be empty. In case of no keys pass null");
        }
    }
}
