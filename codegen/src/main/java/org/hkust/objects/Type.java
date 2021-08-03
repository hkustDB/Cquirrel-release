package org.hkust.objects;

import com.google.common.collect.ImmutableMap;
import org.hkust.checkerutils.CheckerUtils;
import org.jetbrains.annotations.Nullable;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

public class Type {
    //types with String constructor
    private static Map<String, Class> types;

    //Note: if all numbers types can't be instantiated with string use BigDecimal
    static {
        Map<String, Class> typesTemp = new HashMap<>();
        typesTemp.put("int", Integer.class);
        typesTemp.put("integer", Integer.class);
        typesTemp.put("string", String.class);
        typesTemp.put("varchar", String.class);
        typesTemp.put("double", Double.class);
        typesTemp.put("date", Date.class);
        typesTemp.put("char", Character.class);
        typesTemp.put("long", Long.class);
        types = ImmutableMap.copyOf(typesTemp);
    }

    private static final Map<Class<?>, String> stringConversionMethods = new HashMap<>();

    static {
        stringConversionMethods.put(Integer.class, "toInt");
        stringConversionMethods.put(Double.class, "toDouble");
        stringConversionMethods.put(Long.class, "toLong");
        stringConversionMethods.put(Date.class, "format.parse");
    }

    @Nullable
    public static String getStringConversionMethod(Class<?> clss) {
        return stringConversionMethods.get(clss);
    }

    @Nullable
    public static Class<?> getClass(final String className) {
        CheckerUtils.checkNullOrEmpty(className, "className");
        String typeLower = className.toLowerCase();
        return types.get(typeLower);
    }
}
