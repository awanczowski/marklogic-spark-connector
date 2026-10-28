/*
 * Copyright (c) 2023-2025 Progress Software Corporation and/or its subsidiaries or affiliates. All Rights Reserved.
 */
package com.marklogic.spark.core.extraction;

import java.util.HashMap;
import java.util.Map;

public abstract class ExtractionUtil {

    /**
     * Defines XML namespaces associated with metadata extracted by Apache Tika.
     */
    private static final Map<String, String> PREFIX_TO_NAMESPACE_MAPPING = new HashMap<>();

    static {
        PREFIX_TO_NAMESPACE_MAPPING.put("dc", "http://purl.org/dc/elements/1.1/");
        PREFIX_TO_NAMESPACE_MAPPING.put("dcterms", "http://purl.org/dc/terms/");
        PREFIX_TO_NAMESPACE_MAPPING.put("exif", "http://ns.adobe.com/exif/1.0/");
        PREFIX_TO_NAMESPACE_MAPPING.put("pdf", "http://ns.adobe.com/pdf/1.3/");
        PREFIX_TO_NAMESPACE_MAPPING.put("tiff", "http://ns.adobe.com/tiff/1.0/");
        PREFIX_TO_NAMESPACE_MAPPING.put("xhtml", "http://www.w3.org/1999/xhtml");
        PREFIX_TO_NAMESPACE_MAPPING.put("xmp", "http://ns.adobe.com/xap/1.0/");
    }

    public static String getNamespace(String prefix) {
        return PREFIX_TO_NAMESPACE_MAPPING.get(prefix);
    }

    /**
     * Converts a metadata name - e.g. a custom document property such as "My Property" - into a valid XML local name
     * by replacing characters that are not allowed in an XML name with an underscore,
     * and prefixing an underscore if the name does not start with a valid name start character. Names that are
     * already valid are returned unchanged.
     *
     * @since 3.1.3
     */
    public static String toXmlLocalName(String name) {
        if (name == null || name.isEmpty()) {
            return "_";
        }
        StringBuilder result = new StringBuilder(name.length() + 1);
        name.codePoints().forEach(c -> {
            if (isXmlNameChar(c)) {
                result.appendCodePoint(c);
            } else {
                result.append('_');
            }
        });
        if (!isXmlNameStartChar(result.codePointAt(0))) {
            result.insert(0, '_');
        }
        return result.toString();
    }

    /**
     * Replaces characters that are not allowed in XML 1.0 - e.g. control characters in document properties - with
     * the Unicode replacement character, as Tika does for extracted text. Values without such characters are returned
     * unchanged.
     *
     * @since 3.1.3
     */
    public static String toValidXmlText(String text) {
        if (text == null || text.codePoints().allMatch(ExtractionUtil::isXmlChar)) {
            return text;
        }
        StringBuilder result = new StringBuilder(text.length());
        text.codePoints().forEach(c -> result.appendCodePoint(isXmlChar(c) ? c : 0xFFFD));
        return result.toString();
    }

    private static boolean isXmlChar(int c) {
        return c == 0x9 || c == 0xA || c == 0xD
            || (c >= 0x20 && c <= 0xD7FF)
            || (c >= 0xE000 && c <= 0xFFFD)
            || (c >= 0x10000 && c <= 0x10FFFF);
    }

    // Per the NameStartChar production in XML 1.0, excluding ":" as the name is used as a local name.
    private static boolean isXmlNameStartChar(int c) {
        return (c >= 'A' && c <= 'Z') || c == '_' || (c >= 'a' && c <= 'z')
            || (c >= 0xC0 && c <= 0xD6) || (c >= 0xD8 && c <= 0xF6) || (c >= 0xF8 && c <= 0x2FF)
            || (c >= 0x370 && c <= 0x37D) || (c >= 0x37F && c <= 0x1FFF) || (c >= 0x200C && c <= 0x200D)
            || (c >= 0x2070 && c <= 0x218F) || (c >= 0x2C00 && c <= 0x2FEF) || (c >= 0x3001 && c <= 0xD7FF)
            || (c >= 0xF900 && c <= 0xFDCF) || (c >= 0xFDF0 && c <= 0xFFFD) || (c >= 0x10000 && c <= 0xEFFFF);
    }

    // Per the NameChar production in XML 1.0, excluding ":".
    private static boolean isXmlNameChar(int c) {
        return isXmlNameStartChar(c) || c == '-' || c == '.' || (c >= '0' && c <= '9') || c == 0xB7
            || (c >= 0x300 && c <= 0x36F) || (c >= 0x203F && c <= 0x2040);
    }
    
    private ExtractionUtil() {
    }
}
