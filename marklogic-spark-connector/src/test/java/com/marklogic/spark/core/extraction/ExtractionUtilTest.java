/*
 * Copyright (c) 2026 Progress Software Corporation and/or its subsidiaries or affiliates. All Rights Reserved.
 */
package com.marklogic.spark.core.extraction;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class ExtractionUtilTest {

    @Test
    void validNamesAreUnchanged() {
        for (String name : new String[]{"X-TIKA-Parsed-By", "docinfo-custom-Company", "Content-Type", "creator", "_x", "a.b"}) {
            assertEquals(name, ExtractionUtil.toXmlLocalName(name), "Expected valid name to be unchanged");
        }
    }

    @Test
    void invalidNameCharactersAreReplaced() {
        assertEquals("custom-My_Property", ExtractionUtil.toXmlLocalName("custom-My Property"));
        assertEquals("custom-My_Property__Draft_", ExtractionUtil.toXmlLocalName("custom-My Property (Draft)"));
        assertEquals("a_b", ExtractionUtil.toXmlLocalName("a/b"));
    }

    @Test
    void namesMustStartWithValidCharacter() {
        assertEquals("_2nd_Property", ExtractionUtil.toXmlLocalName("2nd Property"));
        assertEquals("_-name", ExtractionUtil.toXmlLocalName("-name"));
        assertEquals("_", ExtractionUtil.toXmlLocalName(""));
        assertEquals("_", ExtractionUtil.toXmlLocalName(null));
    }

    @Test
    void invalidXmlCharactersInTextAreReplaced() {
        assertEquals("a\uFFFDb\uFFFDc", ExtractionUtil.toValidXmlText("a\u0001b\u000Bc"));
        assertEquals("x\uFFFDy", ExtractionUtil.toValidXmlText("x\uD800y"), "Unpaired surrogates are invalid");
        assertEquals("tab\tnewline\ncr\r\uD83D\uDE00", ExtractionUtil.toValidXmlText("tab\tnewline\ncr\r\uD83D\uDE00"),
            "Whitespace and supplementary characters are valid");
        String valid = "Valid text";
        assertSame(valid, ExtractionUtil.toValidXmlText(valid));
        assertNull(ExtractionUtil.toValidXmlText(null));
    }
}
