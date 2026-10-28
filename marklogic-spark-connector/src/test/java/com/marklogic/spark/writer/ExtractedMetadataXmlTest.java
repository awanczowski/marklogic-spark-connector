/*
 * Copyright (c) 2026 Progress Software Corporation and/or its subsidiaries or affiliates. All Rights Reserved.
 */
package com.marklogic.spark.writer;

import com.marklogic.client.document.DocumentWriteOperation;
import com.marklogic.client.io.BytesHandle;
import com.marklogic.client.io.DOMHandle;
import com.marklogic.spark.Util;
import com.marklogic.spark.core.DocumentInputs;
import org.junit.jupiter.api.Test;
import org.w3c.dom.Element;

import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class ExtractedMetadataXmlTest {

    @Test
    void metadataWithInvalidNamesAndValuesIsRetained() {
        Map<String, String> metadata = new LinkedHashMap<>();
        metadata.put("custom:My Property", "Example value");
        metadata.put("custom:2nd Property", "Another value");
        metadata.put("dc:creator", "Some\u0001one");
        metadata.put("Content-Type", "application/pdf");

        DocumentInputs inputs = new DocumentInputs("/test.xlsx", new BytesHandle(new byte[]{1}), null, null);
        inputs.setExtractedText("text");
        inputs.setExtractedMetadata(metadata);
        DocumentWriteOperation extracted = new DocBuilderFactory()
            .withUriMaker((uri, values) -> uri)
            .withExtractedTextDocumentType("xml")
            .withExtractedTextDropSource(true)
            .newDocBuilder()
            .buildDocuments(inputs).iterator().next();

        Element root = ((DOMHandle) extracted.getContent()).get().getDocumentElement();
        Element extractedMetadata = (Element) root.getElementsByTagNameNS(Util.DEFAULT_XML_NAMESPACE, "extracted-metadata").item(0);
        assertEquals(4, extractedMetadata.getChildNodes().getLength(), "No metadata entry is dropped");

        Element myProperty = metadataElement(extractedMetadata, "custom-My_Property");
        assertEquals("custom:My Property", myProperty.getAttribute("key"));
        assertEquals("Example value", myProperty.getTextContent());

        Element secondProperty = metadataElement(extractedMetadata, "custom-2nd_Property");
        assertEquals("custom:2nd Property", secondProperty.getAttribute("key"));
        assertEquals("Another value", secondProperty.getTextContent());

        Element creator = (Element) extractedMetadata.getElementsByTagNameNS("http://purl.org/dc/elements/1.1/", "creator").item(0);
        assertEquals("Some\uFFFDone", creator.getTextContent(), "Invalid XML characters in values are replaced");
        assertFalse(creator.hasAttribute("key"), "The key is only added when the name had to be changed");

        assertFalse(metadataElement(extractedMetadata, "Content-Type").hasAttribute("key"));
        assertFalse(((DOMHandle) extracted.getContent()).toString().contains("\u0001"));
    }

    private Element metadataElement(Element extractedMetadata, String localName) {
        Element element = (Element) extractedMetadata.getElementsByTagNameNS(Util.DEFAULT_XML_NAMESPACE, localName).item(0);
        assertNotNull(element, "Expected element: " + localName);
        return element;
    }
}
