/*
 * Copyright (c) 2026 Progress Software Corporation and/or its subsidiaries or affiliates. All Rights Reserved.
 */
package com.marklogic.spark.core.extraction;

import com.marklogic.client.io.BytesHandle;
import com.marklogic.spark.ConnectorException;
import com.marklogic.spark.core.DocumentInputs;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.w3c.dom.Document;
import org.xml.sax.InputSource;

import javax.xml.parsers.DocumentBuilderFactory;
import java.io.StringReader;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

class TikaTextExtractorTest {

    @Test
    void defaultAndExplicitTextPreserveExistingOutput() throws Exception {
        DocumentInputs inputs = inputs("hello-world.docx");
        ExtractionResult result = new TikaTextExtractor().extractText(inputs).orElseThrow();
        assertEquals("Hello world.\nThis file is used for testing text extraction.\n", result.getText());
        assertEquals(result.getText(), new TikaTextExtractor("TEXT").extractText(inputs).orElseThrow().getText());
        assertEquals("application/vnd.openxmlformats-officedocument.wordprocessingml.document",
            result.getMetadata().get("Content-Type"));
    }

    @ParameterizedTest
    @ValueSource(strings = {"hello-world.docx", "marklogic-getting-started.pdf"})
    void htmlIsStructuredXhtmlWithMetadata(String filename) throws Exception {
        DocumentInputs inputs = inputs(filename);
        ExtractionResult result = new TikaTextExtractor("HTML").extractText(inputs).orElseThrow();
        DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
        factory.setNamespaceAware(true);
        Document html = factory.newDocumentBuilder().parse(new InputSource(new StringReader(result.getText())));
        assertEquals("html", html.getDocumentElement().getLocalName());
        assertEquals("http://www.w3.org/1999/xhtml", html.getDocumentElement().getNamespaceURI());
        assertEquals(1, html.getElementsByTagNameNS("http://www.w3.org/1999/xhtml", "body").getLength());
        assertTrue(html.getElementsByTagNameNS("http://www.w3.org/1999/xhtml", "p").getLength() > 0);
        assertTrue(html.getDocumentElement().getTextContent().contains(
            filename.endsWith("docx") ? "Hello world." : "MarkLogic Server Table of Contents"));
        assertEquals(new TikaTextExtractor().extractText(inputs).orElseThrow().getMetadata().get("Content-Type"),
            result.getMetadata().get("Content-Type"));
        assertFalse(result.getMetadata().containsKey("pdf:unmappedUnicodeCharsPerPage"));
    }

    @Test
    void htmlIsNotTruncatedAtTextWriteLimit() throws Exception {
        String line = "Row of text used to exceed the default Tika write limit.\n";
        String body = line.repeat(200_000 / line.length() + 1) + "END-OF-DOCUMENT";
        DocumentInputs inputs = new DocumentInputs("/large.txt",
            new BytesHandle(body.getBytes(java.nio.charset.StandardCharsets.UTF_8)), null, null);

        String html = new TikaTextExtractor("html").extractText(inputs).orElseThrow().getText();
        assertTrue(html.length() > 200_000);
        assertTrue(html.contains("END-OF-DOCUMENT"));
        DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
        factory.setNamespaceAware(true);
        assertNotNull(factory.newDocumentBuilder().parse(new InputSource(new StringReader(html))));

        assertEquals(100_000, new TikaTextExtractor().extractText(inputs).orElseThrow().getText().length(),
            "Text extraction keeps Tika's default write limit");
    }

    @ParameterizedTest
    @ValueSource(strings = {"text", "html"})
    void emptyAndMissingContent(String format) {
        TikaTextExtractor extractor = new TikaTextExtractor(format);
        assertTrue(extractor.extractText(new DocumentInputs("/empty", new BytesHandle(new byte[0]), null, null)).isEmpty());
        assertTrue(extractor.extractText(new DocumentInputs("/missing", null, null, null)).isEmpty());
    }

    @Test
    void rejectsInvalidFormats() {
        assertThrows(ConnectorException.class, () -> new TikaTextExtractor("pdf"));
        assertThrows(ConnectorException.class, () -> new TikaTextExtractor(""));
        assertThrows(ConnectorException.class, () -> new TikaTextExtractor(null));
    }

    private DocumentInputs inputs(String filename) throws Exception {
        byte[] bytes = Files.readAllBytes(Path.of("src/test/resources/extraction-files", filename));
        return new DocumentInputs("/" + filename, new BytesHandle(bytes), null, null);
    }
}
