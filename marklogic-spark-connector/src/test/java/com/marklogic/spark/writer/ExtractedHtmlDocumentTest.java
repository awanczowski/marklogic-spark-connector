/*
 * Copyright (c) 2026 Progress Software Corporation and/or its subsidiaries or affiliates. All Rights Reserved.
 */
package com.marklogic.spark.writer;

import com.marklogic.client.document.DocumentWriteOperation;
import com.marklogic.client.io.BytesHandle;
import com.marklogic.client.io.DOMHandle;
import com.marklogic.client.io.JacksonHandle;
import com.marklogic.spark.Context;
import com.marklogic.spark.Options;
import com.marklogic.spark.Util;
import com.marklogic.spark.core.DocumentInputs;
import com.marklogic.spark.core.DocumentPipeline;
import com.marklogic.spark.core.DocumentPipelineFactory;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.w3c.dom.Element;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class ExtractedHtmlDocumentTest {

    private static final String XHTML_NS = "http://www.w3.org/1999/xhtml";

    @ParameterizedTest
    @ValueSource(strings = {"json", "xml"})
    void htmlIsAStringInJsonAndElementsInXml(String documentType) throws Exception {
        DocumentInputs inputs = new DocumentInputs("/hello.docx",
            new BytesHandle(Files.readAllBytes(Path.of("src/test/resources/extraction-files/hello-world.docx"))),
            null, null);
        try (DocumentPipeline pipeline = DocumentPipelineFactory.newDocumentPipeline(new Context(Map.of(
            Options.WRITE_EXTRACTED_TEXT, "true",
            Options.WRITE_EXTRACTED_TEXT_FORMAT, "html")))) {
            pipeline.processDocuments(List.of(inputs));
        }
        DocBuilder builder = new DocBuilderFactory()
            .withUriMaker((uri, values) -> uri)
            .withExtractedTextDocumentType(documentType)
            .withExtractedTextFormat("html")
            .newDocBuilder();
        var documents = builder.buildDocuments(inputs);
        assertEquals(2, documents.size());
        DocumentWriteOperation extracted = documents.stream()
            .filter(doc -> doc.getUri().endsWith("-extracted-text." + documentType)).findFirst().orElseThrow();
        assertTrue(inputs.getExtractedText().contains("<html"));
        if ("json".equals(documentType)) {
            var doc = ((JacksonHandle) extracted.getContent()).get();
            assertTrue(doc.get("content").isTextual());
            assertEquals(inputs.getExtractedText(), doc.get("content").asText());
            assertEquals("/hello.docx", doc.get("source-uri").asText());
            assertEquals(inputs.getExtractedMetadata().get("Content-Type"), doc.get("extracted-metadata").get("Content-Type").asText());
        } else {
            var doc = ((DOMHandle) extracted.getContent()).get();
            Element content = (Element) doc.getElementsByTagNameNS(Util.DEFAULT_XML_NAMESPACE, "content").item(0);
            Element html = (Element) content.getFirstChild();
            assertEquals(1, content.getChildNodes().getLength(), "content should contain only the XHTML root element");
            assertEquals("html", html.getLocalName());
            assertEquals(XHTML_NS, html.getNamespaceURI());
            assertEquals(1, html.getElementsByTagNameNS(XHTML_NS, "body").getLength());
            assertTrue(html.getElementsByTagNameNS(XHTML_NS, "p").getLength() > 0);
            assertTrue(content.getTextContent().contains("Hello world."));
            assertEquals("/hello.docx",
                doc.getElementsByTagNameNS(Util.DEFAULT_XML_NAMESPACE, "source-uri").item(0).getTextContent());
        }
    }

    @Test
    void malformedHtmlFallsBackToTextInXmlEnvelope() {
        DocumentInputs inputs = new DocumentInputs("/broken.docx", new BytesHandle(new byte[]{1}), null, null);
        inputs.setExtractedText("<html><body><p>unclosed</body>");
        DocBuilder builder = new DocBuilderFactory()
            .withUriMaker((uri, values) -> uri)
            .withExtractedTextDocumentType("xml")
            .withExtractedTextFormat("html")
            .newDocBuilder();
        DocumentWriteOperation extracted = builder.buildDocuments(inputs).stream()
            .filter(doc -> doc.getUri().endsWith("-extracted-text.xml")).findFirst().orElseThrow();
        Element content = (Element) ((DOMHandle) extracted.getContent()).get()
            .getElementsByTagNameNS(Util.DEFAULT_XML_NAMESPACE, "content").item(0);
        assertEquals(0, content.getElementsByTagName("*").getLength());
        assertEquals(inputs.getExtractedText(), content.getTextContent());
    }

    @Test
    void textFormatRemainsTextInXmlEnvelope() {
        DocumentInputs inputs = new DocumentInputs("/plain.docx", new BytesHandle(new byte[]{1}), null, null);
        inputs.setExtractedText("<b>not markup</b>");
        DocBuilder builder = new DocBuilderFactory()
            .withUriMaker((uri, values) -> uri)
            .withExtractedTextDocumentType("xml")
            .newDocBuilder();
        DocumentWriteOperation extracted = builder.buildDocuments(inputs).stream()
            .filter(doc -> doc.getUri().endsWith("-extracted-text.xml")).findFirst().orElseThrow();
        Element content = (Element) ((DOMHandle) extracted.getContent()).get()
            .getElementsByTagNameNS(Util.DEFAULT_XML_NAMESPACE, "content").item(0);
        assertEquals(0, content.getElementsByTagName("*").getLength());
        assertEquals("<b>not markup</b>", content.getTextContent());
    }
}
