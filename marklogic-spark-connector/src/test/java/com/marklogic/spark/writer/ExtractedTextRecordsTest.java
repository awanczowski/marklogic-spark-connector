/*
 * Copyright (c) 2026 Progress Software Corporation and/or its subsidiaries or affiliates. All Rights Reserved.
 */
package com.marklogic.spark.writer;

import com.marklogic.client.document.DocumentWriteOperation;
import com.marklogic.client.io.BytesHandle;
import com.marklogic.client.io.DOMHandle;
import com.marklogic.spark.ConnectorException;
import com.marklogic.spark.Context;
import com.marklogic.spark.Options;
import com.marklogic.spark.Util;
import com.marklogic.spark.core.DocumentInputs;
import com.marklogic.spark.core.DocumentPipeline;
import com.marklogic.spark.core.DocumentPipelineFactory;
import com.marklogic.spark.core.classifier.MockSemaphoreProxy;
import com.marklogic.spark.core.extraction.ExtractedRecord;
import com.marklogic.spark.core.extraction.ExtractedTextRecordSplitter;
import com.marklogic.spark.writer.classifier.ClassifierTestUtil;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

class ExtractedTextRecordsTest {

    private static final String XHTML_NS = "http://www.w3.org/1999/xhtml";

    private static final String XHTML = """
        <html xmlns="http://www.w3.org/1999/xhtml"><head><title>t</title></head><body>
        <div class="sheet" id="s1"><h1>First</h1><table><tbody><tr><td>A1</td></tr></tbody></table>
          <div class="threaded-comment">nested comment</div></div>
        <div class="package-entry"><h1>image1.png</h1></div>
        <div class="sheet" id="s2"><h1>Second &amp; more</h1><table><tbody><tr><td>B1</td></tr></tbody></table></div>
        </body></html>""";

    @Test
    void splitterFiltersOnAttributeAndKeepsNestedMatchesInsideRecords() {
        List<ExtractedRecord> records =
            new ExtractedTextRecordSplitter(XHTML_NS, "div", "class=sheet", null).split(XHTML);
        assertEquals(2, records.size());
        assertTrue(records.get(0).getContent().contains("nested comment"));
        assertTrue(records.get(1).getContent().contains("Second &amp; more"));
        assertTrue(records.stream().noneMatch(r -> r.getContent().contains("image1.png")));
        assertNull(records.get(0).getUriValue());

        assertEquals(3, new ExtractedTextRecordSplitter(XHTML_NS, "div", null, null).split(XHTML).size(),
            "Without a filter, every outermost div is a record, including the package-entry div");
    }

    @Test
    void splitterRejectsInvalidAttributeFilter() {
        assertThrows(ConnectorException.class, () -> new ExtractedTextRecordSplitter(XHTML_NS, "div", "class", null));
        assertThrows(ConnectorException.class, () -> new ExtractedTextRecordSplitter(XHTML_NS, "div", "=sheet", null));
        assertThrows(ConnectorException.class, () -> new ExtractedTextRecordSplitter(XHTML_NS, " ", null, null));
    }

    @Test
    void bareRecordsReplaceExtractedDocumentAndUsePositionUris() {
        DocumentInputs inputs = inputsWithExtractedText(XHTML);
        List<DocumentWriteOperation> docs = new ArrayList<>(newBuilder("class=sheet", null, false, true).buildDocuments(inputs));

        assertEquals(List.of("/test.xlsx-extracted-1.xml", "/test.xlsx-extracted-2.xml"),
            docs.stream().map(DocumentWriteOperation::getUri).toList(),
            "The source document is dropped and the single extracted text document is replaced by records");

        Element first = root(docs.get(0));
        assertEquals("div", first.getLocalName());
        assertEquals(XHTML_NS, first.getNamespaceURI());
        assertEquals("sheet", first.getAttribute("class"));
        assertEquals(1, first.getElementsByTagNameNS(XHTML_NS, "table").getLength());
        assertEquals(1, first.getElementsByTagNameNS(XHTML_NS, "div").getLength(), "Nested div stays in its record");
        assertEquals("Second & more", root(docs.get(1)).getElementsByTagNameNS(XHTML_NS, "h1").item(0).getTextContent());
    }

    @Test
    void sourceDocumentIsKeptUnlessDropped() {
        Collection<DocumentWriteOperation> docs = newBuilder("class=sheet", null, false, false)
            .buildDocuments(inputsWithExtractedText(XHTML));
        assertEquals(List.of("/test.xlsx", "/test.xlsx-extracted-1.xml", "/test.xlsx-extracted-2.xml"),
            docs.stream().map(DocumentWriteOperation::getUri).toList());
    }

    @Test
    void envelopedRecords() {
        DocumentInputs inputs = inputsWithExtractedText(XHTML);
        inputs.setExtractedMetadata(Map.of("dc:creator", "someone"));
        List<DocumentWriteOperation> docs = new ArrayList<>(newBuilder("class=sheet", null, true, false).buildDocuments(inputs));
        assertEquals(3, docs.size());

        Element envelope = root(docs.get(2));
        assertEquals("root", envelope.getLocalName());
        assertEquals(Util.DEFAULT_XML_NAMESPACE, envelope.getNamespaceURI());
        assertEquals("/test.xlsx", child(envelope, "source-uri").getTextContent());
        assertEquals("2", child(envelope, "record-index").getTextContent());
        Element content = child(envelope, "content");
        assertEquals(1, content.getChildNodes().getLength());
        Element div = (Element) content.getFirstChild();
        assertEquals(XHTML_NS, div.getNamespaceURI());
        assertEquals("s2", div.getAttribute("id"));
        assertEquals("someone", envelope.getElementsByTagNameNS("http://purl.org/dc/elements/1.1/", "creator").item(0).getTextContent());
    }

    @Test
    void envelopeOmitsSourceUriWhenSourceIsDropped() {
        List<DocumentWriteOperation> docs = new ArrayList<>(newBuilder("class=sheet", null, true, true)
            .buildDocuments(inputsWithExtractedText(XHTML)));
        assertEquals(0, root(docs.get(0)).getElementsByTagNameNS(Util.DEFAULT_XML_NAMESPACE, "source-uri").getLength());
        assertEquals("1", child(root(docs.get(0)), "record-index").getTextContent());
    }

    @Test
    void uriAttribute() {
        List<String> uris = newBuilder("class=sheet", "id", false, true).buildDocuments(inputsWithExtractedText(XHTML))
            .stream().map(DocumentWriteOperation::getUri).toList();
        assertEquals(List.of("/test.xlsx-extracted-s1.xml", "/test.xlsx-extracted-s2.xml"), uris);
    }

    @Test
    void missingUriAttributeValue() {
        DocBuilder builder = newBuilder(null, "id", false, true);
        DocumentInputs inputs = inputsWithExtractedText(XHTML);
        ConnectorException ex = assertThrows(ConnectorException.class, () -> builder.buildDocuments(inputs));
        assertTrue(ex.getMessage().contains("No value for attribute 'id' found on record 2"), ex.getMessage());
    }

    @Test
    void duplicateUriAttributeValue() {
        DocBuilder builder = newBuilder("class=sheet", "class", false, true);
        DocumentInputs inputs = inputsWithExtractedText(XHTML);
        ConnectorException ex = assertThrows(ConnectorException.class, () -> builder.buildDocuments(inputs));
        assertTrue(ex.getMessage().contains("Duplicate value 'sheet' for attribute 'class'"), ex.getMessage());
    }

    @Test
    void noMatchesFallsBackToSingleExtractedDocument() {
        List<String> uris = newBuilder("class=nothing", null, false, true).buildDocuments(inputsWithExtractedText(XHTML))
            .stream().map(DocumentWriteOperation::getUri).toList();
        assertEquals(List.of("/test.xlsx-extracted-text.xml"), uris);
    }

    @Test
    void realTikaXhtml() throws Exception {
        DocumentInputs inputs = new DocumentInputs("/hello.docx",
            new BytesHandle(Files.readAllBytes(Path.of("src/test/resources/extraction-files/hello-world.docx"))), null, null);
        try (DocumentPipeline pipeline = DocumentPipelineFactory.newDocumentPipeline(new Context(Map.of(
            Options.WRITE_EXTRACTED_TEXT, "true",
            Options.WRITE_EXTRACTED_TEXT_FORMAT, "HTML")))) {
            pipeline.processDocuments(List.of(inputs));
        }
        List<DocumentWriteOperation> docs = new ArrayList<>(new DocBuilderFactory()
            .withUriMaker((uri, values) -> uri)
            .withExtractedTextDocumentType("xml")
            .withExtractedTextFormat("html")
            .withExtractedTextDropSource(true)
            .withExtractedTextRecords(XHTML_NS, "p", null, null, false)
            .newDocBuilder()
            .buildDocuments(inputs));
        assertEquals(2, docs.size());
        assertEquals("Hello world.", root(docs.get(0)).getTextContent());
        assertEquals("p", root(docs.get(1)).getLocalName());
    }

    @Test
    void validation() {
        assertDoesNotThrow(() -> writeContext(Map.of()).validateExtractedTextRecordOptions());
        assertDoesNotThrow(() -> writeContext(validRecordOptions()).validateExtractedTextRecordOptions());

        Map<String, String> options = validRecordOptions();
        options.put(Options.WRITE_EXTRACTED_TEXT_FORMAT, "text");
        assertValidationError(options, "requires 'spark.marklogic.write.extractedText.format' to be 'html'");

        options = validRecordOptions();
        options.remove(Options.WRITE_EXTRACTED_TEXT_DOCUMENT_TYPE);
        assertValidationError(options, "requires 'spark.marklogic.write.extractedText.documentType' to be 'xml'");

        options = validRecordOptions();
        options.put(Options.WRITE_SPLITTER_XPATH, "//p");
        assertValidationError(options, "cannot be combined with splitter options");

        options = validRecordOptions();
        options.put(Options.WRITE_SPLITTER_JSON_POINTERS, "");
        assertValidationError(options, "cannot be combined with splitter options");

        options = validRecordOptions();
        options.put(Options.WRITE_SPLITTER_TEXT, "true");
        assertValidationError(options, "cannot be combined with splitter options");
    }

    @Test
    void classifierRequiresEnvelope() {
        Map<String, String> options = validRecordOptions();
        options.put(Options.WRITE_CLASSIFIER_HOST, "localhost");
        assertValidationError(options,
            "'spark.marklogic.write.extractedText.aggregate.element' requires 'spark.marklogic.write.extractedText.aggregate.envelope' when a classifier is configured");

        options.put(Options.WRITE_EXTRACTED_TEXT_AGGREGATE_ENVELOPE, "true");
        assertDoesNotThrow(() -> writeContext(options).validateExtractedTextRecordOptions());
    }

    @Test
    void eachRecordIsClassifiedIndividually() throws Exception {
        Map<String, String> options = validRecordOptions();
        options.put(Options.WRITE_EXTRACTED_TEXT, "true");
        options.put(Options.WRITE_EXTRACTED_TEXT_AGGREGATE_ELEMENT, "p");
        options.put(Options.WRITE_EXTRACTED_TEXT_AGGREGATE_ENVELOPE, "true");
        options.put(Options.WRITE_EXTRACTED_TEXT_DROP_SOURCE, "true");
        options.put(ClassifierTestUtil.MOCK_RESPONSE_OPTION, mockResponseWithMarkers(2));

        DocumentInputs inputs = new DocumentInputs("/hello.docx",
            new BytesHandle(Files.readAllBytes(Path.of("src/test/resources/extraction-files/hello-world.docx"))), null, null);
        try (DocumentPipeline pipeline = DocumentPipelineFactory.newDocumentPipeline(new Context(options))) {
            pipeline.processDocuments(List.of(inputs));
        }

        assertEquals(1, MockSemaphoreProxy.getTimesInvoked(), "Records are classified in a single multi-article request");
        assertNull(inputs.getDocumentClassification(), "The source document is not classified when records are produced");
        assertEquals(2, inputs.getExtractedRecords().size());

        WriteContext context = new WriteContext(new StructType().add("test", DataTypes.StringType), options);
        context.validateExtractedTextRecordOptions();
        List<DocumentWriteOperation> docs = new ArrayList<>(new DocBuilderFactory()
            .withUriMaker((uri, values) -> uri)
            .withExtractedTextDocumentType("xml")
            .withExtractedTextFormat("html")
            .withExtractedTextDropSource(true)
            .withExtractedTextRecords(XHTML_NS, "p", null, null, true)
            .newDocBuilder()
            .buildDocuments(inputs));

        assertEquals(2, docs.size());
        for (int i = 0; i < docs.size(); i++) {
            Element envelope = root(docs.get(i));
            Element classification = (Element) envelope.getElementsByTagNameNS(Util.DEFAULT_XML_NAMESPACE, "classification").item(0);
            assertNotNull(classification, "Each record envelope has its own classification");
            Element marker = (Element) classification.getElementsByTagName("SYSTEM").item(0);
            assertEquals("record-" + (i + 1), marker.getAttribute("value"), "Each record receives the classification of its own article");
        }
        assertEquals("Hello world.", child(root(docs.get(0)), "content").getTextContent());
    }

    @Test
    void sourceDocumentIsClassifiedWhenNoRecordsAreProduced() throws Exception {
        Map<String, String> options = validRecordOptions();
        options.put(Options.WRITE_EXTRACTED_TEXT, "true");
        options.put(Options.WRITE_EXTRACTED_TEXT_AGGREGATE_ELEMENT, "no-such-element");
        options.put(Options.WRITE_EXTRACTED_TEXT_AGGREGATE_ENVELOPE, "true");
        options.put(ClassifierTestUtil.MOCK_RESPONSE_OPTION, mockResponseWithMarkers(1));

        DocumentInputs inputs = new DocumentInputs("/hello.docx",
            new BytesHandle(Files.readAllBytes(Path.of("src/test/resources/extraction-files/hello-world.docx"))), null, null);
        try (DocumentPipeline pipeline = DocumentPipelineFactory.newDocumentPipeline(new Context(options))) {
            pipeline.processDocuments(List.of(inputs));
        }
        assertTrue(inputs.getExtractedRecords().isEmpty());
        assertNotNull(inputs.getDocumentClassification());

        List<String> uris = new DocBuilderFactory()
            .withUriMaker((uri, values) -> uri)
            .withExtractedTextDocumentType("xml")
            .withExtractedTextFormat("html")
            .withExtractedTextRecords(XHTML_NS, "no-such-element", null, null, true)
            .newDocBuilder()
            .buildDocuments(inputs).stream().map(DocumentWriteOperation::getUri).toList();
        assertEquals(List.of("/hello.docx", "/hello.docx-extracted-text.xml"), uris);
    }

    private String mockResponseWithMarkers(int articleCount) {
        StringBuilder response = new StringBuilder("<response><STRUCTUREDDOCUMENT>");
        for (int i = 1; i <= articleCount; i++) {
            response.append("<ARTICLE><SYSTEM name=\"marker\" value=\"record-").append(i).append("\"/></ARTICLE>");
        }
        return response.append("</STRUCTUREDDOCUMENT></response>").toString();
    }

    @Test
    void splitterSettingsThatDoNotEnableSplittingAreAllowed() {
        Map<String, String> options = validRecordOptions();
        // Flux always sends a default of 1 for this option.
        options.put(Options.WRITE_SPLITTER_SIDECAR_MAX_CHUNKS, "1");
        options.put(Options.WRITE_SPLITTER_SIDECAR_DOCUMENT_TYPE, "XML");
        options.put(Options.WRITE_SPLITTER_TEXT, "false");
        assertDoesNotThrow(() -> writeContext(options).validateExtractedTextRecordOptions());
    }

    private Map<String, String> validRecordOptions() {
        Map<String, String> options = new HashMap<>();
        options.put(Options.WRITE_EXTRACTED_TEXT_AGGREGATE_ELEMENT, "div");
        options.put(Options.WRITE_EXTRACTED_TEXT_FORMAT, "HTML");
        options.put(Options.WRITE_EXTRACTED_TEXT_DOCUMENT_TYPE, "XML");
        return options;
    }

    private void assertValidationError(Map<String, String> options, String expectedMessage) {
        WriteContext context = writeContext(options);
        ConnectorException ex = assertThrows(ConnectorException.class, context::validateExtractedTextRecordOptions);
        assertTrue(ex.getMessage().contains(expectedMessage), ex.getMessage());
    }

    private WriteContext writeContext(Map<String, String> options) {
        return new WriteContext(new StructType().add("test", DataTypes.StringType), options);
    }

    private DocBuilder newBuilder(String attributeFilter, String uriAttribute, boolean envelope, boolean dropSource) {
        return new DocBuilderFactory()
            .withUriMaker((uri, values) -> uri)
            .withExtractedTextDocumentType("xml")
            .withExtractedTextFormat("html")
            .withExtractedTextDropSource(dropSource)
            .withExtractedTextRecords(XHTML_NS, "div", attributeFilter, uriAttribute, envelope)
            .newDocBuilder();
    }

    private DocumentInputs inputsWithExtractedText(String extractedText) {
        DocumentInputs inputs = new DocumentInputs("/test.xlsx", new BytesHandle(new byte[]{1}), null, null);
        inputs.setExtractedText(extractedText);
        return inputs;
    }

    private Element root(DocumentWriteOperation doc) {
        Document document = ((DOMHandle) doc.getContent()).get();
        return document.getDocumentElement();
    }

    private Element child(Element parent, String localName) {
        return (Element) parent.getElementsByTagNameNS(Util.DEFAULT_XML_NAMESPACE, localName).item(0);
    }
}
