/*
 * Copyright (c) 2026 Progress Software Corporation and/or its subsidiaries or affiliates. All Rights Reserved.
 */
package com.marklogic.spark.core.classifier;

import com.marklogic.spark.dom.DOMHelper;
import org.junit.jupiter.api.Test;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class SemaphoreArticleMetadataTest {

    @Test
    void metadataIsSentAsMetaElementsInEachArticle() {
        CapturingProxy proxy = new CapturingProxy();
        List<TestContent> contents = List.of(
            new TestContent("First text", metadata("file:/data/a.xlsx", "1")),
            new TestContent("Second text", metadata("file:/data/b.xlsx", "2"))
        );
        new SemaphoreTextClassifier(proxy, "UTF-8", 10, true).classifyChunks(new ArrayList<>(contents));

        String request = proxy.requests.get(0);
        assertTrue(request.startsWith("<?xml"), "Semaphore requires the XML declaration");
        Document doc = new DOMHelper(null).parseXmlString(request, null);
        NodeList articles = doc.getElementsByTagName("ARTICLE");
        assertEquals(2, articles.getLength());

        Element first = (Element) articles.item(0);
        NodeList metas = first.getElementsByTagName("META");
        assertEquals(3, metas.getLength());
        assertEquals("source-path", ((Element) metas.item(0)).getAttribute("name"));
        assertEquals("file:/data/a.xlsx", ((Element) metas.item(0)).getAttribute("value"));
        assertEquals("a.xlsx", ((Element) metas.item(1)).getAttribute("value"));
        assertEquals("1", ((Element) metas.item(2)).getAttribute("value"));
        assertEquals("First text", first.getTextContent(), "META elements add no text to the article");
        assertEquals("First text", first.getLastChild().getNodeValue());

        assertEquals("b.xlsx", ((Element) ((Element) articles.item(1)).getElementsByTagName("META").item(1)).getAttribute("value"));
        assertNotNull(contents.get(1).classification, "Classifications are still matched to articles by position");
    }

    @Test
    void metadataIsNotSentByDefault() {
        CapturingProxy proxy = new CapturingProxy();
        new SemaphoreTextClassifier(proxy, "UTF-8", 10)
            .classifyChunks(new ArrayList<>(List.of(new TestContent("Text", metadata("file:/data/a.xlsx", "1")))));
        Document doc = new DOMHelper(null).parseXmlString(proxy.requests.get(0), null);
        assertEquals(0, doc.getElementsByTagName("META").getLength());
        assertEquals("Text", doc.getElementsByTagName("ARTICLE").item(0).getTextContent());
    }

    @Test
    void nullValuesAreSkipped() {
        CapturingProxy proxy = new CapturingProxy();
        Map<String, String> metadata = new LinkedHashMap<>();
        metadata.put("source-path", null);
        metadata.put("record-index", "1");
        new SemaphoreTextClassifier(proxy, "UTF-8", 10, true)
            .classifyChunks(new ArrayList<>(List.of(new TestContent("Text", metadata))));
        Document doc = new DOMHelper(null).parseXmlString(proxy.requests.get(0), null);
        NodeList metas = doc.getElementsByTagName("META");
        assertEquals(1, metas.getLength());
        assertEquals("record-index", ((Element) metas.item(0)).getAttribute("name"));
    }

    private static Map<String, String> metadata(String sourcePath, String index) {
        Map<String, String> metadata = new LinkedHashMap<>();
        metadata.put("source-path", sourcePath);
        metadata.put("file-name", sourcePath.substring(sourcePath.lastIndexOf('/') + 1));
        metadata.put("record-index", index);
        return metadata;
    }

    private static class TestContent implements TextClassifier.ClassifiableContent {
        private final String text;
        private final Map<String, String> metadata;
        private byte[] classification;

        TestContent(String text, Map<String, String> metadata) {
            this.text = text;
            this.metadata = metadata;
        }

        @Override
        public String getTextToClassify() {
            return text;
        }

        @Override
        public void addClassification(byte[] classification) {
            this.classification = classification;
        }

        @Override
        public Map<String, String> getMetadata() {
            return metadata;
        }
    }

    /**
     * Captures each multi-article request and returns one article per article in the request.
     */
    private static class CapturingProxy implements SemaphoreProxy {
        final List<String> requests = new ArrayList<>();

        @Override
        public byte[] classifyDocument(byte[] content, String uri) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Document classifyArticles(byte[] multiArticleDocumentBytes) {
            String request = new String(multiArticleDocumentBytes, StandardCharsets.UTF_8);
            requests.add(request);
            int count = new DOMHelper(null).parseXmlString(request, null).getElementsByTagName("ARTICLE").getLength();
            String response = "<response><STRUCTUREDDOCUMENT>" +
                "<ARTICLE><SYSTEM name=\"DeterminedLanguage\" value=\"english\"/></ARTICLE>".repeat(count) +
                "</STRUCTUREDDOCUMENT></response>";
            return new DOMHelper(null).parseXmlString(response, null);
        }

        @Override
        public void close() {
            // Nothing to close.
        }
    }
}
