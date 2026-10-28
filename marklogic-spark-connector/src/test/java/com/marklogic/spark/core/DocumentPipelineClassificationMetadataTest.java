/*
 * Copyright (c) 2026 Progress Software Corporation and/or its subsidiaries or affiliates. All Rights Reserved.
 */
package com.marklogic.spark.core;

import com.marklogic.client.io.BytesHandle;
import com.marklogic.spark.core.classifier.TextClassifier;
import com.marklogic.spark.core.extraction.ExtractedTextRecordSplitter;
import com.marklogic.spark.core.extraction.TikaTextExtractor;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class DocumentPipelineClassificationMetadataTest {

    @Test
    void recordsIdentifyTheirSourceToTheClassifier() throws Exception {
        CapturingClassifier classifier = new CapturingClassifier();
        DocumentPipeline pipeline = new DocumentPipeline(new TikaTextExtractor("html"), null, classifier, null, null,
            new ExtractedTextRecordSplitter(ExtractedTextRecordSplitter.XHTML_NAMESPACE, "p", null, null));

        DocumentInputs inputs = new DocumentInputs("file:/data/files/hello-world.docx",
            new BytesHandle(Files.readAllBytes(Path.of("src/test/resources/extraction-files/hello-world.docx"))), null, null);
        pipeline.processDocuments(List.of(inputs));

        assertEquals(2, classifier.contents.size());
        for (int i = 0; i < 2; i++) {
            assertEquals(Map.of(
                "source-path", "file:/data/files/hello-world.docx",
                "file-name", "hello-world.docx",
                "record-index", Integer.toString(i + 1)
            ), classifier.contents.get(i).getMetadata());
        }
        assertEquals(List.of("source-path", "file-name", "record-index"),
            new ArrayList<>(classifier.contents.get(0).getMetadata().keySet()), "Metadata is sent in a predictable order");
        assertEquals(0, classifier.documentsClassified, "The source document is not classified when records are produced");
    }

    private static class CapturingClassifier implements TextClassifier {
        final List<ClassifiableContent> contents = new ArrayList<>();
        int documentsClassified;

        @Override
        public void classifyDocument(DocumentInputs inputs) {
            documentsClassified++;
        }

        @Override
        public void classifyChunks(List<ClassifiableContent> classifiableContents) {
            contents.addAll(classifiableContents);
        }

        @Override
        public void close() {
            // Nothing to close.
        }
    }
}
