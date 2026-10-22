/*
 * Copyright (c) 2023-2025 Progress Software Corporation and/or its subsidiaries or affiliates. All Rights Reserved.
 */
package com.marklogic.spark.writer;

import com.marklogic.client.io.DocumentMetadataHandle;
import com.marklogic.client.io.Format;
import com.marklogic.spark.core.extraction.ExtractedTextRecordSplitter;
import com.marklogic.spark.core.splitter.ChunkAssembler;
import com.marklogic.spark.Util;

/**
 * This is intended to migrate to java-client-api and likely just be a Builder class on DocumentWriteOperation.
 */
class DocBuilderFactory {

    private DocumentMetadataHandle metadata;
    private DocumentMetadataHandle extractedTextMetadata;
    private DocBuilder.UriMaker uriMaker;
    private Format extractedTextFormat;
    private boolean extractedTextDropSource;
    private boolean extractedTextHtml;
    private ExtractedTextRecordSplitter extractedTextRecordSplitter;
    private boolean extractedTextRecordEnvelope;
    private String extractedTextRecordCollections;
    private String extractedTextRecordPermissions;
    private boolean extractedTextRecordKeepDocument;
    private ChunkAssembler chunkAssembler;

    DocBuilderFactory() {
        this.metadata = new DocumentMetadataHandle();
    }

    DocBuilder newDocBuilder() {
        DocBuilder.ExtractedTextConfig extractedTextConfig = new DocBuilder.ExtractedTextConfig(extractedTextFormat,
            newExtractedTextMetadata(), extractedTextDropSource, extractedTextHtml, extractedTextRecordSplitter,
            extractedTextRecordEnvelope, newRecordMetadata(), extractedTextRecordKeepDocument);
        return new DocBuilder(uriMaker, metadata, extractedTextConfig, chunkAssembler);
    }

    /**
     * When the full extracted text document is kept alongside records, it defaults to the source document collections
     * and permissions - like the records - unless extracted text collections or permissions are specified. Otherwise,
     * the extracted text document does not inherit the source document collections.
     */
    private DocumentMetadataHandle newExtractedTextMetadata() {
        if (extractedTextRecordSplitter == null || !extractedTextRecordKeepDocument) {
            return extractedTextMetadata;
        }
        DocumentMetadataHandle documentMetadata = new DocumentMetadataHandle();
        boolean hasCollections = extractedTextMetadata != null && !extractedTextMetadata.getCollections().isEmpty();
        boolean hasPermissions = extractedTextMetadata != null && !extractedTextMetadata.getPermissions().isEmpty();
        documentMetadata.getCollections().addAll(hasCollections ? extractedTextMetadata.getCollections() : metadata.getCollections());
        documentMetadata.getPermissions().putAll(hasPermissions ? extractedTextMetadata.getPermissions() : metadata.getPermissions());
        return documentMetadata;
    }

    /**
     * Records default to the collections and permissions of the source document, as specified by the user.
     */
    private DocumentMetadataHandle newRecordMetadata() {
        if (extractedTextRecordSplitter == null) {
            return null;
        }
        DocumentMetadataHandle recordMetadata = new DocumentMetadataHandle();
        if (extractedTextRecordCollections != null && !extractedTextRecordCollections.trim().isEmpty()) {
            recordMetadata.withCollections(extractedTextRecordCollections.split(","));
        } else {
            recordMetadata.getCollections().addAll(metadata.getCollections());
        }
        if (extractedTextRecordPermissions != null && !extractedTextRecordPermissions.trim().isEmpty()) {
            Util.addPermissionsFromDelimitedString(recordMetadata.getPermissions(), extractedTextRecordPermissions);
        } else {
            recordMetadata.getPermissions().putAll(metadata.getPermissions());
        }
        return recordMetadata;
    }

    DocBuilderFactory withCollections(String collections) {
        if (collections != null && !collections.trim().isEmpty()) {
            metadata.withCollections(collections.split(","));
        }
        return this;
    }

    DocBuilderFactory withPermissions(String permissionsString) {
        com.marklogic.spark.Util.addPermissionsFromDelimitedString(metadata.getPermissions(), permissionsString);
        return this;
    }

    DocBuilderFactory withMetadataValue(String key, String value) {
        metadata.withMetadataValue(key, value);
        return this;
    }

    DocBuilderFactory withDocumentProperty(String name, Object value) {
        metadata.withProperty(name, value);
        return this;
    }

    DocBuilderFactory withUriMaker(DocBuilder.UriMaker uriMaker) {
        this.uriMaker = uriMaker;
        return this;
    }

    DocBuilderFactory withExtractedTextDocumentType(String extractedTextDocumentType) {
        if (extractedTextDocumentType != null && !extractedTextDocumentType.trim().isEmpty()) {
            this.extractedTextFormat = "xml".equalsIgnoreCase(extractedTextDocumentType) ? Format.XML : Format.JSON;
        }
        return this;
    }

    DocBuilderFactory withExtractedTextCollections(String collections) {
        if (collections != null && !collections.trim().isEmpty()) {
            if (extractedTextMetadata == null) {
                extractedTextMetadata = new DocumentMetadataHandle();
            }
            extractedTextMetadata.withCollections(collections.split(","));
        }
        return this;
    }

    DocBuilderFactory withExtractedTextPermissions(String permissionsString) {
        if (permissionsString != null && !permissionsString.trim().isEmpty()) {
            if (extractedTextMetadata == null) {
                extractedTextMetadata = new DocumentMetadataHandle();
            }
            Util.addPermissionsFromDelimitedString(extractedTextMetadata.getPermissions(), permissionsString);
        }
        return this;
    }

    DocBuilderFactory withExtractedTextDropSource(boolean extractedTextDropSource) {
        this.extractedTextDropSource = extractedTextDropSource;
        return this;
    }

    DocBuilderFactory withExtractedTextFormat(String extractedTextFormat) {
        this.extractedTextHtml = "html".equalsIgnoreCase(extractedTextFormat);
        return this;
    }

    /**
     * @param element         if null or empty, extracted text is not split into records
     * @param attributeFilter optional, of the form name=value
     * @param uriAttribute    optional attribute whose value is used in each record URI instead of its position
     */
    DocBuilderFactory withExtractedTextRecords(String namespace, String element, String attributeFilter, String uriAttribute, boolean envelope) {
        if (element != null && !element.trim().isEmpty()) {
            this.extractedTextRecordSplitter = new ExtractedTextRecordSplitter(namespace, element, attributeFilter, uriAttribute);
            this.extractedTextRecordEnvelope = envelope;
        }
        return this;
    }

    /**
     * @param collections optional comma-delimited collections; defaults to the source document collections
     * @param permissions optional comma-delimited roles and capabilities; defaults to the source document permissions
     * @param keepDocument whether to also write the full extracted text document
     */
    DocBuilderFactory withExtractedTextRecordOutput(String collections, String permissions, boolean keepDocument) {
        this.extractedTextRecordCollections = collections;
        this.extractedTextRecordPermissions = permissions;
        this.extractedTextRecordKeepDocument = keepDocument;
        return this;
    }

    DocBuilderFactory withChunkAssembler(ChunkAssembler chunkAssembler) {
        this.chunkAssembler = chunkAssembler;
        return this;
    }
}
