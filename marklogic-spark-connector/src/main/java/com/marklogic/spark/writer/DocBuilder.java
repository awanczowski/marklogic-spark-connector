/*
 * Copyright (c) 2023-2026 Progress Software Corporation and/or its subsidiaries or affiliates. All Rights Reserved.
 */
package com.marklogic.spark.writer;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import com.marklogic.client.document.DocumentWriteOperation;
import com.marklogic.client.impl.DocumentWriteOperationImpl;
import com.marklogic.client.io.DOMHandle;
import com.marklogic.client.io.DocumentMetadataHandle;
import com.marklogic.client.io.Format;
import com.marklogic.client.io.JacksonHandle;
import com.marklogic.client.io.marker.AbstractWriteHandle;
import com.marklogic.spark.ConnectorException;
import com.marklogic.spark.Util;
import com.marklogic.spark.core.DocumentInputs;
import com.marklogic.spark.core.extraction.ExtractedRecord;
import com.marklogic.spark.core.extraction.ExtractedTextRecordSplitter;
import com.marklogic.spark.core.extraction.ExtractionUtil;
import com.marklogic.spark.core.splitter.ChunkAssembler;
import com.marklogic.spark.dom.DOMHelper;
import com.marklogic.spark.dom.NamespaceContextFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import javax.xml.parsers.DocumentBuilder;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.*;
import java.util.function.Consumer;

/**
 * Knows how to build instances of {@code DocumentWriteOperation} based on the data in {@code DocBuilder.DocumentInputs}.
 * The latter is expected to contain data read from a Spark row, normalized into a standard set of inputs regardless
 * of the schema of the Spark row.
 */
public class DocBuilder {

    public interface UriMaker {
        String makeURI(String initialUri, JsonNode uriTemplateValues);
    }

    public static class ExtractedTextConfig {
        private final Format format;
        private final DocumentMetadataHandle metadata;
        private final boolean dropSource;
        private final boolean html;
        private final ExtractedTextRecordSplitter recordSplitter;
        private final boolean recordEnvelope;
        private final DocumentMetadataHandle recordMetadata;
        private final boolean recordKeepDocument;

        public ExtractedTextConfig(Format format, DocumentMetadataHandle metadata, boolean dropSource) {
            this(format, metadata, dropSource, false);
        }

        public ExtractedTextConfig(Format format, DocumentMetadataHandle metadata, boolean dropSource, boolean html) {
            this(format, metadata, dropSource, html, null, false, null, false);
        }

        /**
         * @param recordMetadata     collections and permissions for each record
         * @param recordKeepDocument whether to write the full extracted text document in addition to the records
         */
        ExtractedTextConfig(Format format, DocumentMetadataHandle metadata, boolean dropSource, boolean html,
                            ExtractedTextRecordSplitter recordSplitter, boolean recordEnvelope,
                            DocumentMetadataHandle recordMetadata, boolean recordKeepDocument) {
            this.format = format;
            this.metadata = metadata;
            this.dropSource = dropSource;
            this.html = html;
            this.recordSplitter = recordSplitter;
            this.recordEnvelope = recordEnvelope;
            this.recordMetadata = recordMetadata;
            this.recordKeepDocument = recordKeepDocument;
        }
    }

    // Metadata keys that link each record to its source document and, optionally, the full extracted text document.
    static final String RECORD_SOURCE_URI_KEY = "source-uri";
    static final String RECORD_EXTRACTED_TEXT_URI_KEY = "extracted-text-uri";

    private final UriMaker uriMaker;
    private final DocumentMetadataHandle metadataFromOptions;
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final DOMHelper domHelper = new DOMHelper(NamespaceContextFactory.makeDefaultNamespaceContext());
    private final ExtractedTextConfig extractedTextConfig;
    private final ChunkAssembler chunkAssembler;
    private DocumentBuilder documentBuilder;
    private final XmlMapper xmlMapper;

    DocBuilder(UriMaker uriMaker, DocumentMetadataHandle metadata, ExtractedTextConfig extractedTextConfig, ChunkAssembler chunkAssembler) {
        this.uriMaker = uriMaker;
        this.metadataFromOptions = metadata;
        this.extractedTextConfig = extractedTextConfig;
        this.chunkAssembler = chunkAssembler;
        xmlMapper = new XmlMapper();
    }

    /**
     * @param inputs set of inputs constructed from a single Spark row.
     * @return one or more documents to write to MarkLogic, based on the inputs object.
     */
    Collection<DocumentWriteOperation> buildDocuments(DocumentInputs inputs) {
        // Using a map to ensure we don't have 2+ documents with the same URI. Some operations below will want to
        // overwrite an entry in a map, which is perfectly fine.
        final Map<String, DocumentWriteOperation> documents = new LinkedHashMap<>();
        DocumentWriteOperation mainDocument = buildMainDocument(inputs);
        documents.put(mainDocument.getUri(), mainDocument);

        List<DocumentWriteOperation> extractedTextDocs = buildExtractedTextDocuments(inputs, mainDocument);
        extractedTextDocs.forEach(doc -> documents.put(doc.getUri(), doc));

        // Chunks are only associated with the single extracted text document; records replace that document and
        // cannot be combined with splitting.
        DocumentWriteOperation extractedTextDoc = extractedTextDocs.size() == 1 ? extractedTextDocs.get(0) : null;
        buildChunkDocuments(inputs, mainDocument, extractedTextDoc).forEach(doc -> documents.put(doc.getUri(), doc));

        if (!extractedTextDocs.isEmpty() && extractedTextConfig.dropSource) {
            documents.remove(mainDocument.getUri());
        }
        return documents.values();
    }

    private DocumentWriteOperation buildMainDocument(DocumentInputs inputs) {
        final String sourceUri = uriMaker.makeURI(inputs.getInitialUri(), inputs.getColumnValuesForUriTemplate());
        final String graph = inputs.getGraph();
        final DocumentMetadataHandle metadataFromRow = inputs.getInitialMetadata();

        AbstractWriteHandle content = inputs.getContent();
        if (inputs.getDocumentClassification() != null && inputs.getExtractedText() == null) {
            content = addClassificationToMainDocument(inputs, sourceUri);
        }

        DocumentMetadataHandle sourceMetadata = metadataFromRow;
        if (sourceMetadata != null) {
            // If the row contains metadata, use it, but first override it based on the metadata specified by user options.
            overrideMetadataFromRowWithMetadataFromOptions(sourceMetadata);
            if (graph != null) {
                sourceMetadata.getCollections().add(graph);
            }
            return new DocumentWriteOperationImpl(sourceUri, sourceMetadata, content);
        }
        // If the row doesn't contain metadata, use the metadata specified by user options. We need to be careful
        // not to modify that object though, as it will be reused on subsequent calls.
        sourceMetadata = metadataFromOptions;
        if (graph != null && !sourceMetadata.getCollections().contains(graph)) {
            sourceMetadata = newMetadataWithGraph(graph);
        }
        return new DocumentWriteOperationImpl(sourceUri, sourceMetadata, content);
    }

    private AbstractWriteHandle addClassificationToMainDocument(DocumentInputs inputs, String sourceUri) {
        AbstractWriteHandle content = inputs.getContent();
        final Format sourceDocumentFormat = Util.determineSourceDocumentFormat(content, sourceUri);
        final byte[] classification = inputs.getDocumentClassification();
        if (Format.XML.equals(sourceDocumentFormat)) {
            Document originalDoc = domHelper.extractDocument(content, inputs.getInitialUri());
            addClassificationToXmlDocument(originalDoc, inputs.getInitialUri(), classification);
            return new DOMHandle(originalDoc);
        } else if (Format.JSON.equals(sourceDocumentFormat)) {
            JsonNode doc = Util.getJsonFromHandle(content);
            addClassificationToJsonDocument((ObjectNode) doc, inputs.getInitialUri(), classification);
            return new JacksonHandle(doc);
        }
        Util.MAIN_LOGGER.warn("Cannot add classification to document with URI {}; document is neither JSON nor XML.", sourceUri);
        return content;
    }

    private void addClassificationToJsonDocument(ObjectNode jsonDocument, String uri, byte[] classification) {
        try {
            JsonNode classificationData = xmlMapper.readTree(classification);
            jsonDocument.set("classification", classificationData);
        } catch (IOException e) {
            throw new ConnectorException(String.format("Unable to classify data from document with URI: %s; cause: %s", uri, e.getMessage()), e);
        }
    }

    public void addClassificationToXmlDocument(Document document, String uri, byte[] classification) {
        try {
            if (documentBuilder == null) {
                documentBuilder = DOMHelper.newSecureDocumentBuilderFactory().newDocumentBuilder();
            }
            Document classificationXml = documentBuilder.parse(new ByteArrayInputStream(classification));
            Node classificationNode = document.createElementNS(Util.DEFAULT_XML_NAMESPACE, "classification");
            NodeList articleChildNodes = classificationXml.getDocumentElement().getChildNodes();
            for (int i = 0; i < articleChildNodes.getLength(); i++) {
                Node importedChildNode = document.importNode(articleChildNodes.item(i), true);
                classificationNode.appendChild(importedChildNode);
            }
            document.getFirstChild().appendChild(classificationNode);
        } catch (Exception e) {
            throw new ConnectorException(String.format("Unable to classify data from document with URI: %s; cause: %s", uri, e.getMessage()), e);
        }
    }

    private List<DocumentWriteOperation> buildExtractedTextDocuments(DocumentInputs inputs, DocumentWriteOperation mainDocument) {
        if (inputs.getExtractedText() == null) {
            return List.of();
        }
        String sourceUri = mainDocument.getUri();

        DocumentMetadataHandle metadataToUse = this.extractedTextConfig.metadata != null ?
            this.extractedTextConfig.metadata :
            (DocumentMetadataHandle) mainDocument.getMetadata();

        if (Format.XML.equals(this.extractedTextConfig.format)) {
            if (this.extractedTextConfig.recordSplitter != null) {
                final boolean keepDocument = this.extractedTextConfig.recordKeepDocument;
                List<DocumentWriteOperation> records = buildExtractedRecordDocuments(sourceUri, inputs,
                    keepDocument ? makeExtractedXmlUri(sourceUri) : null);
                if (!records.isEmpty()) {
                    if (!keepDocument) {
                        return records;
                    }
                    List<DocumentWriteOperation> documents = new ArrayList<>(records.size() + 1);
                    documents.add(buildExtractedXmlDocument(sourceUri, inputs, metadataToUse));
                    documents.addAll(records);
                    return documents;
                }
            }
            return List.of(buildExtractedXmlDocument(sourceUri, inputs, metadataToUse));
        }
        return List.of(buildExtractedJsonDocument(sourceUri, inputs, metadataToUse));
    }

    /**
     * Builds one document per record. Records are normally split - and optionally classified - by the document
     * pipeline; if the pipeline did not split the extracted text, it is split here. Returns an empty list - causing the
     * single extracted text document to be written instead - if the XHTML cannot be split or no element matches.
     */
    private List<DocumentWriteOperation> buildExtractedRecordDocuments(String sourceUri, DocumentInputs inputs, String extractedTextUri) {
        final ExtractedTextRecordSplitter splitter = this.extractedTextConfig.recordSplitter;
        List<ExtractedRecord> records = inputs.getExtractedRecords();
        if (records == null) {
            try {
                records = splitter.split(inputs.getExtractedText());
            } catch (ConnectorException e) {
                Util.MAIN_LOGGER.warn("Unable to split extracted text into records for document with URI: {}; writing a single extracted text document instead; cause: {}",
                    sourceUri, e.getMessage());
                return List.of();
            }
            if (records.isEmpty()) {
                Util.MAIN_LOGGER.warn("No '{}' elements matched in extracted text for document with URI: {}; writing a single extracted text document instead.",
                    splitter.getElement(), sourceUri);
            }
        }
        if (records.isEmpty()) {
            return List.of();
        }

        List<DocumentWriteOperation> documents = new ArrayList<>(records.size());
        Set<String> uris = new HashSet<>();
        for (int i = 0; i < records.size(); i++) {
            final int recordIndex = i + 1;
            final ExtractedRecord extractedRecord = records.get(i);
            final String uri = makeRecordUri(sourceUri, recordIndex, extractedRecord.getUriValue());
            if (!uris.add(uri)) {
                throw new ConnectorException(String.format(
                    "Duplicate value '%s' for attribute '%s' in extracted text for document with URI: %s; each record must have a unique value.",
                    extractedRecord.getUriValue(), splitter.getUriAttribute(), sourceUri));
            }

            Document recordXml = domHelper.parseXmlString(extractedRecord.getContent(), sourceUri);
            Document doc = extractedTextConfig.recordEnvelope ?
                newExtractedXmlEnvelope(sourceUri, inputs, new RecordLinks(recordIndex, extractedTextUri),
                    content -> content.appendChild(content.getOwnerDocument().importNode(recordXml.getDocumentElement(), true)),
                    extractedRecord.getClassification() != null ? extractedRecord.getClassification() : inputs.getDocumentClassification()) :
                recordXml;
            documents.add(new DocumentWriteOperationImpl(uri, newRecordMetadata(sourceUri, extractedTextUri), new DOMHandle(doc)));
        }
        return documents;
    }

    /**
     * Each record gets its own metadata so that the links to its source - which are also in the envelope, if used -
     * can be queried regardless of whether the record is enveloped.
     */
    private DocumentMetadataHandle newRecordMetadata(String sourceUri, String extractedTextUri) {
        DocumentMetadataHandle base = extractedTextConfig.recordMetadata;
        DocumentMetadataHandle recordMetadata = new DocumentMetadataHandle();
        if (base != null) {
            recordMetadata.getCollections().addAll(base.getCollections());
            recordMetadata.getPermissions().putAll(base.getPermissions());
        }
        recordMetadata.getMetadataValues().put(RECORD_SOURCE_URI_KEY, sourceUri);
        if (extractedTextUri != null) {
            recordMetadata.getMetadataValues().put(RECORD_EXTRACTED_TEXT_URI_KEY, extractedTextUri);
        }
        return recordMetadata;
    }

    /**
     * @param index            1-based position of the record in the extracted text
     * @param extractedTextUri URI of the full extracted text document, if it is also written
     */
    private record RecordLinks(int index, String extractedTextUri) {
    }

    private static String makeExtractedXmlUri(String sourceUri) {
        return sourceUri + "-extracted-text.xml";
    }

    private String makeRecordUri(String sourceUri, int recordIndex, String uriValue) {
        final String uriAttribute = this.extractedTextConfig.recordSplitter.getUriAttribute();
        if (uriAttribute == null) {
            return String.format("%s-extracted-%d.xml", sourceUri, recordIndex);
        }
        if (uriValue == null || uriValue.trim().isEmpty()) {
            throw new ConnectorException(String.format(
                "No value for attribute '%s' found on record %d in extracted text for document with URI: %s.",
                uriAttribute, recordIndex, sourceUri));
        }
        return String.format("%s-extracted-%s.xml", sourceUri, uriValue.trim());
    }

    /**
     * If an instance of {@code DocumentInputs} has metadata specified (i.e. metadata from the Spark row), override it
     * with any metadata specified by the user via options.
     *
     * @param metadataFromRow
     */
    private void overrideMetadataFromRowWithMetadataFromOptions(DocumentMetadataHandle metadataFromRow) {
        if (!metadataFromOptions.getCollections().isEmpty()) {
            metadataFromRow.setCollections(metadataFromOptions.getCollections());
        }
        if (!metadataFromOptions.getPermissions().isEmpty()) {
            metadataFromRow.setPermissions(metadataFromOptions.getPermissions());
        }
        if (metadataFromOptions.getQuality() != 0) {
            metadataFromRow.setQuality(metadataFromOptions.getQuality());
        }
        if (!metadataFromOptions.getProperties().isEmpty()) {
            metadataFromRow.setProperties(metadataFromOptions.getProperties());
        }
        if (!metadataFromOptions.getMetadataValues().isEmpty()) {
            metadataFromRow.setMetadataValues(metadataFromOptions.getMetadataValues());
        }
    }

    /**
     * If a semantics graph is specified in the set of document inputs, must copy the DocumentMetadataHandle instance
     * in this class to a new DocumentMetadataHandle instance that includes the graph as a collection. This is done to
     * avoid modifying the DocumentMetadataHandle instance owned by this class which is expected to be reused for
     * many documents.
     *
     * @param graph
     * @return
     */
    private DocumentMetadataHandle newMetadataWithGraph(String graph) {
        DocumentMetadataHandle newMetadata = new DocumentMetadataHandle();
        newMetadata.getCollections().addAll(metadataFromOptions.getCollections());
        newMetadata.getPermissions().putAll(metadataFromOptions.getPermissions());
        newMetadata.setQuality(metadataFromOptions.getQuality());
        newMetadata.setProperties(metadataFromOptions.getProperties());
        newMetadata.setMetadataValues(metadataFromOptions.getMetadataValues());
        newMetadata.getCollections().add(graph);
        return newMetadata;
    }

    private DocumentWriteOperation buildExtractedJsonDocument(String sourceUri, DocumentInputs inputs, DocumentMetadataHandle sourceMetadata) {
        ObjectNode doc = objectMapper.createObjectNode();
        if (!extractedTextConfig.dropSource) {
            doc.put("source-uri", sourceUri);
        }
        doc.put("content", inputs.getExtractedText());
        if (inputs.getExtractedMetadata() != null) {
            ObjectNode node = doc.putObject("extracted-metadata");
            inputs.getExtractedMetadata().entrySet().forEach(entry -> {
                // Replacing the colon, which is not allowable in an index declaration.
                String key = entry.getKey().replace(":", "-");
                node.put(key, entry.getValue());
            });
        }
        if (inputs.getDocumentClassification() != null) {
            addClassificationToJsonDocument(doc, sourceUri, inputs.getDocumentClassification());
        }
        String uri = sourceUri + "-extracted-text.json";
        return new DocumentWriteOperationImpl(uri, sourceMetadata, new JacksonHandle(doc));
    }

    private DocumentWriteOperation buildExtractedXmlDocument(String sourceUri, DocumentInputs inputs, DocumentMetadataHandle sourceMetadata) {
        Document doc = newExtractedXmlEnvelope(sourceUri, inputs, null,
            content -> appendExtractedContent(content.getOwnerDocument(), content, sourceUri, inputs.getExtractedText()),
            inputs.getDocumentClassification());
        return new DocumentWriteOperationImpl(makeExtractedXmlUri(sourceUri), sourceMetadata, new DOMHandle(doc));
    }

    /**
     * @param recordLinks     only provided when the envelope is for a record split from the extracted text; a record
     *                        always includes its source URI, as it is a fragment of the source
     * @param contentAppender populates the "content" element
     * @param classification  optional classification to add to the envelope
     */
    private Document newExtractedXmlEnvelope(String sourceUri, DocumentInputs inputs, RecordLinks recordLinks,
                                             Consumer<Element> contentAppender, byte[] classification) {
        Document doc = domHelper.newDocument();
        Element root = doc.createElementNS(Util.DEFAULT_XML_NAMESPACE, "root");
        doc.appendChild(root);

        if (recordLinks != null || !extractedTextConfig.dropSource) {
            Element sourceElement = doc.createElementNS(Util.DEFAULT_XML_NAMESPACE, "source-uri");
            sourceElement.appendChild(doc.createTextNode(sourceUri));
            root.appendChild(sourceElement);
        }

        if (recordLinks != null) {
            if (recordLinks.extractedTextUri() != null) {
                Element extractedElement = doc.createElementNS(Util.DEFAULT_XML_NAMESPACE, RECORD_EXTRACTED_TEXT_URI_KEY);
                extractedElement.appendChild(doc.createTextNode(recordLinks.extractedTextUri()));
                root.appendChild(extractedElement);
            }
            Element indexElement = doc.createElementNS(Util.DEFAULT_XML_NAMESPACE, "record-index");
            indexElement.appendChild(doc.createTextNode(Integer.toString(recordLinks.index())));
            root.appendChild(indexElement);
        }

        Element content = doc.createElementNS(Util.DEFAULT_XML_NAMESPACE, "content");
        contentAppender.accept(content);
        root.appendChild(content);

        if (inputs.getExtractedMetadata() != null && !inputs.getExtractedMetadata().isEmpty()) {
            Element metadata = doc.createElementNS(Util.DEFAULT_XML_NAMESPACE, "extracted-metadata");
            root.appendChild(metadata);
            inputs.getExtractedMetadata().entrySet().forEach(entry -> {
                try {
                    Element metadataElement = createXmlMetadataElement(doc, entry);
                    metadata.appendChild(metadataElement);
                } catch (Exception e) {
                    Util.MAIN_LOGGER.warn("Unable to convert extracted metadata into XML: {}; cause: {}", entry, e.getMessage());
                }
            });
        }

        if (classification != null) {
            addClassificationToXmlDocument(doc, sourceUri, classification);
        }
        return doc;
    }

    /**
     * When the extracted content is Tika XHTML, it is inserted as XML elements so that it can be queried as structure;
     * otherwise it is added as text. If the XHTML cannot be parsed, it falls back to text so no content is lost.
     */
    private void appendExtractedContent(Document doc, Element content, String sourceUri, String extractedText) {
        if (extractedTextConfig.html) {
            try {
                Document xhtml = domHelper.parseXmlString(extractedText, sourceUri);
                content.appendChild(doc.importNode(xhtml.getDocumentElement(), true));
                return;
            } catch (ConnectorException e) {
                Util.MAIN_LOGGER.warn("Unable to insert extracted HTML as XML for document with URI: {}; storing it as text instead; cause: {}",
                    sourceUri, e.getMessage());
            }
        }
        content.appendChild(doc.createTextNode(extractedText));
    }

    private Element createXmlMetadataElement(Document doc, Map.Entry<String, String> metadataEntry) {
        final String key = metadataEntry.getKey();

        // Ideally, the metadata entry has a recognized XML prefix that can be mapped to a useful namespace.
        // If not, we default to this local name to avoid creating an element with a colon in its name, which is not
        // allowed.
        String localName = metadataEntry.getKey().replace(":", "-");
        String namespace = Util.DEFAULT_XML_NAMESPACE;
        final int pos = key.indexOf(":");
        if (pos > -1) {
            final String prefix = key.substring(0, pos);
            String associatedNamespace = ExtractionUtil.getNamespace(prefix);
            if (associatedNamespace != null) {
                namespace = associatedNamespace;
                localName = key.substring(pos + 1).replace(":", "-");
            }
        }

        // Names such as custom document properties may contain characters that are not allowed in an XML name, like
        // spaces; those are replaced, and the original key is retained so that no information is lost.
        final String validLocalName = ExtractionUtil.toXmlLocalName(localName);
        Element metadataElement = doc.createElementNS(namespace, validLocalName);
        if (!validLocalName.equals(localName)) {
            metadataElement.setAttribute("key", ExtractionUtil.toValidXmlText(key));
        }
        metadataElement.appendChild(doc.createTextNode(ExtractionUtil.toValidXmlText(metadataEntry.getValue())));
        return metadataElement;
    }

    private List<DocumentWriteOperation> buildChunkDocuments(DocumentInputs inputs, DocumentWriteOperation mainDocument, DocumentWriteOperation extractedTextDocument) {
        List<DocumentWriteOperation> chunkDocuments = new ArrayList<>();
        if (inputs.getChunkInputsList() != null && !inputs.getChunkInputsList().isEmpty()) {
            // If there's an extracted doc, we want to use that as the source document so that the user has the option
            // of adding chunks to it.
            DocumentWriteOperation sourceDocument = extractedTextDocument != null ? extractedTextDocument : mainDocument;
            Iterator<DocumentWriteOperation> iterator = chunkAssembler.assembleChunks(
                sourceDocument,
                inputs.getChunkInputsList());
            while (iterator.hasNext()) {
                DocumentWriteOperation doc = iterator.next();
                chunkDocuments.add(doc);
            }
        }
        return chunkDocuments;
    }
}
