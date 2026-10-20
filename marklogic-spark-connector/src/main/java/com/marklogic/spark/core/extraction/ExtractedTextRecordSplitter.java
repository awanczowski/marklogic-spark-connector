/*
 * Copyright (c) 2026 Progress Software Corporation and/or its subsidiaries or affiliates. All Rights Reserved.
 */
package com.marklogic.spark.core.extraction;

import com.marklogic.client.datamovement.NodeOperation;
import com.marklogic.client.datamovement.XMLSplitter;
import com.marklogic.client.io.StringHandle;
import com.marklogic.spark.ConnectorException;
import com.marklogic.spark.Context;
import com.marklogic.spark.Options;

import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;
import java.io.IOException;
import java.io.StringReader;
import java.util.List;

/**
 * Splits extracted XHTML into records using the same streaming {@code XMLSplitter} as aggregate XML import, with
 * optional filtering on an attribute value and optional capture of an attribute value for building record URIs.
 *
 * @since 3.1.3
 */
public class ExtractedTextRecordSplitter {

    public static final String XHTML_NAMESPACE = "http://www.w3.org/1999/xhtml";

    private static final XMLInputFactory xmlInputFactory;

    static {
        xmlInputFactory = XMLInputFactory.newFactory();
        // Prevents XXE attacks; mirrors the configuration used by aggregate XML import.
        xmlInputFactory.setProperty(XMLInputFactory.IS_SUPPORTING_EXTERNAL_ENTITIES, Boolean.FALSE);
        xmlInputFactory.setProperty(XMLInputFactory.SUPPORT_DTD, false);
    }

    private final String namespace;
    private final String element;
    private final String filterAttributeName;
    private final String filterAttributeValue;
    private final String uriAttribute;

    /**
     * @return a splitter based on the extracted text aggregate options, or null if no aggregate element is configured
     */
    public static ExtractedTextRecordSplitter newInstance(Context context) {
        if (!context.hasOption(Options.WRITE_EXTRACTED_TEXT_AGGREGATE_ELEMENT)) {
            return null;
        }
        return new ExtractedTextRecordSplitter(
            context.getStringOption(Options.WRITE_EXTRACTED_TEXT_AGGREGATE_NAMESPACE, XHTML_NAMESPACE),
            context.getStringOption(Options.WRITE_EXTRACTED_TEXT_AGGREGATE_ELEMENT),
            context.getStringOption(Options.WRITE_EXTRACTED_TEXT_AGGREGATE_ATTRIBUTE),
            context.getStringOption(Options.WRITE_EXTRACTED_TEXT_AGGREGATE_URI_ATTRIBUTE));
    }

    /**
     * @param attributeFilter optional, of the form "name=value"
     * @param uriAttribute    optional name of an attribute whose value is captured for each record
     */
    public ExtractedTextRecordSplitter(String namespace, String element, String attributeFilter, String uriAttribute) {
        if (element == null || element.trim().isEmpty()) {
            throw new ConnectorException("Must specify an element name for splitting extracted text into records.");
        }
        this.namespace = namespace;
        this.element = element.trim();
        if (attributeFilter != null && !attributeFilter.trim().isEmpty()) {
            int pos = attributeFilter.indexOf('=');
            if (pos < 1) {
                throw new ConnectorException(String.format(
                    "Invalid attribute filter for splitting extracted text into records: '%s'; must be of the form name=value.",
                    attributeFilter));
            }
            this.filterAttributeName = attributeFilter.substring(0, pos).trim();
            this.filterAttributeValue = attributeFilter.substring(pos + 1);
        } else {
            this.filterAttributeName = null;
            this.filterAttributeValue = null;
        }
        this.uriAttribute = uriAttribute != null && !uriAttribute.trim().isEmpty() ? uriAttribute.trim() : null;
    }

    public String getElement() {
        return element;
    }

    public String getUriAttribute() {
        return uriAttribute;
    }

    /**
     * @return a record for each matching element in document order; nested matches remain inside their record
     * @throws ConnectorException if the XML cannot be parsed
     */
    public List<ExtractedRecord> split(String xml) {
        XMLSplitter<StringHandle> splitter = new XMLSplitter<>(new RecordVisitor());
        try {
            XMLStreamReader reader = xmlInputFactory.createXMLStreamReader(new StringReader(xml));
            return splitter.split(reader).map(handle -> {
                RecordHandle recordHandle = (RecordHandle) handle;
                return new ExtractedRecord(recordHandle.get(), recordHandle.uriValue);
            }).toList();
        } catch (XMLStreamException | IOException | RuntimeException e) {
            throw new ConnectorException(String.format("Unable to split extracted text into records; cause: %s", e.getMessage()), e);
        }
    }

    private static String findAttributeValue(XMLSplitter.StartElementReader reader, String localName) {
        for (int i = 0; i < reader.getAttributeCount(); i++) {
            if (localName.equals(reader.getAttributeLocalName(i))) {
                return reader.getAttributeValue(i);
            }
        }
        return null;
    }

    private static class RecordHandle extends StringHandle {
        private final String uriValue;

        RecordHandle(String content, String uriValue) {
            super(content);
            this.uriValue = uriValue;
        }
    }

    private class RecordVisitor extends XMLSplitter.BasicElementVisitor {

        // Set when an element is selected for processing; the splitter calls makeBufferedHandle immediately afterward.
        private String currentUriValue;

        RecordVisitor() {
            super(namespace, element);
        }

        @Override
        public NodeOperation startElement(XMLSplitter.StartElementReader reader) {
            NodeOperation operation = super.startElement(reader);
            if (operation != NodeOperation.PROCESS) {
                return operation;
            }
            if (filterAttributeName != null && !filterAttributeValue.equals(findAttributeValue(reader, filterAttributeName))) {
                return NodeOperation.DESCEND;
            }
            currentUriValue = uriAttribute != null ? findAttributeValue(reader, uriAttribute) : null;
            return NodeOperation.PROCESS;
        }

        @Override
        public StringHandle makeBufferedHandle(XMLStreamReader xmlStreamReader) {
            return new RecordHandle(serialize(xmlStreamReader), currentUriValue);
        }
    }
}
