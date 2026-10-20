/*
 * Copyright (c) 2026 Progress Software Corporation and/or its subsidiaries or affiliates. All Rights Reserved.
 */
package com.marklogic.spark.core.extraction;

/**
 * A record split from extracted XHTML, along with an optional attribute value for building its URI and its optional
 * classification.
 *
 * @since 3.1.3
 */
public class ExtractedRecord {

    private final String content;
    private final String uriValue;
    private byte[] classification;

    public ExtractedRecord(String content, String uriValue) {
        this.content = content;
        this.uriValue = uriValue;
    }

    /**
     * @return the serialized XML of the record element
     */
    public String getContent() {
        return content;
    }

    /**
     * @return the value of the URI attribute on the record element, or null if no URI attribute was configured or the
     * element does not have the attribute
     */
    public String getUriValue() {
        return uriValue;
    }

    public byte[] getClassification() {
        return classification;
    }

    public void setClassification(byte[] classification) {
        this.classification = classification;
    }
}
