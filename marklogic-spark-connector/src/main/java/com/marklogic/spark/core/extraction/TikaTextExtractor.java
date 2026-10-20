/*
 * Copyright (c) 2023-2025 Progress Software Corporation and/or its subsidiaries or affiliates. All Rights Reserved.
 */
package com.marklogic.spark.core.extraction;

import com.marklogic.spark.ConnectorException;
import com.marklogic.spark.Util;
import com.marklogic.spark.core.DocumentInputs;
import org.apache.tika.Tika;
import org.apache.tika.exception.TikaException;
import org.apache.tika.exception.ZeroByteFileException;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.parser.Parser;
import org.apache.tika.sax.ToXMLContentHandler;
import org.xml.sax.SAXException;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

public class TikaTextExtractor implements TextExtractor {

    // Tika can be configured via environment variables, so we may not need to offer any dedicated configuration support.
    // See https://tika.apache.org/3.1.0/configuring.html .
    private final Tika tika = new Tika();
    private final boolean html;

    public TikaTextExtractor() {
        this("text");
    }

    public TikaTextExtractor(String format) {
        if (!"text".equalsIgnoreCase(format) && !"html".equalsIgnoreCase(format)) {
            throw new ConnectorException("Invalid extracted text format: " + format + "; expected 'text' or 'html'.");
        }
        this.html = "html".equalsIgnoreCase(format);
    }

    @Override
    public Optional<ExtractionResult> extractText(DocumentInputs inputs) {
        byte[] contentAsBytes = inputs.getContentAsBytes();
        if (contentAsBytes == null) {
            return Optional.empty();
        }

        try (ByteArrayInputStream stream = new ByteArrayInputStream(contentAsBytes)) {
            Metadata metadata = new Metadata();
            String extractedText;
            if (html) {
                // No write limit: truncating would produce malformed XHTML, so the full document is always returned.
                ToXMLContentHandler handler = new ToXMLContentHandler();
                ParseContext context = new ParseContext();
                context.set(Parser.class, tika.getParser());
                tika.getParser().parse(stream, handler, metadata, context);
                extractedText = handler.toString();
            } else {
                extractedText = tika.parseToString(stream, metadata);
            }
            // Retain the order of these while dropping known keys that we know are just noise.
            Map<String, String> map = new LinkedHashMap<>();
            for (String name : metadata.names()) {
                if (!name.equals("pdf:unmappedUnicodeCharsPerPage")) {
                    map.put(name, metadata.get(name));
                }
            }
            return Optional.of(new ExtractionResult(extractedText, map));
        } catch (ZeroByteFileException ex) {
            // Tika explodes on this, but for us, we just don't return any extracted text.
            if (Util.MAIN_LOGGER.isDebugEnabled()) {
                Util.MAIN_LOGGER.debug("Document has empty content, so not extracting text; URI: {}", inputs.getInitialUri());
            }
            return Optional.empty();
        } catch (IOException | TikaException | SAXException e) {
            throw new ConnectorException(String.format("Unable to extract text; URI: %s; cause: %s",
                inputs.getInitialUri(), e.getMessage()), e);
        }
    }
}
