/*
 * Copyright 2023 IBM Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy
 * of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

package com.ibm.watson.modelmesh.payload;

import java.net.InetAddress;
import java.net.URI;
import java.net.UnknownHostException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLParameters;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.grpc.Metadata;
import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.base64.Base64;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link PayloadProcessor} that sends payloads to a remote service via HTTP POST.
 */
public class RemotePayloadProcessor implements PayloadProcessor {

    private final static Logger logger = LoggerFactory.getLogger(RemotePayloadProcessor.class);

    private static final ObjectMapper objectMapper = new ObjectMapper();

    private final URI uri;

    private final SSLContext sslContext;
    private final SSLParameters sslParameters;

    private final HttpClient client;

    /**
     * Validates the URI to prevent SSRF attacks by rejecting requests to private/internal networks.
     *
     * @param uri The URI to validate
     * @throws IllegalArgumentException if the URI points to a private or internal network
     */
    private static void validateUri(URI uri) {
        if (uri == null) {
            throw new IllegalArgumentException("URI cannot be null");
        }

        // Validate scheme - only http and https are allowed
        String scheme = uri.getScheme();
        if (scheme == null || (!scheme.equalsIgnoreCase("http") && !scheme.equalsIgnoreCase("https"))) {
            throw new IllegalArgumentException("URI scheme must be http or https, got: " + scheme);
        }

        String host = uri.getHost();
        if (host == null || host.isEmpty()) {
            throw new IllegalArgumentException("URI must have a valid host");
        }

        // Reject localhost and loopback addresses (string-based checks for bypasses)
        String hostLower = host.toLowerCase();

        // Check common localhost variants
        if (hostLower.equals("localhost") ||
            hostLower.equals("127.0.0.1") ||
            hostLower.equals("::1") ||
            hostLower.equals("0.0.0.0") ||
            hostLower.equals("[::]") ||
            hostLower.equals("::") ||
            hostLower.startsWith("127.") ||
            hostLower.startsWith("0x7f.")) {  // hex-encoded 127
            throw new IllegalArgumentException("URI cannot point to localhost or loopback address: " + host);
        }

        // Check for octal-encoded localhost (0177.0.0.1, etc.)
        if (hostLower.matches("0[0-7]+\\..*") || hostLower.matches(".*\\.0[0-7]+\\..*")) {
            throw new IllegalArgumentException("URI cannot use octal IP encoding: " + host);
        }

        // Try to resolve the host and perform additional validation
        // Per OWASP SSRF Prevention: Check ALL resolved IPs to prevent DNS pinning attacks
        // If the host can't be resolved, allow it to pass (the HTTP request will fail later)
        try {
            InetAddress[] allAddresses = InetAddress.getAllByName(host);

            // Validate each resolved IP address (prevents DNS pinning bypass)
            for (InetAddress addr : allAddresses) {
                byte[] addrBytes = addr.getAddress();

                // Reject private IP ranges (RFC 1918)
                if (addr.isSiteLocalAddress()) {
                    throw new IllegalArgumentException("URI resolves to private IP address: " + addr.getHostAddress());
                }

                // Reject loopback addresses
                if (addr.isLoopbackAddress()) {
                    throw new IllegalArgumentException("URI resolves to loopback address: " + addr.getHostAddress());
                }

                // Reject link-local addresses (includes AWS metadata service at 169.254.169.254)
                if (addr.isLinkLocalAddress()) {
                    throw new IllegalArgumentException("URI resolves to link-local address: " + addr.getHostAddress());
                }

                // Reject multicast addresses
                if (addr.isMulticastAddress()) {
                    throw new IllegalArgumentException("URI resolves to multicast address: " + addr.getHostAddress());
                }

                // Additional check for 0.0.0.0
                if (addr.isAnyLocalAddress()) {
                    throw new IllegalArgumentException("URI resolves to wildcard address: " + addr.getHostAddress());
                }

                // Reject IPv6 unique local addresses (RFC 4193: fc00::/7)
                if (addrBytes.length == 16 && (addrBytes[0] & 0xfe) == 0xfc) {
                    throw new IllegalArgumentException("URI resolves to IPv6 unique local address (RFC 4193): " + addr.getHostAddress());
                }
            }

        } catch (UnknownHostException e) {
            // If the host can't be resolved, allow it to continue
            // The actual HTTP request will fail later with a proper error
            logger.warn("Unable to resolve host for SSRF validation: {}", host);
        }
    }

    public RemotePayloadProcessor(URI uri) {
        this(uri, null, null);
    }

    public RemotePayloadProcessor(URI uri, SSLContext sslContext, SSLParameters sslParameters) {
        validateUri(uri);
        this.uri = uri;
        this.sslContext = sslContext;
        this.sslParameters = sslParameters;
        if (sslContext != null && sslParameters != null) {
            // OWASP SSRF Prevention: Explicitly disable HTTP redirects to prevent bypass
            this.client = HttpClient.newBuilder()
                    .sslContext(sslContext)
                    .sslParameters(sslParameters)
                    .followRedirects(HttpClient.Redirect.NEVER)
                    .build();
        } else {
            // OWASP SSRF Prevention: Explicitly disable HTTP redirects to prevent bypass
            this.client = HttpClient.newBuilder()
                    .followRedirects(HttpClient.Redirect.NEVER)
                    .build();
        }
    }

    @Override
    public boolean process(Payload payload) {
        return sendPayload(payload);
    }

    private static PayloadContent prepareContentBody(Payload payload) {
        String id = payload.getId();
        String modelId = payload.getModelId();
        String vModelId = payload.getVModelId();
        String kind = payload.getKind().toString().toLowerCase();
        ByteBuf byteBuf = payload.getData();
        String data = byteBuf != null ? encodeBinaryToString(byteBuf) : "";
        Metadata metadata = payload.getMetadata();
        Map<String, String> metadataMap = new HashMap<>();
        if (metadata != null) {
            for (String key : metadata.keys()) {
                if (key.endsWith("-bin")) {
                    byte[] bytes = metadata.get(Metadata.Key.of(key, Metadata.BINARY_BYTE_MARSHALLER));
                    metadataMap.put(key, java.util.Base64.getEncoder().encodeToString(bytes));
                } else {
                    String value = metadata.get(Metadata.Key.of(key, Metadata.ASCII_STRING_MARSHALLER));
                    metadataMap.put(key, value);
                }
            }
        }
        String status = payload.getStatus() != null ? payload.getStatus().getCode().toString() : "";
        return new PayloadContent(id, modelId, vModelId, data, kind, status, metadataMap);
    }

    private static String encodeBinaryToString(ByteBuf byteBuf) {
        ByteBuf encodedBinary = Base64.encode(byteBuf, byteBuf.readerIndex(), byteBuf.readableBytes(), false);
        //TODO custom jackson serialization for this field to avoid round-tripping to string
        return encodedBinary.toString(StandardCharsets.US_ASCII);
    }

    private boolean sendPayload(Payload payload) {
        try {
            PayloadContent payloadContent = prepareContentBody(payload);
            HttpRequest request = HttpRequest.newBuilder()
                    .uri(uri)
                    .headers("Content-Type", "application/json")
                    .POST(HttpRequest.BodyPublishers.ofString(objectMapper.writeValueAsString(payloadContent)))
                    .build();

            HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
            if (response.statusCode() != 200) {
                logger.warn("Processing {} with request {} didn't succeed: {}", payload, payloadContent, response);
            }
        } catch (Throwable e) {
            logger.error("An error occurred while sending payload {} to {}: {}", payload, uri, e.getCause());
        }
        return false;
    }

    @Override
    public String getName() {
        return "remote";
    }

    private static class PayloadContent {

        private final String id;
        private final String modelid;
        private final String vModelId;
        private final String data;
        private final String kind;
        private final String status;
        private final Map<String, String> metadata;

        private PayloadContent(String id, String modelid, String vModelId, String data, String kind,
                               String status, Map<String, String> metadata) {
            this.id = id;
            this.modelid = modelid;
            this.vModelId = vModelId;
            this.data = data;
            this.kind = kind;
            this.status = status;
            this.metadata = metadata;
        }

        public String getId() {
            return id;
        }

        public String getKind() {
            return kind;
        }

        public String getModelid() {
            return modelid;
        }

        public String getvModelId() {
            return vModelId;
        }

        public String getData() {
            return data;
        }

        public String getStatus() {
            return status;
        }

        public Map<String, String> getMetadata() {
            return metadata;
        }

        @Override
        public String toString() {
            return "PayloadContent{" +
                    "id='" + id + '\'' +
                    ", modelid='" + modelid + '\'' +
                    ", vModelId=" + (vModelId != null ? ('\'' + vModelId + '\'') : "null") +
                    ", data='" + data + '\'' +
                    ", kind='" + kind + '\'' +
                    ", status='" + status + '\'' +
                    ", metadata='" + metadata + '\'' +
                    '}';
        }
    }
}
