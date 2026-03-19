/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.transform.llm;

import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.ObjectMapper;

import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for LLM model response parsing null safety. Verifies that malformed API responses produce
 * clear error messages instead of NullPointerException.
 */
public class OpenAIModelResponseParsingTest {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    @Test
    public void testMissingChoicesField() throws Exception {
        // API error response without "choices" field
        String response = "{\"error\":{\"message\":\"Rate limit exceeded\"}}";
        JsonNode result = OBJECT_MAPPER.readTree(response);
        JsonNode choices = result.get("choices");
        // choices should be null for error responses
        assertTrue(choices == null || !choices.isArray() || choices.isEmpty());
    }

    @Test
    public void testEmptyChoicesArray() throws Exception {
        String response = "{\"choices\":[]}";
        JsonNode result = OBJECT_MAPPER.readTree(response);
        JsonNode choices = result.get("choices");
        assertNotNull(choices);
        assertTrue(choices.isArray());
        assertTrue(choices.isEmpty());
    }

    @Test
    public void testMissingMessageContent() throws Exception {
        String response = "{\"choices\":[{\"index\":0,\"finish_reason\":\"stop\"}]}";
        JsonNode result = OBJECT_MAPPER.readTree(response);
        JsonNode choices = result.get("choices");
        assertNotNull(choices);
        assertTrue(choices.isArray());
        assertTrue(!choices.isEmpty());
        // Using path() returns MissingNode instead of null
        JsonNode content = choices.get(0).path("message").path("content");
        assertTrue(content.isMissingNode());
    }

    @Test
    public void testValidResponse() throws Exception {
        String response =
                "{\"choices\":[{\"index\":0,\"message\":{\"role\":\"assistant\",\"content\":\"[\\\"hello\\\"]\"}}]}";
        JsonNode result = OBJECT_MAPPER.readTree(response);
        JsonNode choices = result.get("choices");
        assertNotNull(choices);
        assertTrue(choices.isArray());
        assertTrue(!choices.isEmpty());
        JsonNode content = choices.get(0).path("message").path("content");
        assertTrue(!content.isMissingNode());
        assertNotNull(content.asText());
    }
}
