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

package org.apache.seatunnel.engine.server.service.jar;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.FileOutputStream;
import java.lang.reflect.Method;
import java.nio.file.Files;
import java.nio.file.Path;

public class ServerConnectorPackageClientTest {

    @Test
    public void testStorageConnectorJarFileClosesStream(@TempDir Path tempDir) throws Exception {
        ServerConnectorPackageClient client =
                new ServerConnectorPackageClient(null, null);

        byte[] testData = "test jar content".getBytes();
        File storageFile = tempDir.resolve("test-connector.jar").toFile();

        // Use reflection to invoke the private method
        Method method =
                ServerConnectorPackageClient.class.getDeclaredMethod(
                        "storageConnectorJarFile", byte[].class, File.class);
        method.setAccessible(true);
        method.invoke(client, testData, storageFile);

        // Verify file was written correctly
        Assertions.assertTrue(storageFile.exists());
        byte[] readBack = Files.readAllBytes(storageFile.toPath());
        Assertions.assertArrayEquals(testData, readBack);

        // Verify the stream is closed - we can write to the file again (not locked)
        try (FileOutputStream fos = new FileOutputStream(storageFile)) {
            fos.write("overwrite".getBytes());
        }
        Assertions.assertTrue(storageFile.exists());
    }

    @Test
    public void testStorageConnectorJarFileSkipsExistingFile(@TempDir Path tempDir)
            throws Exception {
        ServerConnectorPackageClient client =
                new ServerConnectorPackageClient(null, null);

        byte[] originalData = "original content".getBytes();
        byte[] newData = "new content".getBytes();
        File storageFile = tempDir.resolve("existing-connector.jar").toFile();

        // Create the file first
        Files.write(storageFile.toPath(), originalData);

        Method method =
                ServerConnectorPackageClient.class.getDeclaredMethod(
                        "storageConnectorJarFile", byte[].class, File.class);
        method.setAccessible(true);
        method.invoke(client, newData, storageFile);

        // Verify original content is preserved (not overwritten)
        byte[] readBack = Files.readAllBytes(storageFile.toPath());
        Assertions.assertArrayEquals(originalData, readBack);
    }
}
