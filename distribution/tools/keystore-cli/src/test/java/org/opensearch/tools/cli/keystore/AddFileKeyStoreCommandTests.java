/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.tools.cli.keystore;

import org.opensearch.cli.Command;
import org.opensearch.cli.ExitCodes;
import org.opensearch.cli.UserException;
import org.opensearch.common.collect.Tuple;
import org.opensearch.common.settings.KeyStoreWrapper;
import org.opensearch.env.Environment;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.containsString;

public class AddFileKeyStoreCommandTests extends KeyStoreCommandTestCase {
    @Override
    protected Command newCommand() {
        return new AddFileKeyStoreCommand() {
            @Override
            protected Environment createEnv(Map<String, String> settings) throws UserException {
                return env;
            }
        };
    }

    private Path createRandomFile() throws IOException {
        int length = randomIntBetween(10, 20);
        byte[] bytes = new byte[length];
        for (int i = 0; i < length; ++i) {
            bytes[i] = randomByte();
        }
        Path file = env.configDir().resolve(randomAlphaOfLength(16));
        Files.write(file, bytes);
        return file;
    }

    private void addFile(KeyStoreWrapper keystore, String setting, Path file, String password) throws Exception {
        keystore.setFile(setting, Files.readAllBytes(file));
        keystore.save(env.configDir(), password.toCharArray());
    }

    public void testMissingCreateWithEmptyPasswordWhenPrompted() throws Exception {
        String password = "";
        Path file1 = createRandomFile();
        terminal.addTextInput("y");
        execute("foo", file1.toString());
        assertSecureFile("foo", file1, password);
    }

    public void testMissingCreateWithEmptyPasswordWithoutPromptIfForced() throws Exception {
        String password = "";
        Path file1 = createRandomFile();
        execute("-f", "foo", file1.toString());
        assertSecureFile("foo", file1, password);
    }

    public void testMissingNoCreate() throws Exception {
        terminal.addSecretInput(randomFrom("", "keystorepassword"));
        terminal.addTextInput("n"); // explicit no
        execute("foo");
        assertNull(KeyStoreWrapper.load(env.configDir()));
    }

    public void testOverwritePromptDefault() throws Exception {
        String password = "keystorepassword";
        Path file = createRandomFile();
        KeyStoreWrapper keystore = createKeystore(password);
        addFile(keystore, "foo", file, password);
        terminal.addSecretInput(password);
        terminal.addSecretInput(password);
        terminal.addTextInput("");
        execute("foo", "path/dne");
        assertSecureFile("foo", file, password);
    }

    public void testOverwritePromptExplicitNo() throws Exception {
        String password = "keystorepassword";
        Path file = createRandomFile();
        KeyStoreWrapper keystore = createKeystore(password);
        addFile(keystore, "foo", file, password);
        terminal.addSecretInput(password);
        terminal.addTextInput("n"); // explicit no
        execute("foo", "path/dne");
        assertSecureFile("foo", file, password);
    }

    public void testOverwritePromptExplicitYes() throws Exception {
        String password = "keystorepassword";
        Path file1 = createRandomFile();
        KeyStoreWrapper keystore = createKeystore(password);
        addFile(keystore, "foo", file1, password);
        terminal.addSecretInput(password);
        terminal.addSecretInput(password);
        terminal.addTextInput("y");
        Path file2 = createRandomFile();
        execute("foo", file2.toString());
        assertSecureFile("foo", file2, password);
    }

    public void testOverwriteForceShort() throws Exception {
        String password = "keystorepassword";
        Path file1 = createRandomFile();
        KeyStoreWrapper keystore = createKeystore(password);
        addFile(keystore, "foo", file1, password);
        Path file2 = createRandomFile();
        terminal.addSecretInput(password);
        terminal.addSecretInput(password);
        execute("-f", "foo", file2.toString());
        assertSecureFile("foo", file2, password);
    }

    public void testOverwriteForceLong() throws Exception {
        String password = "keystorepassword";
        Path file1 = createRandomFile();
        KeyStoreWrapper keystore = createKeystore(password);
        addFile(keystore, "foo", file1, password);
        Path file2 = createRandomFile();
        terminal.addSecretInput(password);
        execute("--force", "foo", file2.toString());
        assertSecureFile("foo", file2, password);
    }

    public void testForceDoesNotAlreadyExist() throws Exception {
        String password = "keystorepassword";
        createKeystore(password);
        Path file = createRandomFile();
        terminal.addSecretInput(password);
        execute("--force", "foo", file.toString());
        assertSecureFile("foo", file, password);
    }

    public void testMissingSettingName() throws Exception {
        String password = "keystorepassword";
        createKeystore(password);
        terminal.addSecretInput(password);
        UserException e = expectThrows(UserException.class, this::execute);
        assertEquals(ExitCodes.USAGE, e.exitCode);
        assertThat(e.getMessage(), containsString("Missing setting name"));
    }

    public void testMissingFileName() throws Exception {
        String password = "keystorepassword";
        createKeystore(password);
        terminal.addSecretInput(password);
        UserException e = expectThrows(UserException.class, () -> execute("foo"));
        assertEquals(ExitCodes.USAGE, e.exitCode);
        assertThat(e.getMessage(), containsString("settings and filenames must come in pairs"));
    }

    public void testFileDNE() throws Exception {
        String password = "keystorepassword";
        createKeystore(password);
        terminal.addSecretInput(password);
        UserException e = expectThrows(UserException.class, () -> execute("foo", "path/dne"));
        assertEquals(ExitCodes.IO_ERROR, e.exitCode);
        assertThat(e.getMessage(), containsString("File [path/dne] does not exist"));
    }

    public void testExtraArguments() throws Exception {
        String password = "keystorepassword";
        createKeystore(password);
        Path file = createRandomFile();
        terminal.addSecretInput(password);
        UserException e = expectThrows(UserException.class, () -> execute("foo", file.toString(), "bar"));
        assertEquals(e.getMessage(), ExitCodes.USAGE, e.exitCode);
        assertThat(e.getMessage(), containsString("settings and filenames must come in pairs"));
    }

    public void testIncorrectPassword() throws Exception {
        String password = "keystorepassword";
        createKeystore(password);
        Path file = createRandomFile();
        terminal.addSecretInput("thewrongkeystorepassword");
        UserException e = expectThrows(UserException.class, () -> execute("foo", file.toString()));
        assertEquals(e.getMessage(), ExitCodes.DATA_ERROR, e.exitCode);
        assertThat(
            e.getMessage(),
            anyOf(
                containsString("Provided keystore password was incorrect"),
                containsString("Keystore has been corrupted or tampered with")
            )
        );
    }

    public void testAddToUnprotectedKeystore() throws Exception {
        String password = "";
        Path file = createRandomFile();
        KeyStoreWrapper keystore = createKeystore(password);
        addFile(keystore, "foo", file, password);
        terminal.addTextInput("");
        // will not be prompted for a password
        execute("foo", "path/dne");
        assertSecureFile("foo", file, password);
    }

    public void testAddMultipleFiles() throws Exception {
        final String password = "keystorepassword";
        createKeystore(password);
        final int n = randomIntBetween(1, 8);
        final List<Tuple<String, Path>> settingFilePairs = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            settingFilePairs.add(Tuple.tuple("foo" + i, createRandomFile()));
        }
        terminal.addSecretInput(password);
        execute(settingFilePairs.stream().flatMap(t -> Stream.of(t.v1(), t.v2().toString())).toArray(String[]::new));
        for (int i = 0; i < n; i++) {
            assertSecureFile(settingFilePairs.get(i).v1(), settingFilePairs.get(i).v2(), password);
        }
    }

}
