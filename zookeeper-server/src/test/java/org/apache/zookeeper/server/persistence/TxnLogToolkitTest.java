/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.zookeeper.server.persistence;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsNot.not;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.commons.io.FileUtils;
import org.apache.jute.BinaryOutputArchive;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.test.ClientBase;
import org.apache.zookeeper.txn.ErrorTxn;
import org.apache.zookeeper.txn.MultiTxn;
import org.apache.zookeeper.txn.Txn;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class TxnLogToolkitTest {

    private static final File testData = new File(System.getProperty("test.data.dir", "src/test/resources/data"));

    private final ByteArrayOutputStream outContent = new ByteArrayOutputStream();
    private final ByteArrayOutputStream errContent = new ByteArrayOutputStream();
    private File mySnapDir;

    @BeforeEach
    public void setUp() throws IOException {
        System.setOut(new PrintStream(outContent));
        System.setErr(new PrintStream(errContent));
        File snapDir = new File(testData, "invalidsnap");
        mySnapDir = ClientBase.createTmpDir();
        FileUtils.copyDirectory(snapDir, mySnapDir);
    }

    @AfterEach
    public void tearDown() throws IOException {
        System.setOut(System.out);
        System.setErr(System.err);
        mySnapDir.setWritable(true);
        FileUtils.deleteDirectory(mySnapDir);
    }

    @Test
    public void testDumpMode() throws Exception {
        // Arrange
        File logfile = new File(new File(mySnapDir, "version-2"), "log.274");
        TxnLogToolkit lt = new TxnLogToolkit(false, false, logfile.toString(), true);

        // Act
        lt.dump(null);

        // Assert
        // no exception thrown
    }

    @Test
    public void testInitMissingFile() throws FileNotFoundException, TxnLogToolkit.TxnLogToolkitException {
        assertThrows(TxnLogToolkit.TxnLogToolkitException.class, () -> {
            // Arrange & Act
            File logfile = new File("this_file_should_not_exists");
            TxnLogToolkit lt = new TxnLogToolkit(false, false, logfile.toString(), true);
        });
    }

    @Test
    public void testMultiTxnDecode() throws IOException {
        //MultiTxn with four ops, and the first op error.
        List<Txn> txns = new ArrayList<>();
        int type = -1;
        for (int i = 0; i < 4; i++) {
            ErrorTxn txn;
            if (i == 0) {
                txn = new ErrorTxn(KeeperException.Code.NONODE.intValue());
            } else {
                txn = new ErrorTxn(KeeperException.Code.RUNTIMEINCONSISTENCY.intValue());
            }
            try (ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
                BinaryOutputArchive boa = BinaryOutputArchive.getArchive(baos);
                txn.serialize(boa, "request");
                ByteBuffer bb = ByteBuffer.wrap(baos.toByteArray());
                txns.add(new Txn(type, bb.array()));
            }
        }
        MultiTxn multiTxn = new MultiTxn(txns);

        String formattedTxnStr = TxnLogToolkit.getFormattedTxnStr(multiTxn);
        assertEquals("error:-101;error:-2;error:-2;error:-2", formattedTxnStr);
    }

    @Test
    public void testInitWithRecoveryFileExists() {
        assertThrows(TxnLogToolkit.TxnLogToolkitException.class, () -> {
            // Arrange & Act
            File logfile = new File(new File(mySnapDir, "version-2"), "log.274");
            File recoveryFile = new File(new File(mySnapDir, "version-2"), "log.274.fixed");
            recoveryFile.createNewFile();
            TxnLogToolkit lt = new TxnLogToolkit(true, false, logfile.toString(), true);
        });
    }

    @Test
    public void testDumpWithCrcError() throws Exception {
        // Arrange
        File logfile = new File(new File(mySnapDir, "version-2"), "log.42");
        TxnLogToolkit lt = new TxnLogToolkit(false, false, logfile.toString(), true);

        // Act
        lt.dump(null);

        // Assert
        String output = outContent.toString();
        Pattern p = Pattern.compile("^CRC ERROR.*session 0x8061fac5ddeb0000 cxid 0x0 zxid 0x8800000002 createSession 30000$", Pattern.MULTILINE);
        Matcher m = p.matcher(output);
        assertTrue(m.find(), "Output doesn't indicate CRC error for the broken session id: " + output);
    }

    @Test
    public void testRecoveryFixBrokenFile() throws Exception {
        // Arrange
        File logfile = new File(new File(mySnapDir, "version-2"), "log.42");
        TxnLogToolkit lt = new TxnLogToolkit(true, false, logfile.toString(), true);

        // Act
        lt.dump(null);

        // Assert
        String output = outContent.toString();
        assertThat(output, containsString("CRC FIXED"));

        // Should be able to dump the recovered logfile with no CRC error
        outContent.reset();
        logfile = new File(new File(mySnapDir, "version-2"), "log.42.fixed");
        lt = new TxnLogToolkit(false, false, logfile.toString(), true);
        lt.dump(null);
        output = outContent.toString();
        assertThat(output, not(containsString("CRC ERROR")));
    }

    @Test
    public void testRecoveryInteractiveMode() throws Exception {
        // Arrange
        File logfile = new File(new File(mySnapDir, "version-2"), "log.42");
        TxnLogToolkit lt = new TxnLogToolkit(true, false, logfile.toString(), false);

        // Act
        lt.dump(new Scanner("y\n"));

        // Assert
        String output = outContent.toString();
        assertThat(output, containsString("CRC ERROR"));

        // Should be able to dump the recovered logfile with no CRC error
        outContent.reset();
        logfile = new File(new File(mySnapDir, "version-2"), "log.42.fixed");
        lt = new TxnLogToolkit(false, false, logfile.toString(), true);
        lt.dump(null);
        output = outContent.toString();
        assertThat(output, not(containsString("CRC ERROR")));
    }

}
