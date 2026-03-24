/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.accumulo.tserver.log;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.util.Arrays;

import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.crypto.CryptoUtils;
import org.apache.accumulo.core.spi.crypto.CryptoEnvironment;
import org.apache.accumulo.core.spi.crypto.CryptoService;
import org.apache.accumulo.core.spi.crypto.FileDecrypter;
import org.apache.accumulo.core.spi.wal.WriteAheadLogFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DfsWalReader implements WriteAheadLogFactory.WalReader {

  public static final String LOG_FILE_HEADER_V3 = "--- Log File Header (v3) ---";

  /**
   * Simplified encryption technique supported in V4.
   *
   * @since 2.0.0
   */
  public static final String LOG_FILE_HEADER_V4 = "--- Log File Header (v4) ---";

  private static final Logger log = LoggerFactory.getLogger(DfsWalReader.class);

  private final CryptoService cryptoService;
  private final long walBlockSize;

  public DfsWalReader(CryptoService cryptoService, AccumuloConfiguration conf) {
    this.cryptoService = cryptoService;
    this.walBlockSize = computeWalBlockSize(conf);
  }

  /**
   * Reads the WAL file header, and returns a decrypting stream which wraps the original stream. If
   * the file is not encrypted, the original stream is returned.
   *
   * @throws WriteAheadLogFactory.WalHeaderIncompleteException if the header cannot be fully read
   *         (can happen if the tserver died before finishing)
   */

  @Override
  public DataInputStream open(DataInputStream raw)
      throws IOException, WriteAheadLogFactory.WalHeaderIncompleteException {

    byte[] magic4 = LOG_FILE_HEADER_V4.getBytes(UTF_8);
    byte[] magic3 = LOG_FILE_HEADER_V3.getBytes(UTF_8);

    byte[] magicBuffer = new byte[magic4.length];
    try {
      raw.readFully(magicBuffer);
    } catch (EOFException e) {
      // Explicitly catch any exceptions that should be converted to LogHeaderIncompleteException
      // A TabletServer might have died before the (complete) header was written
      throw new WriteAheadLogFactory.WalHeaderIncompleteException(e);
    }

    if (Arrays.equals(magicBuffer, magic4)) {
      // v4: Apply Optional encryption. The crypto params written after the header tell the
      // FileDecrypter how to
      // reconstruct the encryption key
      FileDecrypter decrypter =
          CryptoUtils.getFileDecrypter(cryptoService, CryptoEnvironment.Scope.WAL, null, raw);
      log.debug("Using {} for decrypting WAL", cryptoService.getClass().getSimpleName());
      var stream = decrypter.decryptStream(raw);
      return stream instanceof DataInputStream ? (DataInputStream) stream
          : new DataInputStream(stream);

    } else if (Arrays.equals(magicBuffer, magic3)) {
      // v3: Accumulo 1.9 format. Encryption is represented by a class name written as a UTF string
      // immediately
      // after the magic bytes. Only NullCryptoModule (no encryption) is supported for upgrade
      // reads.
      String cryptoModuleClassname;
      try {
        cryptoModuleClassname = raw.readUTF();
      } catch (EOFException e) {
        throw new WriteAheadLogFactory.WalHeaderIncompleteException(e);
      }
      if (!cryptoModuleClassname.equals("NullCryptoModule")) {
        throw new IllegalArgumentException(
            "Old encryption modules not supported at this time.  Unsupported module : "
                + cryptoModuleClassname);
      }
      return raw;

    } else {
      throw new IllegalArgumentException(
          "Unsupported write ahead log version " + new String(magicBuffer, UTF_8));
    }
  }

  @Override
  public long getWalBlockSize() {
    return walBlockSize;
  }

  static long computeWalBlockSize(AccumuloConfiguration conf) {
    @SuppressWarnings("deprecation")
    long blockSize = conf.getAsBytes(Property.TSERV_WAL_BLOCKSIZE);
    if (blockSize == 0) {
      blockSize = (long) (conf.getAsBytes(Property.TSERV_WAL_MAX_SIZE) * 1.1);
    }
    return blockSize;
  }

}
