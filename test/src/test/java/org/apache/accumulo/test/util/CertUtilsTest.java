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
package org.apache.accumulo.test.util;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.io.InputStream;
import java.nio.file.Files;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.PublicKey;
import java.security.SignatureException;
import java.security.cert.Certificate;

import org.apache.accumulo.harness.WithTestNames;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

public class CertUtilsTest extends WithTestNames {
  private static final String KEYSTORE_TYPE = "JKS";
  private static final String PASSWORD = "CertUtilsTestPassword";
  private static final char[] PASSWORD_CHARS = PASSWORD.toCharArray();
  private static final String RDN_STRING = "o=Apache Accumulo,cn=CertUtilsTest";

  @TempDir
  private static File tempDir;

  private CertUtils getUtils() {
    return new CertUtils(KEYSTORE_TYPE, RDN_STRING, "RSA", 4096, "SHA512WITHRSA");
  }

  @SuppressFBWarnings(value = "HARD_CODE_PASSWORD", justification = "test password is okay")
  @Test
  public void createSelfSigned() throws Exception {
    CertUtils certUtils = getUtils();
    File tempSubDir = tempDir.toPath().resolve(testName()).toFile();
    assertTrue(tempSubDir.isDirectory() || tempSubDir.mkdir());
    File keyStoreFile = tempSubDir.toPath().resolve("selfsigned.jks").toFile();
    certUtils.createSelfSignedCert(keyStoreFile, "test", PASSWORD);

    KeyStore keyStore = KeyStore.getInstance(KEYSTORE_TYPE);
    try (InputStream fis = Files.newInputStream(keyStoreFile.toPath())) {
      keyStore.load(fis, PASSWORD_CHARS);
    }
    Certificate cert = CertUtils.findCert(keyStore);

    cert.verify(cert.getPublicKey()); // throws exception if it can't be verified
  }

  @Test
  public void createPublicSelfSigned() throws Exception {
    CertUtils certUtils = getUtils();
    File tempSubDir = tempDir.toPath().resolve(testName()).toFile();
    assertTrue(tempSubDir.isDirectory() || tempSubDir.mkdir());
    File rootKeyStoreFile = tempSubDir.toPath().resolve("root.jks").toFile();
    certUtils.createSelfSignedCert(rootKeyStoreFile, "test", PASSWORD);
    File publicKeyStoreFile = tempSubDir.toPath().resolve("public.jks").toFile();
    certUtils.createPublicCert(publicKeyStoreFile, "test", rootKeyStoreFile.getAbsolutePath(),
        PASSWORD, "");

    KeyStore keyStore = KeyStore.getInstance(KEYSTORE_TYPE);
    try (InputStream fis = Files.newInputStream(publicKeyStoreFile.toPath())) {
      keyStore.load(fis, new char[0]);
    }
    var e = assertThrows(KeyStoreException.class,
        () -> CertUtils.findPrivateKey(keyStore, PASSWORD_CHARS),
        "expected not to find private key in keystore");
    assertTrue(e.getMessage().contains("private key"));
    Certificate cert = CertUtils.findCert(keyStore);
    cert.verify(cert.getPublicKey()); // throws exception if it can't be verified
  }

  @SuppressFBWarnings(value = "HARD_CODE_PASSWORD", justification = "test password is okay")
  @Test
  public void createSigned() throws Exception {
    CertUtils certUtils = getUtils();
    File tempSubDir = tempDir.toPath().resolve(testName()).toFile();
    assertTrue(tempSubDir.isDirectory() || tempSubDir.mkdir());
    File rootKeyStoreFile = tempSubDir.toPath().resolve("root.jks").toFile();
    certUtils.createSelfSignedCert(rootKeyStoreFile, "test", PASSWORD);
    File signedKeyStoreFile = tempSubDir.toPath().resolve("signed.jks").toFile();
    certUtils.createSignedCert(signedKeyStoreFile, "test", PASSWORD,
        rootKeyStoreFile.getAbsolutePath(), PASSWORD);

    KeyStore rootKeyStore = KeyStore.getInstance(KEYSTORE_TYPE);
    try (InputStream fis = Files.newInputStream(rootKeyStoreFile.toPath())) {
      rootKeyStore.load(fis, PASSWORD_CHARS);
    }
    Certificate rootCert = CertUtils.findCert(rootKeyStore);

    KeyStore signedKeyStore = KeyStore.getInstance(KEYSTORE_TYPE);
    try (InputStream fis = Files.newInputStream(signedKeyStoreFile.toPath())) {
      signedKeyStore.load(fis, PASSWORD_CHARS);
    }

    Certificate signedCert = CertUtils.findCert(signedKeyStore);
    PublicKey pubKey = signedCert.getPublicKey();
    assertThrows(SignatureException.class, () -> signedCert.verify(pubKey),
        "signed cert should not be able to verify itself");

    signedCert.verify(rootCert.getPublicKey()); // throws exception if it can't be verified
  }

  @Test
  public void publicOnlyVerfication() throws Exception {
    // this approximates the real life scenario. the client will only have the public key of each
    // cert (the root made by us as below, but the signed cert extracted by the SSL transport)
    CertUtils certUtils = getUtils();
    File tempSubDir = tempDir.toPath().resolve(testName()).toFile();
    assertTrue(tempSubDir.isDirectory() || tempSubDir.mkdir());
    File rootKeyStoreFile = tempSubDir.toPath().resolve("root.jks").toFile();
    certUtils.createSelfSignedCert(rootKeyStoreFile, "test", PASSWORD);
    File publicRootKeyStoreFile = tempSubDir.toPath().resolve("publicroot.jks").toFile();
    certUtils.createPublicCert(publicRootKeyStoreFile, "test", rootKeyStoreFile.getAbsolutePath(),
        PASSWORD, "");
    File signedKeyStoreFile = tempSubDir.toPath().resolve("signed.jks").toFile();
    certUtils.createSignedCert(signedKeyStoreFile, "test", PASSWORD,
        rootKeyStoreFile.getAbsolutePath(), PASSWORD);
    File publicSignedKeyStoreFile = tempSubDir.toPath().resolve("publicsigned.jks").toFile();
    certUtils.createPublicCert(publicSignedKeyStoreFile, "test",
        signedKeyStoreFile.getAbsolutePath(), PASSWORD, "");

    KeyStore rootKeyStore = KeyStore.getInstance(KEYSTORE_TYPE);
    try (InputStream fis = Files.newInputStream(publicRootKeyStoreFile.toPath())) {
      rootKeyStore.load(fis, new char[0]);
    }
    KeyStore signedKeyStore = KeyStore.getInstance(KEYSTORE_TYPE);
    try (InputStream fis = Files.newInputStream(publicSignedKeyStoreFile.toPath())) {
      signedKeyStore.load(fis, new char[0]);
    }
    Certificate rootCert = CertUtils.findCert(rootKeyStore);
    Certificate signedCert = CertUtils.findCert(signedKeyStore);
    PublicKey pubKey = signedCert.getPublicKey();

    assertThrows(SignatureException.class, () -> signedCert.verify(pubKey),
        "signed cert should not be able to verify itself");

    signedCert.verify(rootCert.getPublicKey()); // throws exception if it can't be verified
  }

  @SuppressFBWarnings(value = "HARD_CODE_PASSWORD", justification = "test password is okay")
  @Test
  public void signingChain() throws Exception {
    // no reason the keypair we generate for the tservers need to be able to sign anything,
    // but this is a way to make sure the private and public keys created actually correspond.
    CertUtils certUtils = getUtils();
    File tempSubDir = tempDir.toPath().resolve(testName()).toFile();
    assertTrue(tempSubDir.isDirectory() || tempSubDir.mkdir());
    File rootKeyStoreFile = tempSubDir.toPath().resolve("root.jks").toFile();
    certUtils.createSelfSignedCert(rootKeyStoreFile, "test", PASSWORD);
    File signedCaKeyStoreFile = tempSubDir.toPath().resolve("signedca.jks").toFile();
    certUtils.createSignedCert(signedCaKeyStoreFile, "test", PASSWORD,
        rootKeyStoreFile.getAbsolutePath(), PASSWORD);
    File signedLeafKeyStoreFile = tempSubDir.toPath().resolve("signedleaf.jks").toFile();
    certUtils.createSignedCert(signedLeafKeyStoreFile, "test", PASSWORD,
        signedCaKeyStoreFile.getAbsolutePath(), PASSWORD);

    KeyStore caKeyStore = KeyStore.getInstance(KEYSTORE_TYPE);
    try (InputStream fis = Files.newInputStream(signedCaKeyStoreFile.toPath())) {
      caKeyStore.load(fis, PASSWORD_CHARS);
    }
    Certificate caCert = CertUtils.findCert(caKeyStore);

    KeyStore leafKeyStore = KeyStore.getInstance(KEYSTORE_TYPE);
    try (InputStream fis = Files.newInputStream(signedLeafKeyStoreFile.toPath())) {
      leafKeyStore.load(fis, PASSWORD_CHARS);
    }
    Certificate leafCert = CertUtils.findCert(leafKeyStore);

    leafCert.verify(caCert.getPublicKey()); // throws exception if it can't be verified
  }
}
