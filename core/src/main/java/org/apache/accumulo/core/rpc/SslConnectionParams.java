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
package org.apache.accumulo.core.rpc;

import java.io.FileNotFoundException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;

import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.thrift.transport.TSSLTransportFactory.TSSLTransportParameters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

public class SslConnectionParams {
  private static final Logger log = LoggerFactory.getLogger(SslConnectionParams.class);

  private boolean useJsse = false;
  private boolean clientAuth = false;

  private boolean keyStoreSet;
  private Path keyStorePath;
  private String keyStorePass;
  private String keyStoreType;

  private boolean trustStoreSet;
  private Path trustStorePath;
  private String trustStorePass;
  private String trustStoreType;

  private String[] cipherSuites;
  private String[] serverProtocols;
  private String clientProtocol;

  // Use the static construction methods
  private SslConnectionParams() {}

  public static SslConnectionParams forConfig(AccumuloConfiguration conf, boolean server) {
    if (!conf.getBoolean(Property.INSTANCE_RPC_SSL_ENABLED)) {
      return null;
    }

    SslConnectionParams result = new SslConnectionParams();
    boolean requireClientAuth = conf.getBoolean(Property.INSTANCE_RPC_SSL_CLIENT_AUTH);
    if (server) {
      result.setClientAuth(requireClientAuth);
    }
    if (conf.getBoolean(Property.RPC_USE_JSSE)) {
      result.setUseJsse(true);
      return result;
    }

    try {
      if (!server || requireClientAuth) {
        result.setTrustStoreFromConf(conf);
      }
      if (server || requireClientAuth) {
        result.setKeyStoreFromConf(conf);
      }
    } catch (FileNotFoundException e) {
      throw new IllegalArgumentException("Could not load configured keystore file", e);
    }

    String ciphers = conf.get(Property.RPC_SSL_CIPHER_SUITES);
    if (ciphers != null && !ciphers.isEmpty()) {
      result.cipherSuites = ciphers.split(",");
    }

    String enabledProtocols = conf.get(Property.RPC_SSL_ENABLED_PROTOCOLS);
    result.serverProtocols = enabledProtocols.split(",");

    result.clientProtocol = conf.get(Property.RPC_SSL_CLIENT_PROTOCOL);

    return result;
  }

  private static String passwordFromConf(AccumuloConfiguration conf, String defaultPassword,
      Property passwordOverrideProperty) {
    String keystorePassword = conf.get(passwordOverrideProperty);
    if (keystorePassword.isEmpty()) {
      keystorePassword = defaultPassword;
    } else {
      if (log.isTraceEnabled()) {
        log.trace("Using explicit SSL private key password from {}",
            passwordOverrideProperty.getKey());
      }
    }
    return keystorePassword;
  }

  private static Path storePathFromConf(AccumuloConfiguration conf, Property pathProperty)
      throws FileNotFoundException {
    return findKeystore(conf.getPath(pathProperty));
  }

  public void setKeyStoreFromConf(AccumuloConfiguration conf) throws FileNotFoundException {
    keyStoreSet = true;
    keyStorePath = storePathFromConf(conf, Property.RPC_SSL_KEYSTORE_PATH);
    keyStorePass = passwordFromConf(conf, conf.get(Property.INSTANCE_SECRET),
        Property.RPC_SSL_KEYSTORE_PASSWORD);
    keyStoreType = conf.get(Property.RPC_SSL_KEYSTORE_TYPE);
  }

  public void setTrustStoreFromConf(AccumuloConfiguration conf) throws FileNotFoundException {
    trustStoreSet = true;
    trustStorePath = storePathFromConf(conf, Property.RPC_SSL_TRUSTSTORE_PATH);
    trustStorePass = passwordFromConf(conf, "", Property.RPC_SSL_TRUSTSTORE_PASSWORD);
    trustStoreType = conf.get(Property.RPC_SSL_TRUSTSTORE_TYPE);
  }

  public static SslConnectionParams forServer(AccumuloConfiguration configuration) {
    return forConfig(configuration, true);
  }

  public static SslConnectionParams forClient(AccumuloConfiguration configuration) {
    return forConfig(configuration, false);
  }

  @SuppressFBWarnings(value = "PATH_TRAVERSAL_IN",
      justification = "code runs in same security context as user who providing the keystore file")
  private static Path findKeystore(String keystorePath) throws FileNotFoundException {
    try {
      // first just try the file
      Path path = Path.of(keystorePath);
      if (Files.exists(path)) {
        return path.toAbsolutePath();
      }
      if (!path.isAbsolute()) {
        // try classpath
        URL url = SslConnectionParams.class.getClassLoader().getResource(keystorePath);
        if (url != null) {
          path = Path.of(url.toURI());
          if (Files.exists(path)) {
            return path.toAbsolutePath();
          }
        }
      }
    } catch (Exception e) {
      log.warn("Exception finding keystore", e);
    }
    throw new FileNotFoundException("Failed to load SSL keystore from " + keystorePath);
  }

  public void setUseJsse(boolean useJsse) {
    this.useJsse = useJsse;
  }

  public boolean useJsse() {
    return useJsse;
  }

  public void setClientAuth(boolean clientAuth) {
    this.clientAuth = clientAuth;
  }

  public boolean isClientAuth() {
    return clientAuth;
  }

  public String[] getServerProtocols() {
    return serverProtocols;
  }

  public String getClientProtocol() {
    return clientProtocol;
  }

  public boolean isKeyStoreSet() {
    return keyStoreSet;
  }

  public Path getKeyStorePath() {
    return keyStorePath;
  }

  /**
   * @return the keyStorePass
   */
  public String getKeyStorePass() {
    return keyStorePass;
  }

  public String getKeyStoreType() {
    return keyStoreType;
  }

  public boolean isTrustStoreSet() {
    return trustStoreSet;
  }

  public Path getTrustStorePath() {
    return trustStorePath;
  }

  public String getTrustStorePass() {
    return trustStorePass;
  }

  /**
   * @return the trustStoreType
   */
  public String getTrustStoreType() {
    return trustStoreType;
  }

  public TSSLTransportParameters getTSSLTransportParameters() {
    if (useJsse) {
      throw new IllegalStateException("Cannot get TSSLTransportParameters for JSSE configuration.");
    }

    TSSLTransportParameters params = new TSSLTransportParameters(clientProtocol, cipherSuites);
    params.requireClientAuth(clientAuth);
    if (keyStoreSet) {
      params.setKeyStore(keyStorePath.toString(), keyStorePass, null, keyStoreType);
    }
    if (trustStoreSet) {
      params.setTrustStore(trustStorePath.toString(), trustStorePass, null, trustStoreType);
    }
    return params;
  }

  @Override
  public int hashCode() {
    int hash = 0;
    hash = 31 * hash + (clientAuth ? 0 : 1);
    hash = 31 * hash + (useJsse ? 0 : 1);
    if (useJsse) {
      return hash;
    }
    hash = 31 * hash + (keyStoreSet ? 0 : 1);
    hash = 31 * hash + (trustStoreSet ? 0 : 1);
    if (keyStoreSet) {
      hash = 31 * hash + keyStorePath.hashCode();
    }
    if (trustStoreSet) {
      hash = 31 * hash + trustStorePath.hashCode();
    }
    hash = 31 * hash + clientProtocol.hashCode();
    hash = 31 * hash + Arrays.hashCode(serverProtocols);
    return hash;
  }

  @Override
  public boolean equals(Object obj) {
    if (!(obj instanceof SslConnectionParams)) {
      return false;
    }

    SslConnectionParams other = (SslConnectionParams) obj;
    if (clientAuth != other.clientAuth) {
      return false;
    }
    if (useJsse) {
      return other.useJsse;
    }
    if (keyStoreSet) {
      if (!other.keyStoreSet) {
        return false;
      }
      if (!keyStorePath.equals(other.keyStorePath) || !keyStorePass.equals(other.keyStorePass)
          || !keyStoreType.equals(other.keyStoreType)) {
        return false;
      }
    }
    if (trustStoreSet) {
      if (!other.trustStoreSet) {
        return false;
      }
      if (!trustStorePath.equals(other.trustStorePath)
          || !trustStorePass.equals(other.trustStorePass)
          || !trustStoreType.equals(other.trustStoreType)) {
        return false;
      }
    }
    if (!Arrays.equals(serverProtocols, other.serverProtocols)) {
      return false;
    }
    return clientProtocol.equals(other.clientProtocol);
  }
}
