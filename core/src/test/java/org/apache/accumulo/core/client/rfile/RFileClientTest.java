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
package org.apache.accumulo.core.client.rfile;

import static com.google.common.collect.MoreCollectors.onlyElement;
import static org.apache.accumulo.core.util.LazySingletons.RANDOM;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.IOException;
import java.net.ConnectException;
import java.net.URI;
import java.nio.file.Files;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.admin.NewTableConfiguration;
import org.apache.accumulo.core.client.rfile.RFile.InputArguments.FencedPath;
import org.apache.accumulo.core.client.sample.RowSampler;
import org.apache.accumulo.core.client.sample.SamplerConfiguration;
import org.apache.accumulo.core.client.summary.CounterSummary;
import org.apache.accumulo.core.client.summary.SummarizerConfiguration;
import org.apache.accumulo.core.client.summary.Summary;
import org.apache.accumulo.core.client.summary.summarizers.FamilySummarizer;
import org.apache.accumulo.core.client.summary.summarizers.VisibilitySummarizer;
import org.apache.accumulo.core.conf.DefaultConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.ArrayByteSequence;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.LoadPlan;
import org.apache.accumulo.core.data.LoadPlanTest;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.file.FileOperations;
import org.apache.accumulo.core.file.FileSKVIterator;
import org.apache.accumulo.core.file.rfile.RFile.RFileSKVIterator;
import org.apache.accumulo.core.file.rfile.RFile.Reader;
import org.apache.accumulo.core.iterators.user.RegExFilter;
import org.apache.accumulo.core.metadata.UnreferencedTabletFile;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.spi.crypto.NoCryptoServiceFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.io.Text;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

@SuppressFBWarnings(value = "PATH_TRAVERSAL_IN", justification = "path is set by test, not user")
public class RFileClientTest {

  @TempDir
  private static java.nio.file.Path tempDir;

  private String createTmpTestFile() throws IOException {
    java.nio.file.Path testFile = Files.createTempFile(tempDir, "test", ".rf");
    Files.deleteIfExists(testFile);
    return testFile.toAbsolutePath().toString();
  }

  String rowStr(int r) {
    return String.format("%06x", r);
  }

  String colStr(int c) {
    return String.format("%04x", c);
  }

  private SortedMap<Key,Value> createTestData(int rows, int families, int qualifiers) {
    return createTestData(0, rows, 0, families, qualifiers);
  }

  private SortedMap<Key,Value> createTestData(int startRow, int rows, int startFamily, int families,
      int qualifiers) {
    return createTestData(startRow, rows, startFamily, families, qualifiers, "");
  }

  private SortedMap<Key,Value> createTestData(int startRow, int rows, int startFamily, int families,
      int qualifiers, String... vis) {
    TreeMap<Key,Value> testData = new TreeMap<>();

    for (int r = 0; r < rows; r++) {
      String row = rowStr(r + startRow);
      for (int f = 0; f < families; f++) {
        String fam = colStr(f + startFamily);
        for (int q = 0; q < qualifiers; q++) {
          String qual = colStr(q);
          for (String v : vis) {
            Key k = new Key(row, fam, qual, v);
            testData.put(k, new Value(k.hashCode() + ""));
          }
        }
      }
    }

    return testData;
  }

  private String createRFile(SortedMap<Key,Value> testData) throws Exception {
    String testFile = createTmpTestFile();

    try (RFileWriter writer = RFile.newWriter().to(testFile)
        .withFileSystem(FileSystem.getLocal(new Configuration())).build()) {
      writer.append(testData.entrySet());
      // TODO ensure compressors are returned
    }

    return testFile;
  }

  @Test
  public void testIndependance() throws Exception {
    // test to ensure two iterators allocated from same RFile scanner are independent.

    LocalFileSystem localFs = FileSystem.getLocal(new Configuration());

    SortedMap<Key,Value> testData = createTestData(10, 10, 10);

    String testFile = createRFile(testData);

    Scanner scanner = RFile.newScanner().from(testFile).withFileSystem(localFs).build();
    Range range1 = Range.exact(rowStr(5));
    scanner.setRange(range1);
    Iterator<Entry<Key,Value>> scnIter1 = scanner.iterator();
    Iterator<Entry<Key,Value>> mapIter1 =
        testData.subMap(range1.getStartKey(), range1.getEndKey()).entrySet().iterator();

    Range range2 = new Range(rowStr(3), true, rowStr(4), true);
    scanner.setRange(range2);
    Iterator<Entry<Key,Value>> scnIter2 = scanner.iterator();
    Iterator<Entry<Key,Value>> mapIter2 =
        testData.subMap(range2.getStartKey(), range2.getEndKey()).entrySet().iterator();

    while (scnIter1.hasNext() || scnIter2.hasNext()) {
      if (scnIter1.hasNext()) {
        assertTrue(mapIter1.hasNext());
        assertEquals(scnIter1.next(), mapIter1.next());
      } else {
        assertFalse(mapIter1.hasNext());
      }

      if (scnIter2.hasNext()) {
        assertTrue(mapIter2.hasNext());
        assertEquals(scnIter2.next(), mapIter2.next());
      } else {
        assertFalse(mapIter2.hasNext());
      }
    }

    assertFalse(mapIter1.hasNext());
    assertFalse(mapIter2.hasNext());

    scanner.close();
  }

  SortedMap<Key,Value> toMap(Scanner scanner) {
    TreeMap<Key,Value> map = new TreeMap<>();
    for (Entry<Key,Value> entry : scanner) {
      map.put(entry.getKey(), entry.getValue());
    }
    return map;
  }

  SortedMap<Key,Value> toMap(FileSKVIterator iterator) throws IOException {
    TreeMap<Key,Value> map = new TreeMap<>();
    while (iterator.hasTop()) {
      // Need to copy Value as the reference gets reused
      map.put(iterator.getTopKey(), new Value(iterator.getTopValue()));
      iterator.next();
    }
    return map;
  }

  @Test
  public void testMultipleSources() throws Exception {
    SortedMap<Key,Value> testData1 = createTestData(10, 10, 10);
    SortedMap<Key,Value> testData2 = createTestData(0, 10, 0, 10, 10);

    String testFile1 = createRFile(testData1);
    String testFile2 = createRFile(testData2);

    LocalFileSystem localFs = FileSystem.getLocal(new Configuration());
    Scanner scanner = RFile.newScanner().from(testFile1, testFile2).withFileSystem(localFs).build();

    TreeMap<Key,Value> expected = new TreeMap<>(testData1);
    expected.putAll(testData2);

    assertEquals(expected, toMap(scanner));

    Range range = new Range(rowStr(3), true, rowStr(14), true);
    scanner.setRange(range);
    assertEquals(expected.subMap(range.getStartKey(), range.getEndKey()), toMap(scanner));

    scanner.close();
  }

  @Test
  public void testFencingScanner() throws Exception {
    SortedMap<Key,Value> testData = createTestData(10, 10, 10);

    String testFile = createRFile(testData);

    LocalFileSystem localFs = FileSystem.getLocal(new Configuration());

    Range range = new Range(rowStr(3), false, rowStr(14), true);
    Scanner scanner = RFile.newScanner()
        .from(new FencedPath(new Path(java.nio.file.Path.of(testFile).toUri()), range))
        .withFileSystem(localFs).build();

    TreeMap<Key,Value> expected = new TreeMap<>(testData);

    // Range is set on the RFile iterator itself and not the scanner
    assertEquals(expected.subMap(range.getStartKey(), range.getEndKey()), toMap(scanner));

    scanner.close();
  }

  @Test
  public void testRequiresRowRange() throws Exception {
    SortedMap<Key,Value> testData = createTestData(10, 10, 10);
    String testFile = createRFile(testData);

    // Row Ranges may have null for start and/or end row or be set.
    // If start is set, it must be inclusive and if end is set it ust be exclusive.
    // End key must also be an exclusive key (end in 0x00 byte).
    // Lastly only the row portion of a key is allowed.

    // Test valid Row Ranges
    URI testFileURI = java.nio.file.Path.of(testFile).toUri();
    new FencedPath(new Path(testFileURI), new Range());
    // This constructor converts to the proper inclusive/exclusive rows
    new FencedPath(new Path(testFileURI), new Range(rowStr(3), false, rowStr(14), true));
    new FencedPath(new Path(testFileURI), new Range(new Key(rowStr(3)).followingKey(PartialKey.ROW),
        true, new Key(rowStr(14)).followingKey(PartialKey.ROW), false));

    // Test invalid Row Ranges
    // Missing 0x00 byte
    assertThrows(IllegalArgumentException.class, () -> new FencedPath(new Path(testFileURI),
        new Range(new Key(rowStr(3)), true, new Key(rowStr(14)), false)));
    // End key inclusive
    assertThrows(IllegalArgumentException.class, () -> new FencedPath(new Path(testFileURI),
        new Range(new Key(rowStr(3)), true, new Key(rowStr(14)), true)));
    // Start key exclusive
    assertThrows(IllegalArgumentException.class, () -> new FencedPath(new Path(testFileURI),
        new Range(new Key(rowStr(3)), false, new Key(rowStr(14)), false)));
    // CF is set which is not allowed
    assertThrows(IllegalArgumentException.class,
        () -> new FencedPath(new Path(testFileURI), new Range(new Key(rowStr(3), colStr(3)), true,
            new Key(rowStr(14)).followingKey(PartialKey.ROW), false)));
  }

  @Test
  public void testFencingReader() throws Exception {
    SortedMap<Key,Value> testData = createTestData(10, 10, 10);

    String testFile = createRFile(testData);

    LocalFileSystem localFs = FileSystem.getLocal(new Configuration());

    Range range = new Range(rowStr(3), false, rowStr(14), true);

    RFileSKVIterator reader = getReader(localFs,
        UnreferencedTabletFile.ofRanged(localFs, java.nio.file.Path.of(testFile).toFile(), range));
    reader.seek(new Range(), List.of(), false);

    TreeMap<Key,Value> expected = new TreeMap<>(testData);

    // Range is set on the RFile iterator itself and not the scanner
    assertEquals(expected.subMap(range.getStartKey(), range.getEndKey()), toMap(reader));

    reader.close();
  }

  @Test
  public void testWriterTableProperties() throws Exception {
    LocalFileSystem localFs = FileSystem.getLocal(new Configuration());

    String testFile = createTmpTestFile();

    Map<String,String> props = new HashMap<>();
    props.put(Property.TABLE_FILE_COMPRESSED_BLOCK_SIZE.getKey(), "1K");
    props.put(Property.TABLE_FILE_COMPRESSED_BLOCK_SIZE_INDEX.getKey(), "1K");
    RFileWriter writer =
        RFile.newWriter().to(testFile).withFileSystem(localFs).withTableProperties(props).build();

    SortedMap<Key,Value> testData1 = createTestData(10, 10, 10);
    writer.append(testData1.entrySet());
    writer.close();

    RFileSKVIterator reader = getReader(localFs,
        UnreferencedTabletFile.of(localFs, java.nio.file.Path.of(testFile).toFile()));
    FileSKVIterator iiter = reader.getIndex();

    int count = 0;
    while (iiter.hasTop()) {
      count++;
      iiter.next();
    }

    // if settings are used then should create multiple index entries
    assertTrue(count > 10);

    reader.close();

    Scanner scanner = RFile.newScanner().from(testFile).withFileSystem(localFs).build();
    assertEquals(testData1, toMap(scanner));
    scanner.close();
  }

  @Test
  public void testLocalityGroups() throws Exception {

    SortedMap<Key,Value> testData1 = createTestData(0, 10, 0, 2, 10);
    SortedMap<Key,Value> testData2 = createTestData(0, 10, 2, 1, 10);
    SortedMap<Key,Value> defaultData = createTestData(0, 10, 3, 7, 10);

    LocalFileSystem localFs = FileSystem.getLocal(new Configuration());
    String testFile = createTmpTestFile();
    RFileWriter writer = RFile.newWriter().to(testFile).withFileSystem(localFs).build();

    writer.startNewLocalityGroup("z", colStr(0), colStr(1));
    writer.append(testData1.entrySet());

    writer.startNewLocalityGroup("h", colStr(2));
    writer.append(testData2.entrySet());

    writer.startDefaultLocalityGroup();
    writer.append(defaultData.entrySet());

    writer.close();

    Scanner scanner = RFile.newScanner().from(testFile).withFileSystem(localFs).build();

    scanner.fetchColumnFamily(new Text(colStr(0)));
    scanner.fetchColumnFamily(new Text(colStr(1)));
    assertEquals(testData1, toMap(scanner));

    scanner.clearColumns();
    scanner.fetchColumnFamily(new Text(colStr(2)));
    assertEquals(testData2, toMap(scanner));

    scanner.clearColumns();
    for (int i = 3; i < 10; i++) {
      scanner.fetchColumnFamily(new Text(colStr(i)));
    }
    assertEquals(defaultData, toMap(scanner));

    scanner.clearColumns();
    assertEquals(createTestData(10, 10, 10), toMap(scanner));

    scanner.close();

    Reader reader = (Reader) getReader(localFs,
        UnreferencedTabletFile.of(localFs, java.nio.file.Path.of(testFile).toFile()));
    Map<String,ArrayList<ByteSequence>> lGroups = reader.getLocalityGroupCF();
    assertTrue(lGroups.containsKey("z"));
    assertEquals(2, lGroups.get("z").size());
    assertTrue(lGroups.get("z").contains(new ArrayByteSequence(colStr(0))));
    assertTrue(lGroups.get("z").contains(new ArrayByteSequence(colStr(1))));
    assertTrue(lGroups.containsKey("h"));
    assertEquals(Arrays.asList(new ArrayByteSequence(colStr(2))), lGroups.get("h"));
    reader.close();
  }

  @Test
  public void testIterators() throws Exception {

    LocalFileSystem localFs = FileSystem.getLocal(new Configuration());
    SortedMap<Key,Value> testData = createTestData(10, 10, 10);
    String testFile = createRFile(testData);

    IteratorSetting is = new IteratorSetting(50, "regex", RegExFilter.class);
    RegExFilter.setRegexs(is, ".*00000[78].*", null, null, null, false);

    Scanner scanner = RFile.newScanner().from(testFile).withFileSystem(localFs).build();
    scanner.addScanIterator(is);

    assertEquals(createTestData(7, 2, 0, 10, 10), toMap(scanner));

    scanner.close();
  }

  @Test
  public void testAuths() throws Exception {
    LocalFileSystem localFs = FileSystem.getLocal(new Configuration());
    String testFile = createTmpTestFile();
    RFileWriter writer = RFile.newWriter().to(testFile).withFileSystem(localFs).build();

    Key k1 = new Key("r1", "f1", "q1", "A&B");
    Key k2 = new Key("r1", "f1", "q2", "A");
    Key k3 = new Key("r1", "f1", "q3");

    Value v1 = new Value("p");
    Value v2 = new Value("c");
    Value v3 = new Value("t");

    writer.append(k1, v1);
    writer.append(k2, v2);
    writer.append(k3, v3);
    writer.close();

    Scanner scanner = RFile.newScanner().from(testFile).withFileSystem(localFs)
        .withAuthorizations(new Authorizations("A")).build();
    assertEquals(Map.of(k2, v2, k3, v3), toMap(scanner));
    assertEquals(new Authorizations("A"), scanner.getAuthorizations());
    scanner.close();

    scanner = RFile.newScanner().from(testFile).withFileSystem(localFs)
        .withAuthorizations(new Authorizations("A", "B")).build();
    assertEquals(Map.of(k1, v1, k2, v2, k3, v3), toMap(scanner));
    assertEquals(new Authorizations("A", "B"), scanner.getAuthorizations());
    scanner.close();

    scanner = RFile.newScanner().from(testFile).withFileSystem(localFs)
        .withAuthorizations(new Authorizations("B")).build();
    assertEquals(Map.of(k3, v3), toMap(scanner));
    assertEquals(new Authorizations("B"), scanner.getAuthorizations());
    scanner.close();
  }

  @Test
  public void testNoSystemIters() throws Exception {
    LocalFileSystem localFs = FileSystem.getLocal(new Configuration());
    String testFile = createTmpTestFile();
    RFileWriter writer = RFile.newWriter().to(testFile).withFileSystem(localFs).build();

    Key k1 = new Key("r1", "f1", "q1");
    k1.setTimestamp(3);

    Key k2 = new Key("r1", "f1", "q1");
    k2.setTimestamp(6);
    k2.setDeleted(true);

    Value v1 = new Value("p");
    Value v2 = new Value("");

    writer.append(k2, v2);
    writer.append(k1, v1);
    writer.close();

    Scanner scanner = RFile.newScanner().from(testFile).withFileSystem(localFs).build();
    assertFalse(scanner.iterator().hasNext());
    scanner.close();

    scanner =
        RFile.newScanner().from(testFile).withFileSystem(localFs).withoutSystemIterators().build();
    assertEquals(Map.of(k2, v2, k1, v1), toMap(scanner));
    scanner.setRange(new Range("r2"));
    assertFalse(scanner.iterator().hasNext());
    scanner.close();
  }

  @Test
  public void testBounds() throws Exception {
    LocalFileSystem localFs = FileSystem.getLocal(new Configuration());
    SortedMap<Key,Value> testData = createTestData(10, 10, 10);
    String testFile = createRFile(testData);

    // set a lower bound row
    Range bounds = new Range(rowStr(3), false, null, true);
    Scanner scanner =
        RFile.newScanner().from(testFile).withFileSystem(localFs).withBounds(bounds).build();
    assertEquals(createTestData(4, 6, 0, 10, 10), toMap(scanner));
    scanner.close();

    // set an upper bound row
    bounds = new Range(null, false, rowStr(7), true);
    scanner = RFile.newScanner().from(testFile).withFileSystem(localFs).withBounds(bounds).build();
    assertEquals(createTestData(8, 10, 10), toMap(scanner));
    scanner.close();

    // set row bounds
    bounds = new Range(rowStr(3), false, rowStr(7), true);
    scanner = RFile.newScanner().from(testFile).withFileSystem(localFs).withBounds(bounds).build();
    assertEquals(createTestData(4, 4, 0, 10, 10), toMap(scanner));
    scanner.close();

    // set a row family bound
    bounds = Range.exact(rowStr(3), colStr(5));
    scanner = RFile.newScanner().from(testFile).withFileSystem(localFs).withBounds(bounds).build();
    assertEquals(createTestData(3, 1, 5, 1, 10), toMap(scanner));
    scanner.close();
  }

  @Test
  public void testScannerTableProperties() throws Exception {
    NewTableConfiguration ntc = new NewTableConfiguration();

    LocalFileSystem localFs = FileSystem.getLocal(new Configuration());
    String testFile = createTmpTestFile();
    RFileWriter writer = RFile.newWriter().to(testFile).withFileSystem(localFs).build();

    Key k1 = new Key("r1", "f1", "q1");
    k1.setTimestamp(3);

    Key k2 = new Key("r1", "f1", "q1");
    k2.setTimestamp(6);

    Value v1 = new Value("p");
    Value v2 = new Value("q");

    writer.append(k2, v2);
    writer.append(k1, v1);
    writer.close();

    // pass in table config that has versioning iterator configured
    Scanner scanner = RFile.newScanner().from(testFile).withFileSystem(localFs)
        .withTableProperties(ntc.getProperties()).build();
    assertEquals(Map.of(k2, v2), toMap(scanner));
    scanner.close();

    scanner = RFile.newScanner().from(testFile).withFileSystem(localFs).build();
    assertEquals(Map.of(k2, v2, k1, v1), toMap(scanner));
    scanner.close();
  }

  @Test
  public void testSampling() throws Exception {

    SortedMap<Key,Value> testData1 = createTestData(1000, 2, 1);

    LocalFileSystem localFs = FileSystem.getLocal(new Configuration());
    String testFile = createTmpTestFile();

    SamplerConfiguration sc = new SamplerConfiguration(RowSampler.class)
        .setOptions(Map.of("hasher", "murmur3_32", "modulus", "19"));

    RFileWriter writer =
        RFile.newWriter().to(testFile).withFileSystem(localFs).withSampler(sc).build();
    writer.append(testData1.entrySet());
    writer.close();

    Scanner scanner = RFile.newScanner().from(testFile).withFileSystem(localFs).build();
    scanner.setSamplerConfiguration(sc);

    RowSampler rowSampler = new RowSampler();
    rowSampler.init(sc);

    SortedMap<Key,Value> sampleData = new TreeMap<>();
    for (Entry<Key,Value> e : testData1.entrySet()) {
      if (rowSampler.accept(e.getKey())) {
        sampleData.put(e.getKey(), e.getValue());
      }
    }

    assertTrue(sampleData.size() < testData1.size());

    assertEquals(sampleData, toMap(scanner));

    scanner.clearSamplerConfiguration();

    assertEquals(testData1, toMap(scanner));

  }

  @Test
  public void testAppendScanner() throws Exception {
    SortedMap<Key,Value> testData = createTestData(10000, 1, 1);
    String testFile = createRFile(testData);

    LocalFileSystem localFs = FileSystem.getLocal(new Configuration());

    Scanner scanner = RFile.newScanner().from(testFile).withFileSystem(localFs).build();

    String testFile2 = createTmpTestFile();
    RFileWriter writer = RFile.newWriter().to(testFile2).build();
    writer.append(scanner);
    writer.close();
    scanner.close();

    scanner = RFile.newScanner().from(testFile2).withFileSystem(localFs).build();
    assertEquals(testData, toMap(scanner));
    scanner.close();
  }

  @Test
  public void testCache() throws Exception {
    SortedMap<Key,Value> testData = createTestData(10000, 1, 1);
    String testFile = createRFile(testData);

    LocalFileSystem localFs = FileSystem.getLocal(new Configuration());
    Scanner scanner = RFile.newScanner().from(testFile).withFileSystem(localFs)
        .withIndexCache(1000000).withDataCache(10000000).build();

    RANDOM.get().ints(100, 0, 10_000).forEach(r -> {
      scanner.setRange(new Range(rowStr(r)));
      String actual = scanner.stream().collect(onlyElement()).getKey().getRow().toString();
      assertEquals(rowStr(r), actual);
    });

    scanner.close();
  }

  @Test
  public void testSummaries() throws Exception {
    SummarizerConfiguration sc1 =
        SummarizerConfiguration.builder(VisibilitySummarizer.class).build();
    SummarizerConfiguration sc2 = SummarizerConfiguration.builder(FamilySummarizer.class).build();

    LocalFileSystem localFs = FileSystem.getLocal(new Configuration());
    String testFile = createTmpTestFile();

    SortedMap<Key,Value> testData1 = createTestData(0, 100, 0, 4, 1, "A&B", "A&B&C");

    RFileWriter writer =
        RFile.newWriter().to(testFile).withFileSystem(localFs).withSummarizers(sc1, sc2).build();
    writer.append(testData1.entrySet());
    writer.close();

    // verify summary data
    Collection<Summary> summaries = RFile.summaries().from(testFile).withFileSystem(localFs).read();
    assertEquals(2, summaries.size());
    for (Summary summary : summaries) {
      assertEquals(0, summary.getFileStatistics().getInaccurate());
      assertEquals(1, summary.getFileStatistics().getTotal());
      String className = summary.getSummarizerConfiguration().getClassName();
      CounterSummary counterSummary = new CounterSummary(summary);
      if (className.equals(FamilySummarizer.class.getName())) {
        Map<String,Long> counters = counterSummary.getCounters();
        Map<String,Long> expected = Map.of("0000", 200L, "0001", 200L, "0002", 200L, "0003", 200L);
        assertEquals(expected, counters);
      } else if (className.equals(VisibilitySummarizer.class.getName())) {
        Map<String,Long> counters = counterSummary.getCounters();
        Map<String,Long> expected = Map.of("A&B", 400L, "A&B&C", 400L);
        assertEquals(expected, counters);
      } else {
        fail("Unexpected classname " + className);
      }
    }

    // check if writing summary data impacted normal rfile functionality
    Scanner scanner = RFile.newScanner().from(testFile).withFileSystem(localFs)
        .withAuthorizations(new Authorizations("A", "B", "C")).build();
    assertEquals(testData1, toMap(scanner));
    scanner.close();

    String testFile2 = createTmpTestFile();
    SortedMap<Key,Value> testData2 = createTestData(100, 100, 0, 4, 1, "A&B", "A&B&C");
    writer =
        RFile.newWriter().to(testFile2).withFileSystem(localFs).withSummarizers(sc1, sc2).build();
    writer.append(testData2.entrySet());
    writer.close();

    // verify reading summaries from multiple files works
    summaries = RFile.summaries().from(testFile, testFile2).withFileSystem(localFs).read();
    assertEquals(2, summaries.size());
    for (Summary summary : summaries) {
      assertEquals(0, summary.getFileStatistics().getInaccurate());
      assertEquals(2, summary.getFileStatistics().getTotal());
      String className = summary.getSummarizerConfiguration().getClassName();
      CounterSummary counterSummary = new CounterSummary(summary);
      if (className.equals(FamilySummarizer.class.getName())) {
        Map<String,Long> counters = counterSummary.getCounters();
        Map<String,Long> expected = Map.of("0000", 400L, "0001", 400L, "0002", 400L, "0003", 400L);
        assertEquals(expected, counters);
      } else if (className.equals(VisibilitySummarizer.class.getName())) {
        Map<String,Long> counters = counterSummary.getCounters();
        Map<String,Long> expected = Map.of("A&B", 800L, "A&B&C", 800L);
        assertEquals(expected, counters);
      } else {
        fail("Unexpected classname " + className);
      }
    }

    // verify reading a subset of summaries works
    summaries = RFile.summaries().from(testFile, testFile2).withFileSystem(localFs)
        .selectSummaries(sc -> sc.equals(sc1)).read();
    checkSummaries(summaries, Map.of("A&B", 800L, "A&B&C", 800L), 0);

    // the following test check boundary conditions for start row and end row
    summaries = RFile.summaries().from(testFile, testFile2).withFileSystem(localFs)
        .selectSummaries(sc -> sc.equals(sc1)).startRow(rowStr(99)).read();
    checkSummaries(summaries, Map.of("A&B", 400L, "A&B&C", 400L), 0);

    summaries = RFile.summaries().from(testFile, testFile2).withFileSystem(localFs)
        .selectSummaries(sc -> sc.equals(sc1)).startRow(rowStr(98)).read();
    checkSummaries(summaries, Map.of("A&B", 800L, "A&B&C", 800L), 1);

    summaries = RFile.summaries().from(testFile, testFile2).withFileSystem(localFs)
        .selectSummaries(sc -> sc.equals(sc1)).startRow(rowStr(0)).read();
    checkSummaries(summaries, Map.of("A&B", 800L, "A&B&C", 800L), 1);

    summaries = RFile.summaries().from(testFile, testFile2).withFileSystem(localFs)
        .selectSummaries(sc -> sc.equals(sc1)).startRow("#").read();
    checkSummaries(summaries, Map.of("A&B", 800L, "A&B&C", 800L), 0);

    summaries = RFile.summaries().from(testFile, testFile2).withFileSystem(localFs)
        .selectSummaries(sc -> sc.equals(sc1)).startRow(rowStr(100)).read();
    checkSummaries(summaries, Map.of("A&B", 400L, "A&B&C", 400L), 1);

    summaries = RFile.summaries().from(testFile, testFile2).withFileSystem(localFs)
        .selectSummaries(sc -> sc.equals(sc1)).endRow(rowStr(99)).read();
    checkSummaries(summaries, Map.of("A&B", 400L, "A&B&C", 400L), 0);

    summaries = RFile.summaries().from(testFile, testFile2).withFileSystem(localFs)
        .selectSummaries(sc -> sc.equals(sc1)).endRow(rowStr(100)).read();
    checkSummaries(summaries, Map.of("A&B", 800L, "A&B&C", 800L), 1);

    summaries = RFile.summaries().from(testFile, testFile2).withFileSystem(localFs)
        .selectSummaries(sc -> sc.equals(sc1)).endRow(rowStr(199)).read();
    checkSummaries(summaries, Map.of("A&B", 800L, "A&B&C", 800L), 0);

    summaries = RFile.summaries().from(testFile, testFile2).withFileSystem(localFs)
        .selectSummaries(sc -> sc.equals(sc1)).startRow(rowStr(50)).endRow(rowStr(150)).read();
    checkSummaries(summaries, Map.of("A&B", 800L, "A&B&C", 800L), 2);

    summaries = RFile.summaries().from(testFile, testFile2).withFileSystem(localFs)
        .selectSummaries(sc -> sc.equals(sc1)).startRow(rowStr(120)).endRow(rowStr(150)).read();
    checkSummaries(summaries, Map.of("A&B", 400L, "A&B&C", 400L), 1);

    summaries = RFile.summaries().from(testFile, testFile2).withFileSystem(localFs)
        .selectSummaries(sc -> sc.equals(sc1)).startRow(rowStr(50)).endRow(rowStr(199)).read();
    checkSummaries(summaries, Map.of("A&B", 800L, "A&B&C", 800L), 1);

    summaries = RFile.summaries().from(testFile, testFile2).withFileSystem(localFs)
        .selectSummaries(sc -> sc.equals(sc1)).startRow("#").endRow(rowStr(150)).read();
    checkSummaries(summaries, Map.of("A&B", 800L, "A&B&C", 800L), 1);

    summaries = RFile.summaries().from(testFile, testFile2).withFileSystem(localFs)
        .selectSummaries(sc -> sc.equals(sc1)).startRow(rowStr(199)).read();
    checkSummaries(summaries, Map.of(), 0);
    summaries = RFile.summaries().from(testFile, testFile2).withFileSystem(localFs)
        .selectSummaries(sc -> sc.equals(sc1)).startRow(rowStr(200)).read();
    checkSummaries(summaries, Map.of(), 0);

    summaries = RFile.summaries().from(testFile, testFile2).withFileSystem(localFs)
        .selectSummaries(sc -> sc.equals(sc1)).endRow("#").read();
    checkSummaries(summaries, Map.of(), 0);

    summaries = RFile.summaries().from(testFile, testFile2).withFileSystem(localFs)
        .selectSummaries(sc -> sc.equals(sc1)).endRow(rowStr(0)).read();
    checkSummaries(summaries, Map.of("A&B", 400L, "A&B&C", 400L), 1);
  }

  private void checkSummaries(Collection<Summary> summaries, Map<String,Long> expected, int extra) {
    assertEquals(1, summaries.size());
    for (Summary summary : summaries) {
      assertEquals(extra, summary.getFileStatistics().getInaccurate());
      assertEquals(extra, summary.getFileStatistics().getExtra());
      assertEquals(2, summary.getFileStatistics().getTotal());
      String className = summary.getSummarizerConfiguration().getClassName();
      CounterSummary counterSummary = new CounterSummary(summary);
      if (className.equals(VisibilitySummarizer.class.getName())) {
        Map<String,Long> counters = counterSummary.getCounters();

        assertEquals(expected, counters);
      } else {
        fail("Unexpected classname " + className);
      }
    }
  }

  @Test
  public void testOutOfOrder() throws Exception {
    // test that exception declared in API is thrown
    Key k1 = new Key("r1", "f1", "q1");
    Value v1 = new Value("1");

    Key k2 = new Key("r2", "f1", "q1");
    Value v2 = new Value("2");

    LocalFileSystem localFs = FileSystem.getLocal(new Configuration());
    String testFile = createTmpTestFile();
    try (RFileWriter writer = RFile.newWriter().to(testFile).withFileSystem(localFs).build()) {
      writer.append(k2, v2);
      assertThrows(IllegalArgumentException.class, () -> writer.append(k1, v1));
    }
  }

  @Test
  public void testOutOfOrderIterable() throws Exception {
    // test that exception declared in API is thrown
    Key k1 = new Key("r1", "f1", "q1");
    Value v1 = new Value("1");

    Key k2 = new Key("r2", "f1", "q1");
    Value v2 = new Value("2");

    ArrayList<Entry<Key,Value>> data = new ArrayList<>();
    data.add(new AbstractMap.SimpleEntry<>(k2, v2));
    data.add(new AbstractMap.SimpleEntry<>(k1, v1));

    LocalFileSystem localFs = FileSystem.getLocal(new Configuration());
    String testFile = createTmpTestFile();
    try (RFileWriter writer = RFile.newWriter().to(testFile).withFileSystem(localFs).build()) {
      assertThrows(IllegalArgumentException.class, () -> writer.append(data));
    }
  }

  @Test
  public void testBadVis() throws Exception {
    // this test has two purposes ensure an exception is thrown and ensure the exception document in
    // the javadoc is thrown
    LocalFileSystem localFs = FileSystem.getLocal(new Configuration());
    String testFile = createTmpTestFile();
    try (RFileWriter writer = RFile.newWriter().to(testFile).withFileSystem(localFs).build()) {
      writer.startDefaultLocalityGroup();
      Key k1 = new Key("r1", "f1", "q1", "(A&(B");
      assertThrows(IllegalArgumentException.class, () -> writer.append(k1, new Value("")));
    }
  }

  @Test
  public void testBadVisIterable() throws Exception {
    // test append(iterable) method
    LocalFileSystem localFs = FileSystem.getLocal(new Configuration());
    String testFile = createTmpTestFile();
    try (RFileWriter writer = RFile.newWriter().to(testFile).withFileSystem(localFs).build()) {
      writer.startDefaultLocalityGroup();
      Key k1 = new Key("r1", "f1", "q1", "(A&(B");
      Entry<Key,Value> entry = new AbstractMap.SimpleEntry<>(k1, new Value(""));
      assertThrows(IllegalArgumentException.class,
          () -> writer.append(Collections.singletonList(entry)));
    }
  }

  @Test
  public void testDoubleStart() throws Exception {
    LocalFileSystem localFs = FileSystem.getLocal(new Configuration());
    String testFile = createTmpTestFile();
    try (RFileWriter writer = RFile.newWriter().to(testFile).withFileSystem(localFs).build()) {
      writer.startDefaultLocalityGroup();
      assertThrows(IllegalStateException.class, writer::startDefaultLocalityGroup);
    }
  }

  @Test
  public void testAppendStartDefault() throws Exception {
    LocalFileSystem localFs = FileSystem.getLocal(new Configuration());
    String testFile = createTmpTestFile();
    try (RFileWriter writer = RFile.newWriter().to(testFile).withFileSystem(localFs).build()) {
      writer.append(new Key("r1", "f1", "q1"), new Value("1"));
      assertThrows(IllegalStateException.class, writer::startDefaultLocalityGroup);
    }
  }

  @Test
  public void testStartAfter() throws Exception {
    LocalFileSystem localFs = FileSystem.getLocal(new Configuration());
    String testFile = createTmpTestFile();
    try (RFileWriter writer = RFile.newWriter().to(testFile).withFileSystem(localFs).build()) {
      Key k1 = new Key("r1", "f1", "q1");
      writer.append(k1, new Value(""));
      assertThrows(IllegalStateException.class, () -> writer.startNewLocalityGroup("lg1", "fam1"));
    }
  }

  @Test
  public void testIllegalColumn() throws Exception {
    LocalFileSystem localFs = FileSystem.getLocal(new Configuration());
    String testFile = createTmpTestFile();
    try (RFileWriter writer = RFile.newWriter().to(testFile).withFileSystem(localFs).build()) {
      writer.startNewLocalityGroup("lg1", "fam1");
      Key k1 = new Key("r1", "f1", "q1");
      // should not be able to append the column family f1
      assertThrows(IllegalArgumentException.class, () -> writer.append(k1, new Value("")));
    }
  }

  @Test
  public void testWrongGroup() throws Exception {
    LocalFileSystem localFs = FileSystem.getLocal(new Configuration());
    String testFile = createTmpTestFile();
    try (RFileWriter writer = RFile.newWriter().to(testFile).withFileSystem(localFs).build()) {
      writer.startNewLocalityGroup("lg1", "fam1");
      Key k1 = new Key("r1", "fam1", "q1");
      writer.append(k1, new Value(""));
      writer.startDefaultLocalityGroup();
      // should not be able to append the column family fam1 to default locality group
      Key k2 = new Key("r1", "fam1", "q2");
      assertThrows(IllegalArgumentException.class, () -> writer.append(k2, new Value("")));
    }
  }

  private RFileSKVIterator getReader(LocalFileSystem localFs, UnreferencedTabletFile testFile)
      throws IOException {
    return (RFileSKVIterator) FileOperations.getInstance().newReaderBuilder()
        .forFile(testFile, localFs, localFs.getConf(), NoCryptoServiceFactory.NONE)
        .withTableConfiguration(DefaultConfiguration.getInstance()).build();
  }

  @Test
  public void testMultipleFilesAndCache() throws Exception {
    SortedMap<Key,Value> testData = createTestData(100, 10, 10);
    List<String> files =
        Arrays.asList(createTmpTestFile(), createTmpTestFile(), createTmpTestFile());

    LocalFileSystem localFs = FileSystem.getLocal(new Configuration());

    for (int i = 0; i < files.size(); i++) {
      try (
          RFileWriter writer = RFile.newWriter().to(files.get(i)).withFileSystem(localFs).build()) {
        for (Entry<Key,Value> entry : testData.entrySet()) {
          if (entry.getKey().hashCode() % files.size() == i) {
            writer.append(entry.getKey(), entry.getValue());
          }
        }
      }
    }

    Scanner scanner = RFile.newScanner().from(files.toArray(new String[files.size()]))
        .withFileSystem(localFs).withIndexCache(1000000).withDataCache(10000000).build();
    assertEquals(testData, toMap(scanner));
    scanner.close();
  }

  @Test
  public void testFileSystemFromUri() throws Exception {
    String localFsClass = "LocalFileSystem";

    String remoteFsHost = "127.0.0.5:8080";
    String fileUri = "hdfs://" + remoteFsHost + "/bulk-xyx/file1.rf";
    // There was a bug in the code where the default hadoop file system was always used. This test
    // checks that the hadoop filesystem used it based on the URI and not the default filesystem. In
    // this env the default file system is the local hadoop file system.
    var exception =
        assertThrows(ConnectException.class, () -> RFile.newWriter().to(fileUri).build());
    assertTrue(exception.getMessage().contains("to " + remoteFsHost
        + " failed on connection exception: java.net.ConnectException: Connection refused"));
    // Ensure the DistributedFileSystem was used.
    assertTrue(Arrays.stream(exception.getStackTrace())
        .anyMatch(ste -> ste.getClassName().contains(DistributedFileSystem.class.getName())));
    assertTrue(Arrays.stream(exception.getStackTrace())
        .noneMatch(ste -> ste.getClassName().contains(localFsClass)));

    var exception2 = assertThrows(RuntimeException.class, () -> {
      var scanner = RFile.newScanner().from(fileUri).build();
      scanner.iterator();
    });
    assertTrue(exception2.getMessage().contains("to " + remoteFsHost
        + " failed on connection exception: java.net.ConnectException: Connection refused"));
    assertTrue(Arrays.stream(exception2.getCause().getStackTrace())
        .anyMatch(ste -> ste.getClassName().contains(DistributedFileSystem.class.getName())));
    assertTrue(Arrays.stream(exception2.getCause().getStackTrace())
        .noneMatch(ste -> ste.getClassName().contains(localFsClass)));

    // verify the assumptions this test is making about the local filesystem being the default.
    var exception3 = assertThrows(IllegalArgumentException.class,
        () -> FileSystem.get(new Configuration()).open(new Path(fileUri)));
    assertTrue(exception3.getMessage().contains("Wrong FS: " + fileUri + ", expected: file:///"));
    assertTrue(Arrays.stream(exception3.getStackTrace())
        .anyMatch(ste -> ste.getClassName().contains(localFsClass)));
  }

  @Test
  public void testLoadPlanEmpty() throws Exception {
    LocalFileSystem localFs = FileSystem.getLocal(new Configuration());

    LoadPlan.SplitResolver splitResolver =
        LoadPlan.SplitResolver.from(new TreeSet<>(List.of(new Text("m"))));

    for (boolean withSplits : List.of(true, false)) {
      String testFile = createTmpTestFile();
      var builder = RFile.newWriter().to(testFile).withFileSystem(localFs);
      if (withSplits) {
        builder = builder.withSplitResolver(splitResolver);
      }
      var writer = builder.build();

      // can not get load plan before closing file
      assertThrows(IllegalStateException.class,
          () -> writer.getLoadPlan(new Path(testFile).getName()));

      try (writer) {
        writer.startDefaultLocalityGroup();
        assertThrows(IllegalStateException.class,
            () -> writer.getLoadPlan(new Path(testFile).getName()));
      }
      var loadPlan = writer.getLoadPlan(new Path(testFile).getName());
      assertEquals(0, loadPlan.getDestinations().size());

      loadPlan = LoadPlan.compute(new URI(testFile), splitResolver);
      assertEquals(0, loadPlan.getDestinations().size());
    }
  }

  @Test
  public void testLoadPlanLocalityGroupsNoSplits() throws Exception {
    LocalFileSystem localFs = FileSystem.getLocal(new Configuration());

    String testFile = createTmpTestFile();
    var writer = RFile.newWriter().to(testFile).withFileSystem(localFs).build();
    try (writer) {
      writer.startNewLocalityGroup("LG1", "F1");
      writer.append(new Key("001", "F1"), "V1");
      writer.append(new Key("005", "F1"), "V2");
      writer.startNewLocalityGroup("LG2", "F3");
      writer.append(new Key("003", "F3"), "V3");
      writer.append(new Key("004", "F3"), "V4");
      writer.startDefaultLocalityGroup();
      writer.append(new Key("007", "F4"), "V5");
      writer.append(new Key("009", "F4"), "V6");
    }

    var filename = new Path(testFile).getName();
    var loadPlan = writer.getLoadPlan(filename);
    assertEquals(1, loadPlan.getDestinations().size());

    // The minimum and maximum rows happend in different locality groups, the load plan should
    // reflect this
    var expectedLoadPlan =
        LoadPlan.builder().loadFileTo(filename, LoadPlan.RangeType.FILE, "001", "009").build();
    assertEquals(expectedLoadPlan.toJson(), loadPlan.toJson());
  }

  @Test
  public void testLoadPlanLocalityGroupsSplits() throws Exception {
    LocalFileSystem localFs = FileSystem.getLocal(new Configuration());

    SortedSet<Text> splits =
        Stream.of("001", "002", "003", "004", "005", "006", "007", "008", "009").map(Text::new)
            .collect(Collectors.toCollection(TreeSet::new));
    var splitResolver = LoadPlan.SplitResolver.from(splits);

    String testFile = createTmpTestFile();
    var writer = RFile.newWriter().to(testFile).withFileSystem(localFs)
        .withSplitResolver(splitResolver).build();
    try (writer) {
      writer.startNewLocalityGroup("LG1", "F1");
      writer.append(new Key("001", "F1"), "V1");
      writer.append(new Key("005", "F1"), "V2");
      writer.startNewLocalityGroup("LG2", "F3");
      writer.append(new Key("003", "F3"), "V3");
      writer.append(new Key("005", "F3"), "V3");
      writer.append(new Key("007", "F3"), "V4");
      writer.startDefaultLocalityGroup();
      writer.append(new Key("007", "F4"), "V5");
      writer.append(new Key("009", "F4"), "V6");
    }

    var filename = new Path(testFile).getName();
    var loadPlan = writer.getLoadPlan(filename);
    assertEquals(5, loadPlan.getDestinations().size());

    var builder = LoadPlan.builder();
    builder.loadFileTo(filename, LoadPlan.RangeType.TABLE, null, "001");
    builder.loadFileTo(filename, LoadPlan.RangeType.TABLE, "004", "005");
    builder.loadFileTo(filename, LoadPlan.RangeType.TABLE, "002", "003");
    builder.loadFileTo(filename, LoadPlan.RangeType.TABLE, "006", "007");
    builder.loadFileTo(filename, LoadPlan.RangeType.TABLE, "008", "009");
    assertEquals(LoadPlanTest.toString(builder.build().getDestinations()),
        LoadPlanTest.toString(loadPlan.getDestinations()));

    loadPlan = LoadPlan.compute(new URI(testFile), splitResolver);
    assertEquals(LoadPlanTest.toString(builder.build().getDestinations()),
        LoadPlanTest.toString(loadPlan.getDestinations()));
  }

  @Test
  public void testIncorrectSplitResolver() throws Exception {
    // for some rows the returns table splits will not contain the row. This should cause an error.
    LoadPlan.SplitResolver splitResolver =
        row -> new LoadPlan.TableSplits(new Text("003"), new Text("005"));

    LocalFileSystem localFs = FileSystem.getLocal(new Configuration());

    String testFile = createTmpTestFile();
    var writer = RFile.newWriter().to(testFile).withFileSystem(localFs)
        .withSplitResolver(splitResolver).build();
    try (writer) {
      writer.startDefaultLocalityGroup();
      writer.append(new Key("004", "F4"), "V2");
      var e = assertThrows(IllegalStateException.class,
          () -> writer.append(new Key("007", "F4"), "V2"));
      assertTrue(e.getMessage().contains("(003,005]"));
      assertTrue(e.getMessage().contains("007"));
    }

    var testFile2 = createTmpTestFile();
    var writer2 = RFile.newWriter().to(testFile2).withFileSystem(localFs).build();
    try (writer2) {
      writer2.startDefaultLocalityGroup();
      writer2.append(new Key("004", "F4"), "V2");
      writer2.append(new Key("007", "F4"), "V2");
    }

    var e = assertThrows(IllegalStateException.class,
        () -> LoadPlan.compute(new URI(testFile), splitResolver));
    assertTrue(e.getMessage().contains("(003,005]"));
    assertTrue(e.getMessage().contains("007"));
  }

  @Test
  public void testGetLoadPlanBeforeClose() throws Exception {
    LocalFileSystem localFs = FileSystem.getLocal(new Configuration());

    String testFile = createTmpTestFile();
    var writer = RFile.newWriter().to(testFile).withFileSystem(localFs).build();
    try (writer) {
      var e = assertThrows(IllegalStateException.class,
          () -> writer.getLoadPlan(new Path(testFile).getName()));
      assertEquals("Attempted to get load plan before closing", e.getMessage());
      writer.startDefaultLocalityGroup();
      writer.append(new Key("004", "F4"), "V2");
      var e2 = assertThrows(IllegalStateException.class,
          () -> writer.getLoadPlan(new Path(testFile).getName()));
      assertEquals("Attempted to get load plan before closing", e2.getMessage());
    }
  }

  @Test
  public void testGetLoadPlanWithPath() throws Exception {
    LocalFileSystem localFs = FileSystem.getLocal(new Configuration());

    String testFile = createTmpTestFile();
    var writer = RFile.newWriter().to(testFile).withFileSystem(localFs).build();
    writer.close();

    var e =
        assertThrows(IllegalArgumentException.class, () -> writer.getLoadPlan(testFile.toString()));
    assertTrue(e.getMessage().contains("Unexpected path"));
    assertEquals(0, writer.getLoadPlan(new Path(testFile).getName()).getDestinations().size());
  }

  @Test
  public void testComputeLoadPlanWithPath() throws Exception {
    LocalFileSystem localFs = FileSystem.getLocal(new Configuration());

    SortedSet<Text> splits =
        Stream.of("001", "002", "003", "004", "005", "006", "007", "008", "009").map(Text::new)
            .collect(Collectors.toCollection(TreeSet::new));
    var splitResolver = LoadPlan.SplitResolver.from(splits);

    String testFile = createTmpTestFile();
    var writer = RFile.newWriter().to(testFile).withFileSystem(localFs)
        .withSplitResolver(splitResolver).build();
    writer.startDefaultLocalityGroup();
    writer.append(new Key("001", "V4"), "test");
    writer.append(new Key("002", "V4"), "test");
    writer.append(new Key("003", "V4"), "test");
    writer.append(new Key("004", "V4"), "test");
    writer.close();

    var e = assertThrows(IllegalArgumentException.class, () -> writer.getLoadPlan(testFile));
    assertTrue(e.getMessage().contains("Unexpected path"));
    assertEquals(4, writer.getLoadPlan(new Path(testFile).getName()).getDestinations().size());
    assertEquals(4, LoadPlan.compute(new URI(testFile), splitResolver).getDestinations().size());

    String hdfsHost = "127.0.0.5:8080";
    String fileUri = "hdfs://" + hdfsHost + "/bulk-xyx/file1.rf";
    URI uri = new URI(fileUri);
    var err = assertThrows(RuntimeException.class, () -> LoadPlan.compute(uri, splitResolver));
    assertTrue(err.getMessage().contains("to " + hdfsHost + " failed on connection exception"));
    assertTrue(Arrays.stream(err.getCause().getStackTrace())
        .anyMatch(ste -> ste.getClassName().contains(DistributedFileSystem.class.getName())));
  }
}
