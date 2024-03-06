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
package org.apache.accumulo.core.iterators;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Collection;
import java.util.Map;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.io.Text;

public class AdamsCounter implements SortedKeyValueIterator<Key,Value> {

  private SortedKeyValueIterator<Key,Value> source;
  private Key key;
  private Value value;

  private Text currentRow = new Text();

  @Override
  public void init(SortedKeyValueIterator<Key,Value> source, Map<String,String> options,
      IteratorEnvironment env) throws IOException {
    this.source = source;
  }

  @Override
  public boolean hasTop() {
    return key != null;
  }

  @Override
  public void next() throws IOException {
    if (source.hasTop()) {
      currentRow = source.getTopKey().getRow(currentRow);
      long ts = source.getTopKey().getTimestamp();
      this.key = createKey(source.getTopKey());
      long count = Long.parseLong(source.getTopValue().toString());
      this.value = new Value(Long.toString(count));

      source.next();
      while (source.hasTop() && this.key.compareTo(createKey(source.getTopKey())) == 0) {
        long nextCount = Long.parseLong(source.getTopValue().toString());
        count = count + nextCount;
        this.value = new Value(Long.toString(count));
        source.next();
      }
    } else {
      this.key = null;
      this.value = null;
    }
  }

  private Key createKey(Key topKey) {
    Text currentRow = source.getTopKey().getRow();
    String[] currentRowSplit = currentRow.toString().split("\0");
    ByteSequence currentColf = source.getTopKey().getColumnFamilyData();
    long ts = source.getTopKey().getTimestamp();
    return new Key(
        (currentRowSplit[0] + '\0' + currentRowSplit[1]).getBytes(Charset.defaultCharset()),
        currentColf.toArray(), new byte[0], new byte[0], ts);
  }

  @Override
  public void seek(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive)
      throws IOException {
    source.seek(range, columnFamilies, inclusive);
    next();
  }

  @Override
  public Key getTopKey() {
    return key;
  }

  @Override
  public Value getTopValue() {
    return value;
  }

  @Override
  public SortedKeyValueIterator<Key,Value> deepCopy(IteratorEnvironment env) {
    return null;
  }

}
