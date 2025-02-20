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
/**
 * Autogenerated by Thrift Compiler (0.17.0)
 *
 * DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING
 *  @generated
 */
package org.apache.accumulo.core.compaction.thrift;

@SuppressWarnings({"cast", "rawtypes", "serial", "unchecked", "unused"})
public class TExternalCompactionList implements org.apache.thrift.TBase<TExternalCompactionList, TExternalCompactionList._Fields>, java.io.Serializable, Cloneable, Comparable<TExternalCompactionList> {
  private static final org.apache.thrift.protocol.TStruct STRUCT_DESC = new org.apache.thrift.protocol.TStruct("TExternalCompactionList");

  private static final org.apache.thrift.protocol.TField COMPACTIONS_FIELD_DESC = new org.apache.thrift.protocol.TField("compactions", org.apache.thrift.protocol.TType.LIST, (short)1);

  private static final org.apache.thrift.scheme.SchemeFactory STANDARD_SCHEME_FACTORY = new TExternalCompactionListStandardSchemeFactory();
  private static final org.apache.thrift.scheme.SchemeFactory TUPLE_SCHEME_FACTORY = new TExternalCompactionListTupleSchemeFactory();

  public @org.apache.thrift.annotation.Nullable java.util.List<TExternalCompaction> compactions; // required

  /** The set of fields this struct contains, along with convenience methods for finding and manipulating them. */
  public enum _Fields implements org.apache.thrift.TFieldIdEnum {
    COMPACTIONS((short)1, "compactions");

    private static final java.util.Map<java.lang.String, _Fields> byName = new java.util.HashMap<java.lang.String, _Fields>();

    static {
      for (_Fields field : java.util.EnumSet.allOf(_Fields.class)) {
        byName.put(field.getFieldName(), field);
      }
    }

    /**
     * Find the _Fields constant that matches fieldId, or null if its not found.
     */
    @org.apache.thrift.annotation.Nullable
    public static _Fields findByThriftId(int fieldId) {
      switch(fieldId) {
        case 1: // COMPACTIONS
          return COMPACTIONS;
        default:
          return null;
      }
    }

    /**
     * Find the _Fields constant that matches fieldId, throwing an exception
     * if it is not found.
     */
    public static _Fields findByThriftIdOrThrow(int fieldId) {
      _Fields fields = findByThriftId(fieldId);
      if (fields == null) throw new java.lang.IllegalArgumentException("Field " + fieldId + " doesn't exist!");
      return fields;
    }

    /**
     * Find the _Fields constant that matches name, or null if its not found.
     */
    @org.apache.thrift.annotation.Nullable
    public static _Fields findByName(java.lang.String name) {
      return byName.get(name);
    }

    private final short _thriftId;
    private final java.lang.String _fieldName;

    _Fields(short thriftId, java.lang.String fieldName) {
      _thriftId = thriftId;
      _fieldName = fieldName;
    }

    @Override
    public short getThriftFieldId() {
      return _thriftId;
    }

    @Override
    public java.lang.String getFieldName() {
      return _fieldName;
    }
  }

  // isset id assignments
  public static final java.util.Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> metaDataMap;
  static {
    java.util.Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> tmpMap = new java.util.EnumMap<_Fields, org.apache.thrift.meta_data.FieldMetaData>(_Fields.class);
    tmpMap.put(_Fields.COMPACTIONS, new org.apache.thrift.meta_data.FieldMetaData("compactions", org.apache.thrift.TFieldRequirementType.DEFAULT, 
        new org.apache.thrift.meta_data.ListMetaData(org.apache.thrift.protocol.TType.LIST, 
            new org.apache.thrift.meta_data.StructMetaData(org.apache.thrift.protocol.TType.STRUCT, TExternalCompaction.class))));
    metaDataMap = java.util.Collections.unmodifiableMap(tmpMap);
    org.apache.thrift.meta_data.FieldMetaData.addStructMetaDataMap(TExternalCompactionList.class, metaDataMap);
  }

  public TExternalCompactionList() {
  }

  public TExternalCompactionList(
    java.util.List<TExternalCompaction> compactions)
  {
    this();
    this.compactions = compactions;
  }

  /**
   * Performs a deep copy on <i>other</i>.
   */
  public TExternalCompactionList(TExternalCompactionList other) {
    if (other.isSetCompactions()) {
      java.util.List<TExternalCompaction> __this__compactions = new java.util.ArrayList<TExternalCompaction>(other.compactions.size());
      for (TExternalCompaction other_element : other.compactions) {
        __this__compactions.add(new TExternalCompaction(other_element));
      }
      this.compactions = __this__compactions;
    }
  }

  @Override
  public TExternalCompactionList deepCopy() {
    return new TExternalCompactionList(this);
  }

  @Override
  public void clear() {
    this.compactions = null;
  }

  public int getCompactionsSize() {
    return (this.compactions == null) ? 0 : this.compactions.size();
  }

  @org.apache.thrift.annotation.Nullable
  public java.util.Iterator<TExternalCompaction> getCompactionsIterator() {
    return (this.compactions == null) ? null : this.compactions.iterator();
  }

  public void addToCompactions(TExternalCompaction elem) {
    if (this.compactions == null) {
      this.compactions = new java.util.ArrayList<TExternalCompaction>();
    }
    this.compactions.add(elem);
  }

  @org.apache.thrift.annotation.Nullable
  public java.util.List<TExternalCompaction> getCompactions() {
    return this.compactions;
  }

  public TExternalCompactionList setCompactions(@org.apache.thrift.annotation.Nullable java.util.List<TExternalCompaction> compactions) {
    this.compactions = compactions;
    return this;
  }

  public void unsetCompactions() {
    this.compactions = null;
  }

  /** Returns true if field compactions is set (has been assigned a value) and false otherwise */
  public boolean isSetCompactions() {
    return this.compactions != null;
  }

  public void setCompactionsIsSet(boolean value) {
    if (!value) {
      this.compactions = null;
    }
  }

  @Override
  public void setFieldValue(_Fields field, @org.apache.thrift.annotation.Nullable java.lang.Object value) {
    switch (field) {
    case COMPACTIONS:
      if (value == null) {
        unsetCompactions();
      } else {
        setCompactions((java.util.List<TExternalCompaction>)value);
      }
      break;

    }
  }

  @org.apache.thrift.annotation.Nullable
  @Override
  public java.lang.Object getFieldValue(_Fields field) {
    switch (field) {
    case COMPACTIONS:
      return getCompactions();

    }
    throw new java.lang.IllegalStateException();
  }

  /** Returns true if field corresponding to fieldID is set (has been assigned a value) and false otherwise */
  @Override
  public boolean isSet(_Fields field) {
    if (field == null) {
      throw new java.lang.IllegalArgumentException();
    }

    switch (field) {
    case COMPACTIONS:
      return isSetCompactions();
    }
    throw new java.lang.IllegalStateException();
  }

  @Override
  public boolean equals(java.lang.Object that) {
    if (that instanceof TExternalCompactionList)
      return this.equals((TExternalCompactionList)that);
    return false;
  }

  public boolean equals(TExternalCompactionList that) {
    if (that == null)
      return false;
    if (this == that)
      return true;

    boolean this_present_compactions = true && this.isSetCompactions();
    boolean that_present_compactions = true && that.isSetCompactions();
    if (this_present_compactions || that_present_compactions) {
      if (!(this_present_compactions && that_present_compactions))
        return false;
      if (!this.compactions.equals(that.compactions))
        return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    int hashCode = 1;

    hashCode = hashCode * 8191 + ((isSetCompactions()) ? 131071 : 524287);
    if (isSetCompactions())
      hashCode = hashCode * 8191 + compactions.hashCode();

    return hashCode;
  }

  @Override
  public int compareTo(TExternalCompactionList other) {
    if (!getClass().equals(other.getClass())) {
      return getClass().getName().compareTo(other.getClass().getName());
    }

    int lastComparison = 0;

    lastComparison = java.lang.Boolean.compare(isSetCompactions(), other.isSetCompactions());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetCompactions()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.compactions, other.compactions);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    return 0;
  }

  @org.apache.thrift.annotation.Nullable
  @Override
  public _Fields fieldForId(int fieldId) {
    return _Fields.findByThriftId(fieldId);
  }

  @Override
  public void read(org.apache.thrift.protocol.TProtocol iprot) throws org.apache.thrift.TException {
    scheme(iprot).read(iprot, this);
  }

  @Override
  public void write(org.apache.thrift.protocol.TProtocol oprot) throws org.apache.thrift.TException {
    scheme(oprot).write(oprot, this);
  }

  @Override
  public java.lang.String toString() {
    java.lang.StringBuilder sb = new java.lang.StringBuilder("TExternalCompactionList(");
    boolean first = true;

    sb.append("compactions:");
    if (this.compactions == null) {
      sb.append("null");
    } else {
      sb.append(this.compactions);
    }
    first = false;
    sb.append(")");
    return sb.toString();
  }

  public void validate() throws org.apache.thrift.TException {
    // check for required fields
    // check for sub-struct validity
  }

  private void writeObject(java.io.ObjectOutputStream out) throws java.io.IOException {
    try {
      write(new org.apache.thrift.protocol.TCompactProtocol(new org.apache.thrift.transport.TIOStreamTransport(out)));
    } catch (org.apache.thrift.TException te) {
      throw new java.io.IOException(te);
    }
  }

  private void readObject(java.io.ObjectInputStream in) throws java.io.IOException, java.lang.ClassNotFoundException {
    try {
      read(new org.apache.thrift.protocol.TCompactProtocol(new org.apache.thrift.transport.TIOStreamTransport(in)));
    } catch (org.apache.thrift.TException te) {
      throw new java.io.IOException(te);
    }
  }

  private static class TExternalCompactionListStandardSchemeFactory implements org.apache.thrift.scheme.SchemeFactory {
    @Override
    public TExternalCompactionListStandardScheme getScheme() {
      return new TExternalCompactionListStandardScheme();
    }
  }

  private static class TExternalCompactionListStandardScheme extends org.apache.thrift.scheme.StandardScheme<TExternalCompactionList> {

    @Override
    public void read(org.apache.thrift.protocol.TProtocol iprot, TExternalCompactionList struct) throws org.apache.thrift.TException {
      org.apache.thrift.protocol.TField schemeField;
      iprot.readStructBegin();
      while (true)
      {
        schemeField = iprot.readFieldBegin();
        if (schemeField.type == org.apache.thrift.protocol.TType.STOP) { 
          break;
        }
        switch (schemeField.id) {
          case 1: // COMPACTIONS
            if (schemeField.type == org.apache.thrift.protocol.TType.LIST) {
              {
                org.apache.thrift.protocol.TList _list10 = iprot.readListBegin();
                struct.compactions = new java.util.ArrayList<TExternalCompaction>(_list10.size);
                @org.apache.thrift.annotation.Nullable TExternalCompaction _elem11;
                for (int _i12 = 0; _i12 < _list10.size; ++_i12)
                {
                  _elem11 = new TExternalCompaction();
                  _elem11.read(iprot);
                  struct.compactions.add(_elem11);
                }
                iprot.readListEnd();
              }
              struct.setCompactionsIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          default:
            org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
        }
        iprot.readFieldEnd();
      }
      iprot.readStructEnd();

      // check for required fields of primitive type, which can't be checked in the validate method
      struct.validate();
    }

    @Override
    public void write(org.apache.thrift.protocol.TProtocol oprot, TExternalCompactionList struct) throws org.apache.thrift.TException {
      struct.validate();

      oprot.writeStructBegin(STRUCT_DESC);
      if (struct.compactions != null) {
        oprot.writeFieldBegin(COMPACTIONS_FIELD_DESC);
        {
          oprot.writeListBegin(new org.apache.thrift.protocol.TList(org.apache.thrift.protocol.TType.STRUCT, struct.compactions.size()));
          for (TExternalCompaction _iter13 : struct.compactions)
          {
            _iter13.write(oprot);
          }
          oprot.writeListEnd();
        }
        oprot.writeFieldEnd();
      }
      oprot.writeFieldStop();
      oprot.writeStructEnd();
    }

  }

  private static class TExternalCompactionListTupleSchemeFactory implements org.apache.thrift.scheme.SchemeFactory {
    @Override
    public TExternalCompactionListTupleScheme getScheme() {
      return new TExternalCompactionListTupleScheme();
    }
  }

  private static class TExternalCompactionListTupleScheme extends org.apache.thrift.scheme.TupleScheme<TExternalCompactionList> {

    @Override
    public void write(org.apache.thrift.protocol.TProtocol prot, TExternalCompactionList struct) throws org.apache.thrift.TException {
      org.apache.thrift.protocol.TTupleProtocol oprot = (org.apache.thrift.protocol.TTupleProtocol) prot;
      java.util.BitSet optionals = new java.util.BitSet();
      if (struct.isSetCompactions()) {
        optionals.set(0);
      }
      oprot.writeBitSet(optionals, 1);
      if (struct.isSetCompactions()) {
        {
          oprot.writeI32(struct.compactions.size());
          for (TExternalCompaction _iter14 : struct.compactions)
          {
            _iter14.write(oprot);
          }
        }
      }
    }

    @Override
    public void read(org.apache.thrift.protocol.TProtocol prot, TExternalCompactionList struct) throws org.apache.thrift.TException {
      org.apache.thrift.protocol.TTupleProtocol iprot = (org.apache.thrift.protocol.TTupleProtocol) prot;
      java.util.BitSet incoming = iprot.readBitSet(1);
      if (incoming.get(0)) {
        {
          org.apache.thrift.protocol.TList _list15 = iprot.readListBegin(org.apache.thrift.protocol.TType.STRUCT);
          struct.compactions = new java.util.ArrayList<TExternalCompaction>(_list15.size);
          @org.apache.thrift.annotation.Nullable TExternalCompaction _elem16;
          for (int _i17 = 0; _i17 < _list15.size; ++_i17)
          {
            _elem16 = new TExternalCompaction();
            _elem16.read(iprot);
            struct.compactions.add(_elem16);
          }
        }
        struct.setCompactionsIsSet(true);
      }
    }
  }

  private static <S extends org.apache.thrift.scheme.IScheme> S scheme(org.apache.thrift.protocol.TProtocol proto) {
    return (org.apache.thrift.scheme.StandardScheme.class.equals(proto.getScheme()) ? STANDARD_SCHEME_FACTORY : TUPLE_SCHEME_FACTORY).getScheme();
  }
  private static void unusedMethod() {}
}

