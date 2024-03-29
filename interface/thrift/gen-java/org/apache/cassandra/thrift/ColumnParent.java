/**
 * Autogenerated by Thrift Compiler (0.14.0)
 *
 * DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING
 *  @generated
 */
package org.apache.cassandra.thrift;
/*
 * 
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 * 
 */


@SuppressWarnings({"cast", "rawtypes", "serial", "unchecked", "unused"})
/**
 * ColumnParent is used when selecting groups of columns from the same ColumnFamily. In directory structure terms, imagine
 * ColumnParent as ColumnPath + '/../'.
 * 
 * See also <a href="cassandra.html#Struct_ColumnPath">ColumnPath</a>
 */
@javax.annotation.Generated(value = "Autogenerated by Thrift Compiler (0.14.0)", date = "2023-11-08")
public class ColumnParent implements org.apache.thrift.TBase<ColumnParent, ColumnParent._Fields>, java.io.Serializable, Cloneable, Comparable<ColumnParent> {
  private static final org.apache.thrift.protocol.TStruct STRUCT_DESC = new org.apache.thrift.protocol.TStruct("ColumnParent");

  private static final org.apache.thrift.protocol.TField COLUMN_FAMILY_FIELD_DESC = new org.apache.thrift.protocol.TField("column_family", org.apache.thrift.protocol.TType.STRING, (short)3);
  private static final org.apache.thrift.protocol.TField SUPER_COLUMN_FIELD_DESC = new org.apache.thrift.protocol.TField("super_column", org.apache.thrift.protocol.TType.STRING, (short)4);

  private static final org.apache.thrift.scheme.SchemeFactory STANDARD_SCHEME_FACTORY = new ColumnParentStandardSchemeFactory();
  private static final org.apache.thrift.scheme.SchemeFactory TUPLE_SCHEME_FACTORY = new ColumnParentTupleSchemeFactory();

  public @org.apache.thrift.annotation.Nullable java.lang.String column_family; // required
  public @org.apache.thrift.annotation.Nullable java.nio.ByteBuffer super_column; // optional

  /** The set of fields this struct contains, along with convenience methods for finding and manipulating them. */
  public enum _Fields implements org.apache.thrift.TFieldIdEnum {
    COLUMN_FAMILY((short)3, "column_family"),
    SUPER_COLUMN((short)4, "super_column");

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
        case 3: // COLUMN_FAMILY
          return COLUMN_FAMILY;
        case 4: // SUPER_COLUMN
          return SUPER_COLUMN;
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

    public short getThriftFieldId() {
      return _thriftId;
    }

    public java.lang.String getFieldName() {
      return _fieldName;
    }
  }

  // isset id assignments
  private static final _Fields optionals[] = {_Fields.SUPER_COLUMN};
  public static final java.util.Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> metaDataMap;
  static {
    java.util.Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> tmpMap = new java.util.EnumMap<_Fields, org.apache.thrift.meta_data.FieldMetaData>(_Fields.class);
    tmpMap.put(_Fields.COLUMN_FAMILY, new org.apache.thrift.meta_data.FieldMetaData("column_family", org.apache.thrift.TFieldRequirementType.REQUIRED, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.STRING)));
    tmpMap.put(_Fields.SUPER_COLUMN, new org.apache.thrift.meta_data.FieldMetaData("super_column", org.apache.thrift.TFieldRequirementType.OPTIONAL, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.STRING        , true)));
    metaDataMap = java.util.Collections.unmodifiableMap(tmpMap);
    org.apache.thrift.meta_data.FieldMetaData.addStructMetaDataMap(ColumnParent.class, metaDataMap);
  }

  public ColumnParent() {
  }

  public ColumnParent(
    java.lang.String column_family)
  {
    this();
    this.column_family = column_family;
  }

  /**
   * Performs a deep copy on <i>other</i>.
   */
  public ColumnParent(ColumnParent other) {
    if (other.isSetColumn_family()) {
      this.column_family = other.column_family;
    }
    if (other.isSetSuper_column()) {
      this.super_column = org.apache.thrift.TBaseHelper.copyBinary(other.super_column);
    }
  }

  public ColumnParent deepCopy() {
    return new ColumnParent(this);
  }

  @Override
  public void clear() {
    this.column_family = null;
    this.super_column = null;
  }

  @org.apache.thrift.annotation.Nullable
  public java.lang.String getColumn_family() {
    return this.column_family;
  }

  public ColumnParent setColumn_family(@org.apache.thrift.annotation.Nullable java.lang.String column_family) {
    this.column_family = column_family;
    return this;
  }

  public void unsetColumn_family() {
    this.column_family = null;
  }

  /** Returns true if field column_family is set (has been assigned a value) and false otherwise */
  public boolean isSetColumn_family() {
    return this.column_family != null;
  }

  public void setColumn_familyIsSet(boolean value) {
    if (!value) {
      this.column_family = null;
    }
  }

  public byte[] getSuper_column() {
    setSuper_column(org.apache.thrift.TBaseHelper.rightSize(super_column));
    return super_column == null ? null : super_column.array();
  }

  public java.nio.ByteBuffer bufferForSuper_column() {
    return org.apache.thrift.TBaseHelper.copyBinary(super_column);
  }

  public ColumnParent setSuper_column(byte[] super_column) {
    this.super_column = super_column == null ? (java.nio.ByteBuffer)null   : java.nio.ByteBuffer.wrap(super_column.clone());
    return this;
  }

  public ColumnParent setSuper_column(@org.apache.thrift.annotation.Nullable java.nio.ByteBuffer super_column) {
    this.super_column = org.apache.thrift.TBaseHelper.copyBinary(super_column);
    return this;
  }

  public void unsetSuper_column() {
    this.super_column = null;
  }

  /** Returns true if field super_column is set (has been assigned a value) and false otherwise */
  public boolean isSetSuper_column() {
    return this.super_column != null;
  }

  public void setSuper_columnIsSet(boolean value) {
    if (!value) {
      this.super_column = null;
    }
  }

  public void setFieldValue(_Fields field, @org.apache.thrift.annotation.Nullable java.lang.Object value) {
    switch (field) {
    case COLUMN_FAMILY:
      if (value == null) {
        unsetColumn_family();
      } else {
        setColumn_family((java.lang.String)value);
      }
      break;

    case SUPER_COLUMN:
      if (value == null) {
        unsetSuper_column();
      } else {
        if (value instanceof byte[]) {
          setSuper_column((byte[])value);
        } else {
          setSuper_column((java.nio.ByteBuffer)value);
        }
      }
      break;

    }
  }

  @org.apache.thrift.annotation.Nullable
  public java.lang.Object getFieldValue(_Fields field) {
    switch (field) {
    case COLUMN_FAMILY:
      return getColumn_family();

    case SUPER_COLUMN:
      return getSuper_column();

    }
    throw new java.lang.IllegalStateException();
  }

  /** Returns true if field corresponding to fieldID is set (has been assigned a value) and false otherwise */
  public boolean isSet(_Fields field) {
    if (field == null) {
      throw new java.lang.IllegalArgumentException();
    }

    switch (field) {
    case COLUMN_FAMILY:
      return isSetColumn_family();
    case SUPER_COLUMN:
      return isSetSuper_column();
    }
    throw new java.lang.IllegalStateException();
  }

  @Override
  public boolean equals(java.lang.Object that) {
    if (that instanceof ColumnParent)
      return this.equals((ColumnParent)that);
    return false;
  }

  public boolean equals(ColumnParent that) {
    if (that == null)
      return false;
    if (this == that)
      return true;

    boolean this_present_column_family = true && this.isSetColumn_family();
    boolean that_present_column_family = true && that.isSetColumn_family();
    if (this_present_column_family || that_present_column_family) {
      if (!(this_present_column_family && that_present_column_family))
        return false;
      if (!this.column_family.equals(that.column_family))
        return false;
    }

    boolean this_present_super_column = true && this.isSetSuper_column();
    boolean that_present_super_column = true && that.isSetSuper_column();
    if (this_present_super_column || that_present_super_column) {
      if (!(this_present_super_column && that_present_super_column))
        return false;
      if (!this.super_column.equals(that.super_column))
        return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    int hashCode = 1;

    hashCode = hashCode * 8191 + ((isSetColumn_family()) ? 131071 : 524287);
    if (isSetColumn_family())
      hashCode = hashCode * 8191 + column_family.hashCode();

    hashCode = hashCode * 8191 + ((isSetSuper_column()) ? 131071 : 524287);
    if (isSetSuper_column())
      hashCode = hashCode * 8191 + super_column.hashCode();

    return hashCode;
  }

  @Override
  public int compareTo(ColumnParent other) {
    if (!getClass().equals(other.getClass())) {
      return getClass().getName().compareTo(other.getClass().getName());
    }

    int lastComparison = 0;

    lastComparison = java.lang.Boolean.compare(isSetColumn_family(), other.isSetColumn_family());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetColumn_family()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.column_family, other.column_family);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = java.lang.Boolean.compare(isSetSuper_column(), other.isSetSuper_column());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetSuper_column()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.super_column, other.super_column);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    return 0;
  }

  @org.apache.thrift.annotation.Nullable
  public _Fields fieldForId(int fieldId) {
    return _Fields.findByThriftId(fieldId);
  }

  public void read(org.apache.thrift.protocol.TProtocol iprot) throws org.apache.thrift.TException {
    scheme(iprot).read(iprot, this);
  }

  public void write(org.apache.thrift.protocol.TProtocol oprot) throws org.apache.thrift.TException {
    scheme(oprot).write(oprot, this);
  }

  @Override
  public java.lang.String toString() {
    java.lang.StringBuilder sb = new java.lang.StringBuilder("ColumnParent(");
    boolean first = true;

    sb.append("column_family:");
    if (this.column_family == null) {
      sb.append("null");
    } else {
      sb.append(this.column_family);
    }
    first = false;
    if (isSetSuper_column()) {
      if (!first) sb.append(", ");
      sb.append("super_column:");
      if (this.super_column == null) {
        sb.append("null");
      } else {
        org.apache.thrift.TBaseHelper.toString(this.super_column, sb);
      }
      first = false;
    }
    sb.append(")");
    return sb.toString();
  }

  public void validate() throws org.apache.thrift.TException {
    // check for required fields
    if (column_family == null) {
      throw new org.apache.thrift.protocol.TProtocolException("Required field 'column_family' was not present! Struct: " + toString());
    }
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

  private static class ColumnParentStandardSchemeFactory implements org.apache.thrift.scheme.SchemeFactory {
    public ColumnParentStandardScheme getScheme() {
      return new ColumnParentStandardScheme();
    }
  }

  private static class ColumnParentStandardScheme extends org.apache.thrift.scheme.StandardScheme<ColumnParent> {

    public void read(org.apache.thrift.protocol.TProtocol iprot, ColumnParent struct) throws org.apache.thrift.TException {
      org.apache.thrift.protocol.TField schemeField;
      iprot.readStructBegin();
      while (true)
      {
        schemeField = iprot.readFieldBegin();
        if (schemeField.type == org.apache.thrift.protocol.TType.STOP) { 
          break;
        }
        switch (schemeField.id) {
          case 3: // COLUMN_FAMILY
            if (schemeField.type == org.apache.thrift.protocol.TType.STRING) {
              struct.column_family = iprot.readString();
              struct.setColumn_familyIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 4: // SUPER_COLUMN
            if (schemeField.type == org.apache.thrift.protocol.TType.STRING) {
              struct.super_column = iprot.readBinary();
              struct.setSuper_columnIsSet(true);
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

    public void write(org.apache.thrift.protocol.TProtocol oprot, ColumnParent struct) throws org.apache.thrift.TException {
      struct.validate();

      oprot.writeStructBegin(STRUCT_DESC);
      if (struct.column_family != null) {
        oprot.writeFieldBegin(COLUMN_FAMILY_FIELD_DESC);
        oprot.writeString(struct.column_family);
        oprot.writeFieldEnd();
      }
      if (struct.super_column != null) {
        if (struct.isSetSuper_column()) {
          oprot.writeFieldBegin(SUPER_COLUMN_FIELD_DESC);
          oprot.writeBinary(struct.super_column);
          oprot.writeFieldEnd();
        }
      }
      oprot.writeFieldStop();
      oprot.writeStructEnd();
    }

  }

  private static class ColumnParentTupleSchemeFactory implements org.apache.thrift.scheme.SchemeFactory {
    public ColumnParentTupleScheme getScheme() {
      return new ColumnParentTupleScheme();
    }
  }

  private static class ColumnParentTupleScheme extends org.apache.thrift.scheme.TupleScheme<ColumnParent> {

    @Override
    public void write(org.apache.thrift.protocol.TProtocol prot, ColumnParent struct) throws org.apache.thrift.TException {
      org.apache.thrift.protocol.TTupleProtocol oprot = (org.apache.thrift.protocol.TTupleProtocol) prot;
      oprot.writeString(struct.column_family);
      java.util.BitSet optionals = new java.util.BitSet();
      if (struct.isSetSuper_column()) {
        optionals.set(0);
      }
      oprot.writeBitSet(optionals, 1);
      if (struct.isSetSuper_column()) {
        oprot.writeBinary(struct.super_column);
      }
    }

    @Override
    public void read(org.apache.thrift.protocol.TProtocol prot, ColumnParent struct) throws org.apache.thrift.TException {
      org.apache.thrift.protocol.TTupleProtocol iprot = (org.apache.thrift.protocol.TTupleProtocol) prot;
      struct.column_family = iprot.readString();
      struct.setColumn_familyIsSet(true);
      java.util.BitSet incoming = iprot.readBitSet(1);
      if (incoming.get(0)) {
        struct.super_column = iprot.readBinary();
        struct.setSuper_columnIsSet(true);
      }
    }
  }

  private static <S extends org.apache.thrift.scheme.IScheme> S scheme(org.apache.thrift.protocol.TProtocol proto) {
    return (org.apache.thrift.scheme.StandardScheme.class.equals(proto.getScheme()) ? STANDARD_SCHEME_FACTORY : TUPLE_SCHEME_FACTORY).getScheme();
  }
}

