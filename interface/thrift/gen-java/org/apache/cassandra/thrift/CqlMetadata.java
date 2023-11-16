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
@javax.annotation.Generated(value = "Autogenerated by Thrift Compiler (0.14.0)", date = "2023-11-08")
public class CqlMetadata implements org.apache.thrift.TBase<CqlMetadata, CqlMetadata._Fields>, java.io.Serializable, Cloneable, Comparable<CqlMetadata> {
  private static final org.apache.thrift.protocol.TStruct STRUCT_DESC = new org.apache.thrift.protocol.TStruct("CqlMetadata");

  private static final org.apache.thrift.protocol.TField NAME_TYPES_FIELD_DESC = new org.apache.thrift.protocol.TField("name_types", org.apache.thrift.protocol.TType.MAP, (short)1);
  private static final org.apache.thrift.protocol.TField VALUE_TYPES_FIELD_DESC = new org.apache.thrift.protocol.TField("value_types", org.apache.thrift.protocol.TType.MAP, (short)2);
  private static final org.apache.thrift.protocol.TField DEFAULT_NAME_TYPE_FIELD_DESC = new org.apache.thrift.protocol.TField("default_name_type", org.apache.thrift.protocol.TType.STRING, (short)3);
  private static final org.apache.thrift.protocol.TField DEFAULT_VALUE_TYPE_FIELD_DESC = new org.apache.thrift.protocol.TField("default_value_type", org.apache.thrift.protocol.TType.STRING, (short)4);

  private static final org.apache.thrift.scheme.SchemeFactory STANDARD_SCHEME_FACTORY = new CqlMetadataStandardSchemeFactory();
  private static final org.apache.thrift.scheme.SchemeFactory TUPLE_SCHEME_FACTORY = new CqlMetadataTupleSchemeFactory();

  public @org.apache.thrift.annotation.Nullable java.util.Map<java.nio.ByteBuffer,java.lang.String> name_types; // required
  public @org.apache.thrift.annotation.Nullable java.util.Map<java.nio.ByteBuffer,java.lang.String> value_types; // required
  public @org.apache.thrift.annotation.Nullable java.lang.String default_name_type; // required
  public @org.apache.thrift.annotation.Nullable java.lang.String default_value_type; // required

  /** The set of fields this struct contains, along with convenience methods for finding and manipulating them. */
  public enum _Fields implements org.apache.thrift.TFieldIdEnum {
    NAME_TYPES((short)1, "name_types"),
    VALUE_TYPES((short)2, "value_types"),
    DEFAULT_NAME_TYPE((short)3, "default_name_type"),
    DEFAULT_VALUE_TYPE((short)4, "default_value_type");

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
        case 1: // NAME_TYPES
          return NAME_TYPES;
        case 2: // VALUE_TYPES
          return VALUE_TYPES;
        case 3: // DEFAULT_NAME_TYPE
          return DEFAULT_NAME_TYPE;
        case 4: // DEFAULT_VALUE_TYPE
          return DEFAULT_VALUE_TYPE;
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
  public static final java.util.Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> metaDataMap;
  static {
    java.util.Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> tmpMap = new java.util.EnumMap<_Fields, org.apache.thrift.meta_data.FieldMetaData>(_Fields.class);
    tmpMap.put(_Fields.NAME_TYPES, new org.apache.thrift.meta_data.FieldMetaData("name_types", org.apache.thrift.TFieldRequirementType.REQUIRED, 
        new org.apache.thrift.meta_data.MapMetaData(org.apache.thrift.protocol.TType.MAP, 
            new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.STRING            , true), 
            new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.STRING))));
    tmpMap.put(_Fields.VALUE_TYPES, new org.apache.thrift.meta_data.FieldMetaData("value_types", org.apache.thrift.TFieldRequirementType.REQUIRED, 
        new org.apache.thrift.meta_data.MapMetaData(org.apache.thrift.protocol.TType.MAP, 
            new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.STRING            , true), 
            new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.STRING))));
    tmpMap.put(_Fields.DEFAULT_NAME_TYPE, new org.apache.thrift.meta_data.FieldMetaData("default_name_type", org.apache.thrift.TFieldRequirementType.REQUIRED, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.STRING)));
    tmpMap.put(_Fields.DEFAULT_VALUE_TYPE, new org.apache.thrift.meta_data.FieldMetaData("default_value_type", org.apache.thrift.TFieldRequirementType.REQUIRED, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.STRING)));
    metaDataMap = java.util.Collections.unmodifiableMap(tmpMap);
    org.apache.thrift.meta_data.FieldMetaData.addStructMetaDataMap(CqlMetadata.class, metaDataMap);
  }

  public CqlMetadata() {
  }

  public CqlMetadata(
    java.util.Map<java.nio.ByteBuffer,java.lang.String> name_types,
    java.util.Map<java.nio.ByteBuffer,java.lang.String> value_types,
    java.lang.String default_name_type,
    java.lang.String default_value_type)
  {
    this();
    this.name_types = name_types;
    this.value_types = value_types;
    this.default_name_type = default_name_type;
    this.default_value_type = default_value_type;
  }

  /**
   * Performs a deep copy on <i>other</i>.
   */
  public CqlMetadata(CqlMetadata other) {
    if (other.isSetName_types()) {
      java.util.Map<java.nio.ByteBuffer,java.lang.String> __this__name_types = new java.util.HashMap<java.nio.ByteBuffer,java.lang.String>(other.name_types);
      this.name_types = __this__name_types;
    }
    if (other.isSetValue_types()) {
      java.util.Map<java.nio.ByteBuffer,java.lang.String> __this__value_types = new java.util.HashMap<java.nio.ByteBuffer,java.lang.String>(other.value_types);
      this.value_types = __this__value_types;
    }
    if (other.isSetDefault_name_type()) {
      this.default_name_type = other.default_name_type;
    }
    if (other.isSetDefault_value_type()) {
      this.default_value_type = other.default_value_type;
    }
  }

  public CqlMetadata deepCopy() {
    return new CqlMetadata(this);
  }

  @Override
  public void clear() {
    this.name_types = null;
    this.value_types = null;
    this.default_name_type = null;
    this.default_value_type = null;
  }

  public int getName_typesSize() {
    return (this.name_types == null) ? 0 : this.name_types.size();
  }

  public void putToName_types(java.nio.ByteBuffer key, java.lang.String val) {
    if (this.name_types == null) {
      this.name_types = new java.util.HashMap<java.nio.ByteBuffer,java.lang.String>();
    }
    this.name_types.put(key, val);
  }

  @org.apache.thrift.annotation.Nullable
  public java.util.Map<java.nio.ByteBuffer,java.lang.String> getName_types() {
    return this.name_types;
  }

  public CqlMetadata setName_types(@org.apache.thrift.annotation.Nullable java.util.Map<java.nio.ByteBuffer,java.lang.String> name_types) {
    this.name_types = name_types;
    return this;
  }

  public void unsetName_types() {
    this.name_types = null;
  }

  /** Returns true if field name_types is set (has been assigned a value) and false otherwise */
  public boolean isSetName_types() {
    return this.name_types != null;
  }

  public void setName_typesIsSet(boolean value) {
    if (!value) {
      this.name_types = null;
    }
  }

  public int getValue_typesSize() {
    return (this.value_types == null) ? 0 : this.value_types.size();
  }

  public void putToValue_types(java.nio.ByteBuffer key, java.lang.String val) {
    if (this.value_types == null) {
      this.value_types = new java.util.HashMap<java.nio.ByteBuffer,java.lang.String>();
    }
    this.value_types.put(key, val);
  }

  @org.apache.thrift.annotation.Nullable
  public java.util.Map<java.nio.ByteBuffer,java.lang.String> getValue_types() {
    return this.value_types;
  }

  public CqlMetadata setValue_types(@org.apache.thrift.annotation.Nullable java.util.Map<java.nio.ByteBuffer,java.lang.String> value_types) {
    this.value_types = value_types;
    return this;
  }

  public void unsetValue_types() {
    this.value_types = null;
  }

  /** Returns true if field value_types is set (has been assigned a value) and false otherwise */
  public boolean isSetValue_types() {
    return this.value_types != null;
  }

  public void setValue_typesIsSet(boolean value) {
    if (!value) {
      this.value_types = null;
    }
  }

  @org.apache.thrift.annotation.Nullable
  public java.lang.String getDefault_name_type() {
    return this.default_name_type;
  }

  public CqlMetadata setDefault_name_type(@org.apache.thrift.annotation.Nullable java.lang.String default_name_type) {
    this.default_name_type = default_name_type;
    return this;
  }

  public void unsetDefault_name_type() {
    this.default_name_type = null;
  }

  /** Returns true if field default_name_type is set (has been assigned a value) and false otherwise */
  public boolean isSetDefault_name_type() {
    return this.default_name_type != null;
  }

  public void setDefault_name_typeIsSet(boolean value) {
    if (!value) {
      this.default_name_type = null;
    }
  }

  @org.apache.thrift.annotation.Nullable
  public java.lang.String getDefault_value_type() {
    return this.default_value_type;
  }

  public CqlMetadata setDefault_value_type(@org.apache.thrift.annotation.Nullable java.lang.String default_value_type) {
    this.default_value_type = default_value_type;
    return this;
  }

  public void unsetDefault_value_type() {
    this.default_value_type = null;
  }

  /** Returns true if field default_value_type is set (has been assigned a value) and false otherwise */
  public boolean isSetDefault_value_type() {
    return this.default_value_type != null;
  }

  public void setDefault_value_typeIsSet(boolean value) {
    if (!value) {
      this.default_value_type = null;
    }
  }

  public void setFieldValue(_Fields field, @org.apache.thrift.annotation.Nullable java.lang.Object value) {
    switch (field) {
    case NAME_TYPES:
      if (value == null) {
        unsetName_types();
      } else {
        setName_types((java.util.Map<java.nio.ByteBuffer,java.lang.String>)value);
      }
      break;

    case VALUE_TYPES:
      if (value == null) {
        unsetValue_types();
      } else {
        setValue_types((java.util.Map<java.nio.ByteBuffer,java.lang.String>)value);
      }
      break;

    case DEFAULT_NAME_TYPE:
      if (value == null) {
        unsetDefault_name_type();
      } else {
        setDefault_name_type((java.lang.String)value);
      }
      break;

    case DEFAULT_VALUE_TYPE:
      if (value == null) {
        unsetDefault_value_type();
      } else {
        setDefault_value_type((java.lang.String)value);
      }
      break;

    }
  }

  @org.apache.thrift.annotation.Nullable
  public java.lang.Object getFieldValue(_Fields field) {
    switch (field) {
    case NAME_TYPES:
      return getName_types();

    case VALUE_TYPES:
      return getValue_types();

    case DEFAULT_NAME_TYPE:
      return getDefault_name_type();

    case DEFAULT_VALUE_TYPE:
      return getDefault_value_type();

    }
    throw new java.lang.IllegalStateException();
  }

  /** Returns true if field corresponding to fieldID is set (has been assigned a value) and false otherwise */
  public boolean isSet(_Fields field) {
    if (field == null) {
      throw new java.lang.IllegalArgumentException();
    }

    switch (field) {
    case NAME_TYPES:
      return isSetName_types();
    case VALUE_TYPES:
      return isSetValue_types();
    case DEFAULT_NAME_TYPE:
      return isSetDefault_name_type();
    case DEFAULT_VALUE_TYPE:
      return isSetDefault_value_type();
    }
    throw new java.lang.IllegalStateException();
  }

  @Override
  public boolean equals(java.lang.Object that) {
    if (that instanceof CqlMetadata)
      return this.equals((CqlMetadata)that);
    return false;
  }

  public boolean equals(CqlMetadata that) {
    if (that == null)
      return false;
    if (this == that)
      return true;

    boolean this_present_name_types = true && this.isSetName_types();
    boolean that_present_name_types = true && that.isSetName_types();
    if (this_present_name_types || that_present_name_types) {
      if (!(this_present_name_types && that_present_name_types))
        return false;
      if (!this.name_types.equals(that.name_types))
        return false;
    }

    boolean this_present_value_types = true && this.isSetValue_types();
    boolean that_present_value_types = true && that.isSetValue_types();
    if (this_present_value_types || that_present_value_types) {
      if (!(this_present_value_types && that_present_value_types))
        return false;
      if (!this.value_types.equals(that.value_types))
        return false;
    }

    boolean this_present_default_name_type = true && this.isSetDefault_name_type();
    boolean that_present_default_name_type = true && that.isSetDefault_name_type();
    if (this_present_default_name_type || that_present_default_name_type) {
      if (!(this_present_default_name_type && that_present_default_name_type))
        return false;
      if (!this.default_name_type.equals(that.default_name_type))
        return false;
    }

    boolean this_present_default_value_type = true && this.isSetDefault_value_type();
    boolean that_present_default_value_type = true && that.isSetDefault_value_type();
    if (this_present_default_value_type || that_present_default_value_type) {
      if (!(this_present_default_value_type && that_present_default_value_type))
        return false;
      if (!this.default_value_type.equals(that.default_value_type))
        return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    int hashCode = 1;

    hashCode = hashCode * 8191 + ((isSetName_types()) ? 131071 : 524287);
    if (isSetName_types())
      hashCode = hashCode * 8191 + name_types.hashCode();

    hashCode = hashCode * 8191 + ((isSetValue_types()) ? 131071 : 524287);
    if (isSetValue_types())
      hashCode = hashCode * 8191 + value_types.hashCode();

    hashCode = hashCode * 8191 + ((isSetDefault_name_type()) ? 131071 : 524287);
    if (isSetDefault_name_type())
      hashCode = hashCode * 8191 + default_name_type.hashCode();

    hashCode = hashCode * 8191 + ((isSetDefault_value_type()) ? 131071 : 524287);
    if (isSetDefault_value_type())
      hashCode = hashCode * 8191 + default_value_type.hashCode();

    return hashCode;
  }

  @Override
  public int compareTo(CqlMetadata other) {
    if (!getClass().equals(other.getClass())) {
      return getClass().getName().compareTo(other.getClass().getName());
    }

    int lastComparison = 0;

    lastComparison = java.lang.Boolean.compare(isSetName_types(), other.isSetName_types());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetName_types()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.name_types, other.name_types);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = java.lang.Boolean.compare(isSetValue_types(), other.isSetValue_types());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetValue_types()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.value_types, other.value_types);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = java.lang.Boolean.compare(isSetDefault_name_type(), other.isSetDefault_name_type());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetDefault_name_type()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.default_name_type, other.default_name_type);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = java.lang.Boolean.compare(isSetDefault_value_type(), other.isSetDefault_value_type());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetDefault_value_type()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.default_value_type, other.default_value_type);
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
    java.lang.StringBuilder sb = new java.lang.StringBuilder("CqlMetadata(");
    boolean first = true;

    sb.append("name_types:");
    if (this.name_types == null) {
      sb.append("null");
    } else {
      sb.append(this.name_types);
    }
    first = false;
    if (!first) sb.append(", ");
    sb.append("value_types:");
    if (this.value_types == null) {
      sb.append("null");
    } else {
      sb.append(this.value_types);
    }
    first = false;
    if (!first) sb.append(", ");
    sb.append("default_name_type:");
    if (this.default_name_type == null) {
      sb.append("null");
    } else {
      sb.append(this.default_name_type);
    }
    first = false;
    if (!first) sb.append(", ");
    sb.append("default_value_type:");
    if (this.default_value_type == null) {
      sb.append("null");
    } else {
      sb.append(this.default_value_type);
    }
    first = false;
    sb.append(")");
    return sb.toString();
  }

  public void validate() throws org.apache.thrift.TException {
    // check for required fields
    if (name_types == null) {
      throw new org.apache.thrift.protocol.TProtocolException("Required field 'name_types' was not present! Struct: " + toString());
    }
    if (value_types == null) {
      throw new org.apache.thrift.protocol.TProtocolException("Required field 'value_types' was not present! Struct: " + toString());
    }
    if (default_name_type == null) {
      throw new org.apache.thrift.protocol.TProtocolException("Required field 'default_name_type' was not present! Struct: " + toString());
    }
    if (default_value_type == null) {
      throw new org.apache.thrift.protocol.TProtocolException("Required field 'default_value_type' was not present! Struct: " + toString());
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

  private static class CqlMetadataStandardSchemeFactory implements org.apache.thrift.scheme.SchemeFactory {
    public CqlMetadataStandardScheme getScheme() {
      return new CqlMetadataStandardScheme();
    }
  }

  private static class CqlMetadataStandardScheme extends org.apache.thrift.scheme.StandardScheme<CqlMetadata> {

    public void read(org.apache.thrift.protocol.TProtocol iprot, CqlMetadata struct) throws org.apache.thrift.TException {
      org.apache.thrift.protocol.TField schemeField;
      iprot.readStructBegin();
      while (true)
      {
        schemeField = iprot.readFieldBegin();
        if (schemeField.type == org.apache.thrift.protocol.TType.STOP) { 
          break;
        }
        switch (schemeField.id) {
          case 1: // NAME_TYPES
            if (schemeField.type == org.apache.thrift.protocol.TType.MAP) {
              {
                org.apache.thrift.protocol.TMap _map172 = iprot.readMapBegin();
                struct.name_types = new java.util.HashMap<java.nio.ByteBuffer,java.lang.String>(2*_map172.size);
                @org.apache.thrift.annotation.Nullable java.nio.ByteBuffer _key173;
                @org.apache.thrift.annotation.Nullable java.lang.String _val174;
                for (int _i175 = 0; _i175 < _map172.size; ++_i175)
                {
                  _key173 = iprot.readBinary();
                  _val174 = iprot.readString();
                  struct.name_types.put(_key173, _val174);
                }
                iprot.readMapEnd();
              }
              struct.setName_typesIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 2: // VALUE_TYPES
            if (schemeField.type == org.apache.thrift.protocol.TType.MAP) {
              {
                org.apache.thrift.protocol.TMap _map176 = iprot.readMapBegin();
                struct.value_types = new java.util.HashMap<java.nio.ByteBuffer,java.lang.String>(2*_map176.size);
                @org.apache.thrift.annotation.Nullable java.nio.ByteBuffer _key177;
                @org.apache.thrift.annotation.Nullable java.lang.String _val178;
                for (int _i179 = 0; _i179 < _map176.size; ++_i179)
                {
                  _key177 = iprot.readBinary();
                  _val178 = iprot.readString();
                  struct.value_types.put(_key177, _val178);
                }
                iprot.readMapEnd();
              }
              struct.setValue_typesIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 3: // DEFAULT_NAME_TYPE
            if (schemeField.type == org.apache.thrift.protocol.TType.STRING) {
              struct.default_name_type = iprot.readString();
              struct.setDefault_name_typeIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 4: // DEFAULT_VALUE_TYPE
            if (schemeField.type == org.apache.thrift.protocol.TType.STRING) {
              struct.default_value_type = iprot.readString();
              struct.setDefault_value_typeIsSet(true);
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

    public void write(org.apache.thrift.protocol.TProtocol oprot, CqlMetadata struct) throws org.apache.thrift.TException {
      struct.validate();

      oprot.writeStructBegin(STRUCT_DESC);
      if (struct.name_types != null) {
        oprot.writeFieldBegin(NAME_TYPES_FIELD_DESC);
        {
          oprot.writeMapBegin(new org.apache.thrift.protocol.TMap(org.apache.thrift.protocol.TType.STRING, org.apache.thrift.protocol.TType.STRING, struct.name_types.size()));
          for (java.util.Map.Entry<java.nio.ByteBuffer, java.lang.String> _iter180 : struct.name_types.entrySet())
          {
            oprot.writeBinary(_iter180.getKey());
            oprot.writeString(_iter180.getValue());
          }
          oprot.writeMapEnd();
        }
        oprot.writeFieldEnd();
      }
      if (struct.value_types != null) {
        oprot.writeFieldBegin(VALUE_TYPES_FIELD_DESC);
        {
          oprot.writeMapBegin(new org.apache.thrift.protocol.TMap(org.apache.thrift.protocol.TType.STRING, org.apache.thrift.protocol.TType.STRING, struct.value_types.size()));
          for (java.util.Map.Entry<java.nio.ByteBuffer, java.lang.String> _iter181 : struct.value_types.entrySet())
          {
            oprot.writeBinary(_iter181.getKey());
            oprot.writeString(_iter181.getValue());
          }
          oprot.writeMapEnd();
        }
        oprot.writeFieldEnd();
      }
      if (struct.default_name_type != null) {
        oprot.writeFieldBegin(DEFAULT_NAME_TYPE_FIELD_DESC);
        oprot.writeString(struct.default_name_type);
        oprot.writeFieldEnd();
      }
      if (struct.default_value_type != null) {
        oprot.writeFieldBegin(DEFAULT_VALUE_TYPE_FIELD_DESC);
        oprot.writeString(struct.default_value_type);
        oprot.writeFieldEnd();
      }
      oprot.writeFieldStop();
      oprot.writeStructEnd();
    }

  }

  private static class CqlMetadataTupleSchemeFactory implements org.apache.thrift.scheme.SchemeFactory {
    public CqlMetadataTupleScheme getScheme() {
      return new CqlMetadataTupleScheme();
    }
  }

  private static class CqlMetadataTupleScheme extends org.apache.thrift.scheme.TupleScheme<CqlMetadata> {

    @Override
    public void write(org.apache.thrift.protocol.TProtocol prot, CqlMetadata struct) throws org.apache.thrift.TException {
      org.apache.thrift.protocol.TTupleProtocol oprot = (org.apache.thrift.protocol.TTupleProtocol) prot;
      {
        oprot.writeI32(struct.name_types.size());
        for (java.util.Map.Entry<java.nio.ByteBuffer, java.lang.String> _iter182 : struct.name_types.entrySet())
        {
          oprot.writeBinary(_iter182.getKey());
          oprot.writeString(_iter182.getValue());
        }
      }
      {
        oprot.writeI32(struct.value_types.size());
        for (java.util.Map.Entry<java.nio.ByteBuffer, java.lang.String> _iter183 : struct.value_types.entrySet())
        {
          oprot.writeBinary(_iter183.getKey());
          oprot.writeString(_iter183.getValue());
        }
      }
      oprot.writeString(struct.default_name_type);
      oprot.writeString(struct.default_value_type);
    }

    @Override
    public void read(org.apache.thrift.protocol.TProtocol prot, CqlMetadata struct) throws org.apache.thrift.TException {
      org.apache.thrift.protocol.TTupleProtocol iprot = (org.apache.thrift.protocol.TTupleProtocol) prot;
      {
        org.apache.thrift.protocol.TMap _map184 = iprot.readMapBegin(org.apache.thrift.protocol.TType.STRING, org.apache.thrift.protocol.TType.STRING); 
        struct.name_types = new java.util.HashMap<java.nio.ByteBuffer,java.lang.String>(2*_map184.size);
        @org.apache.thrift.annotation.Nullable java.nio.ByteBuffer _key185;
        @org.apache.thrift.annotation.Nullable java.lang.String _val186;
        for (int _i187 = 0; _i187 < _map184.size; ++_i187)
        {
          _key185 = iprot.readBinary();
          _val186 = iprot.readString();
          struct.name_types.put(_key185, _val186);
        }
      }
      struct.setName_typesIsSet(true);
      {
        org.apache.thrift.protocol.TMap _map188 = iprot.readMapBegin(org.apache.thrift.protocol.TType.STRING, org.apache.thrift.protocol.TType.STRING); 
        struct.value_types = new java.util.HashMap<java.nio.ByteBuffer,java.lang.String>(2*_map188.size);
        @org.apache.thrift.annotation.Nullable java.nio.ByteBuffer _key189;
        @org.apache.thrift.annotation.Nullable java.lang.String _val190;
        for (int _i191 = 0; _i191 < _map188.size; ++_i191)
        {
          _key189 = iprot.readBinary();
          _val190 = iprot.readString();
          struct.value_types.put(_key189, _val190);
        }
      }
      struct.setValue_typesIsSet(true);
      struct.default_name_type = iprot.readString();
      struct.setDefault_name_typeIsSet(true);
      struct.default_value_type = iprot.readString();
      struct.setDefault_value_typeIsSet(true);
    }
  }

  private static <S extends org.apache.thrift.scheme.IScheme> S scheme(org.apache.thrift.protocol.TProtocol proto) {
    return (org.apache.thrift.scheme.StandardScheme.class.equals(proto.getScheme()) ? STANDARD_SCHEME_FACTORY : TUPLE_SCHEME_FACTORY).getScheme();
  }
}

