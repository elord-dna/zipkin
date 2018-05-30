/**
 * Copyright 2015-2018 The OpenZipkin Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package zipkin2.v1.internal;

import java.util.List;
import java.util.Map;
import zipkin2.Annotation;
import zipkin2.Endpoint;
import zipkin2.Span;
import zipkin2.internal.Buffer;
import zipkin2.internal.Nullable;
import zipkin2.internal.V1Metadata;

import static zipkin2.internal.Buffer.utf8SizeInBytes;
import static zipkin2.v1.internal.HexCodec.lowerHexToUnsignedLong;
import static zipkin2.v1.internal.ThriftCodec.listSizeInBytes;
import static zipkin2.v1.internal.ThriftCodec.writeInt;
import static zipkin2.v1.internal.ThriftCodec.writeLengthPrefixed;
import static zipkin2.v1.internal.ThriftCodec.writeListBegin;
import static zipkin2.v1.internal.ThriftCodec.writeLong;
import static zipkin2.v1.internal.ThriftField.TYPE_BOOL;
import static zipkin2.v1.internal.ThriftField.TYPE_I32;
import static zipkin2.v1.internal.ThriftField.TYPE_I64;
import static zipkin2.v1.internal.ThriftField.TYPE_LIST;
import static zipkin2.v1.internal.ThriftField.TYPE_STOP;
import static zipkin2.v1.internal.ThriftField.TYPE_STRING;
import static zipkin2.v1.internal.ThriftField.TYPE_STRUCT;

// @Immutable
public final class V1ThriftSpanWriter implements Buffer.Writer<Span> {

  static final ThriftField TRACE_ID = new ThriftField(TYPE_I64, 1);
  static final ThriftField TRACE_ID_HIGH = new ThriftField(TYPE_I64, 12);
  static final ThriftField NAME = new ThriftField(TYPE_STRING, 3);
  static final ThriftField ID = new ThriftField(TYPE_I64, 4);
  static final ThriftField PARENT_ID = new ThriftField(TYPE_I64, 5);
  static final ThriftField ANNOTATIONS = new ThriftField(TYPE_LIST, 6);
  static final ThriftField BINARY_ANNOTATIONS = new ThriftField(TYPE_LIST, 8);
  static final ThriftField DEBUG = new ThriftField(TYPE_BOOL, 9);
  static final ThriftField TIMESTAMP = new ThriftField(TYPE_I64, 10);
  static final ThriftField DURATION = new ThriftField(TYPE_I64, 11);

  static final byte[] EMPTY_ARRAY = new byte[0];

  @Override
  public int sizeInBytes(Span value) {
    V1Metadata md = V1Metadata.parse(value);

    int endpointSize =
        value.localEndpoint() != null ? ThriftEndpointCodec.sizeInBytes(value.localEndpoint()) : 0;

    int sizeInBytes = 3 + 8; // TRACE_ID
    if (value.traceId().length() == 32) sizeInBytes += 3 + 8; // TRACE_ID_HIGH
    if (value.parentId() != null) sizeInBytes += 3 + 8; // PARENT_ID
    sizeInBytes += 3 + 8; // ID
    sizeInBytes += 3 + 4; // NAME
    if (value.name() != null) sizeInBytes += utf8SizeInBytes(value.name());

    // we write list thriftFields even when empty to match finagle serialization
    sizeInBytes += 3 + 5; // ANNOTATION field + list overhead
    int annotationsSizeInBytes = annotationsSizeInBytes(value, md, endpointSize);
    sizeInBytes += annotationsSizeInBytes;
    sizeInBytes += 3 + 5; // BINARY_ANNOTATION field + list overhead
    sizeInBytes +=
        binaryAnnotationsSizeInBytes(value, md, annotationsSizeInBytes != 0, endpointSize);

    if (Boolean.TRUE.equals(value.debug())) sizeInBytes += 3 + 1; // DEBUG

    // Don't report timestamp and duration on shared spans (should be server, but not necessarily)
    if (!Boolean.TRUE.equals(value.shared())) {
      if (value.timestampAsLong() != 0L) sizeInBytes += 3 + 8; // TIMESTAMP
      if (value.durationAsLong() != 0L) sizeInBytes += 3 + 8; // DURATION
    }
    sizeInBytes++; // TYPE_STOP
    return sizeInBytes;
  }

  static int annotationsSizeInBytes(Span value, V1Metadata md, int endpointSize) {
    int sizeInBytes = 0;
    if (md.startTs != 0L && md.begin != null) {
      sizeInBytes += ThriftAnnotationWriter.sizeInBytes(2, endpointSize);
    }

    if (md.endTs != 0L && md.end != null) {
      sizeInBytes += ThriftAnnotationWriter.sizeInBytes(2, endpointSize);
    }

    for (int i = 0, length = value.annotations().size(); i < length; i++) {
      int valueSize = utf8SizeInBytes(value.annotations().get(i).value());
      sizeInBytes += ThriftAnnotationWriter.sizeInBytes(valueSize, endpointSize);
    }
    return sizeInBytes;
  }

  static int binaryAnnotationsSizeInBytes(
      Span value, V1Metadata md, boolean wroteAnnotations, int endpointSize) {
    int sizeInBytes = 0;
    for (Map.Entry<String, String> tag : value.tags().entrySet()) {
      int keySize = utf8SizeInBytes(tag.getKey());
      int valueSize = utf8SizeInBytes(tag.getValue());
      sizeInBytes += ThriftBinaryAnnotationWriter.sizeInBytes(keySize, valueSize, endpointSize);
    }
    if (!wroteAnnotations
        && sizeInBytes == 0
        && endpointSize != 0) { // size of empty "lc" binary annotation
      sizeInBytes += ThriftBinaryAnnotationWriter.sizeInBytes(2, 0, endpointSize);
    }
    if (md.remoteEndpointType != null && value.remoteEndpoint() != null) {
      int remoteEndpointSize = ThriftEndpointCodec.sizeInBytes(value.remoteEndpoint());
      sizeInBytes += ThriftBinaryAnnotationWriter.sizeInBytes(2, 1, remoteEndpointSize);
    }
    return sizeInBytes;
  }

  @Override
  public void write(Span value, Buffer buffer) {
    V1Metadata md = V1Metadata.parse(value);
    byte[] endpointBytes = legacyEndpointBytes(value.localEndpoint());

    TRACE_ID.write(buffer);
    writeLong(buffer, lowerHexToUnsignedLong(value.traceId()));

    NAME.write(buffer);
    writeLengthPrefixed(buffer, value.name() != null ? value.name() : "");

    ID.write(buffer);
    writeLong(buffer, lowerHexToUnsignedLong(value.id()));

    if (value.parentId() != null) {
      PARENT_ID.write(buffer);
      writeLong(buffer, lowerHexToUnsignedLong(value.parentId()));
    }

    // we write list thriftFields even when empty to match finagle serialization
    ANNOTATIONS.write(buffer);
    boolean wroteAnnotations = writeAnnotations(value, md, endpointBytes, buffer);
    BINARY_ANNOTATIONS.write(buffer);
    writeBinaryAnnotations(value, md, wroteAnnotations, endpointBytes, buffer);

    if (Boolean.TRUE.equals(value.debug())) {
      DEBUG.write(buffer);
      buffer.writeByte(1);
    }

    if (!Boolean.TRUE.equals(value.shared())) {
      if (value.timestampAsLong() != 0L) {
        TIMESTAMP.write(buffer);
        writeLong(buffer, value.timestampAsLong());
      }
      if (value.durationAsLong() != 0L) {
        DURATION.write(buffer);
        writeLong(buffer, value.durationAsLong());
      }
    }

    if (value.traceId().length() == 32) {
      TRACE_ID_HIGH.write(buffer);
      writeLong(buffer, lowerHexToUnsignedLong(value.traceId(), 0));
    }

    buffer.writeByte(TYPE_STOP);
  }

  void writeBinaryAnnotations(
      Span value, V1Metadata md, boolean wroteAnnotations, byte[] endpointBytes, Buffer buffer) {
    int binaryAnnotationCount = value.tags().size();
    boolean writeLocalComponent =
        !wroteAnnotations && endpointBytes != null && binaryAnnotationCount == 0;
    if (writeLocalComponent) binaryAnnotationCount++;

    boolean hasRemoteEndpoint = md.remoteEndpointType != null && value.remoteEndpoint() != null;
    if (hasRemoteEndpoint) binaryAnnotationCount++;

    writeListBegin(buffer, binaryAnnotationCount);

    for (Map.Entry<String, String> entry : value.tags().entrySet()) {
      ThriftBinaryAnnotationWriter.write(
          entry.getKey(), entry.getValue(), false, endpointBytes, buffer);
    }
    // write an empty "lc" annotation to avoid missing the localEndpoint in an in-process span
    if (writeLocalComponent) {
      ThriftBinaryAnnotationWriter.write("lc", "", false, endpointBytes, buffer);
    }
    if (hasRemoteEndpoint) {
      byte[] remoteEndpointBytes = legacyEndpointBytes(value.remoteEndpoint());
      ThriftBinaryAnnotationWriter.write(
          md.remoteEndpointType, null, true, remoteEndpointBytes, buffer);
    }
  }

  boolean writeAnnotations(Span value, V1Metadata md, byte[] endpointBytes, Buffer buffer) {
    int annotationCount = value.annotations().size();
    boolean beginAnnotation = md.startTs != 0L && md.begin != null;
    if (beginAnnotation) annotationCount++;
    boolean endAnnotation = md.endTs != 0L && md.end != null;
    if (endAnnotation) annotationCount++;
    writeListBegin(buffer, annotationCount);
    if (beginAnnotation) {
      ThriftAnnotationWriter.write(md.startTs, md.begin, endpointBytes, buffer);
    }
    for (int i = 0, length = value.annotations().size(); i < length; i++) {
      Annotation a = value.annotations().get(i);
      ThriftAnnotationWriter.write(a.timestamp(), a.value(), endpointBytes, buffer);
    }
    if (endAnnotation) {
      ThriftAnnotationWriter.write(md.endTs, md.end, endpointBytes, buffer);
    }
    return annotationCount > 0;
  }

  @Override
  public String toString() {
    return "Span";
  }

  public byte[] writeList(List<Span> spans) {
    int lengthOfSpans = spans.size();
    if (lengthOfSpans == 0) return EMPTY_ARRAY;

    Buffer result = new Buffer(listSizeInBytes(this, spans));
    ThriftCodec.writeList(this, spans, result);
    return result.toByteArray();
  }

  public byte[] write(Span onlySpan) {
    Buffer result = new Buffer(sizeInBytes(onlySpan));
    write(onlySpan, result);
    return result.toByteArray();
  }

  public int writeList(List<Span> spans, byte[] out, int pos) {
    int lengthOfSpans = spans.size();
    if (lengthOfSpans == 0) return 0;

    Buffer result = new Buffer(out, pos);
    ThriftCodec.writeList(this, spans, result);

    return result.pos() - pos;
  }

  static byte[] legacyEndpointBytes(@Nullable Endpoint localEndpoint) {
    if (localEndpoint == null) return null;
    Buffer buffer = new Buffer(ThriftEndpointCodec.sizeInBytes(localEndpoint));
    ThriftEndpointCodec.write(localEndpoint, buffer);
    return buffer.toByteArray();
  }

  static class ThriftAnnotationWriter {

    static final ThriftField TIMESTAMP = new ThriftField(TYPE_I64, 1);
    static final ThriftField VALUE = new ThriftField(TYPE_STRING, 2);
    static final ThriftField ENDPOINT = new ThriftField(TYPE_STRUCT, 3);

    static int sizeInBytes(int valueSizeInBytes, int endpointSizeInBytes) {
      int sizeInBytes = 0;
      sizeInBytes += 3 + 8; // TIMESTAMP
      sizeInBytes += 3 + 4 + valueSizeInBytes;
      if (endpointSizeInBytes > 0) sizeInBytes += 3 + endpointSizeInBytes;
      sizeInBytes++; // TYPE_STOP
      return sizeInBytes;
    }

    static void write(long timestamp, String value, byte[] endpointBytes, Buffer buffer) {
      TIMESTAMP.write(buffer);
      writeLong(buffer, timestamp);

      VALUE.write(buffer);
      writeLengthPrefixed(buffer, value);

      if (endpointBytes != null) {
        ENDPOINT.write(buffer);
        buffer.write(endpointBytes);
      }
      buffer.writeByte(TYPE_STOP);
    }
  }

  static class ThriftBinaryAnnotationWriter {

    static final ThriftField KEY = new ThriftField(TYPE_STRING, 1);
    static final ThriftField VALUE = new ThriftField(TYPE_STRING, 2);
    static final ThriftField TYPE = new ThriftField(TYPE_I32, 3);
    static final ThriftField ENDPOINT = new ThriftField(TYPE_STRUCT, 4);

    static int sizeInBytes(int keySize, int valueSize, int endpointSizeInBytes) {
      int sizeInBytes = 0;
      sizeInBytes += 3 + 4 + keySize;
      sizeInBytes += 3 + 4 + valueSize;
      sizeInBytes += 3 + 4; // TYPE
      if (endpointSizeInBytes > 0) sizeInBytes += 3 + endpointSizeInBytes;
      sizeInBytes++; // TYPE_STOP
      return sizeInBytes;
    }

    static void write(
        String key, String stringValue, boolean booleanValue, byte[] endpointBytes, Buffer buffer) {
      KEY.write(buffer);
      writeLengthPrefixed(buffer, key);

      VALUE.write(buffer);
      int type = 0;
      if (stringValue != null) {
        type = 6;
        writeInt(buffer, utf8SizeInBytes(stringValue));
        buffer.writeUtf8(stringValue);
      } else {
        writeInt(buffer, 1);
        buffer.writeByte(booleanValue ? 1 : 0);
      }

      TYPE.write(buffer);
      writeInt(buffer, type);

      if (endpointBytes != null) {
        ENDPOINT.write(buffer);
        buffer.write(endpointBytes);
      }

      buffer.writeByte(TYPE_STOP);
    }
  }
}
