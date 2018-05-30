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

import java.nio.ByteBuffer;
import zipkin2.Endpoint;
import zipkin2.internal.Buffer;

import static zipkin2.internal.Buffer.utf8SizeInBytes;
import static zipkin2.v1.internal.ThriftCodec.readByteArray;
import static zipkin2.v1.internal.ThriftCodec.readUtf8;
import static zipkin2.v1.internal.ThriftCodec.skip;
import static zipkin2.v1.internal.ThriftCodec.writeInt;
import static zipkin2.v1.internal.ThriftCodec.writeLengthPrefixed;
import static zipkin2.v1.internal.ThriftField.TYPE_I16;
import static zipkin2.v1.internal.ThriftField.TYPE_I32;
import static zipkin2.v1.internal.ThriftField.TYPE_STOP;
import static zipkin2.v1.internal.ThriftField.TYPE_STRING;

final class ThriftEndpointCodec {

  static final ThriftField IPV4 = new ThriftField(TYPE_I32, 1);
  static final ThriftField PORT = new ThriftField(TYPE_I16, 2);
  static final ThriftField SERVICE_NAME = new ThriftField(TYPE_STRING, 3);
  static final ThriftField IPV6 = new ThriftField(TYPE_STRING, 4);

  static Endpoint read(ByteBuffer bytes) {
    Endpoint.Builder result = Endpoint.newBuilder();

    while (true) {
      ThriftField thriftField = ThriftField.read(bytes);
      if (thriftField.type == TYPE_STOP) break;

      if (thriftField.isEqualTo(IPV4)) {
        int ipv4 = bytes.getInt();
        result.parseIp(
            new byte[] {
              (byte) (ipv4 >> 24 & 0xff),
              (byte) (ipv4 >> 16 & 0xff),
              (byte) (ipv4 >> 8 & 0xff),
              (byte) (ipv4 & 0xff)
            });
      } else if (thriftField.isEqualTo(PORT)) {
        result.port(bytes.getShort());
      } else if (thriftField.isEqualTo(SERVICE_NAME)) {
        result.serviceName(readUtf8(bytes));
      } else if (thriftField.isEqualTo(IPV6)) {
        result.parseIp(readByteArray(bytes));
      } else {
        skip(bytes, thriftField.type);
      }
    }
    return result.build();
  }

  static int sizeInBytes(Endpoint value) {
    String serviceName = value.serviceName();
    int sizeInBytes = 0;
    sizeInBytes += 3 + 4; // IPV4
    sizeInBytes += 3 + 2; // PORT
    sizeInBytes += 3 + 4 + (serviceName != null ? utf8SizeInBytes(serviceName) : 0);
    if (value.ipv6() != null) sizeInBytes += 3 + 4 + 16;
    sizeInBytes++; // TYPE_STOP
    return sizeInBytes;
  }

  static void write(Endpoint value, Buffer buffer) {
    IPV4.write(buffer);
    buffer.write(value.ipv4Bytes());

    PORT.write(buffer);
    int port = value.portAsInt();
    // write short!
    buffer.writeByte((port >>> 8L) & 0xff);
    buffer.writeByte(port & 0xff);

    SERVICE_NAME.write(buffer);
    writeLengthPrefixed(buffer, value.serviceName() != null ? value.serviceName() : "");

    byte[] ipv6 = value.ipv6Bytes();
    if (ipv6 != null) {
      IPV6.write(buffer);
      writeInt(buffer, 16);
      buffer.write(ipv6);
    }

    buffer.writeByte(TYPE_STOP);
  }
}
