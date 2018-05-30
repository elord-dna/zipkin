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

import java.io.IOException;
import zipkin2.Endpoint;
import zipkin2.internal.JsonCodec.JsonReader;
import zipkin2.internal.JsonCodec.JsonReaderAdapter;
import zipkin2.v1.V1Annotation;
import zipkin2.v1.V1BinaryAnnotation;
import zipkin2.v1.V1Span;

public final class V1JsonSpanReader implements JsonReaderAdapter<V1Span> {

  V1Span.Builder builder;

  @Override
  public V1Span fromJson(JsonReader reader) throws IOException {
    if (builder == null) {
      builder = V1Span.builder();
    } else {
      builder.clear();
    }
    reader.beginObject();
    String name = ""; // tolerate missing name
    while (reader.hasNext()) {
      String nextName = reader.nextName();
      if (nextName.equals("traceId")) {
        builder.traceId(reader.nextString());
        continue;
      } else if (nextName.equals("id")) {
        builder.id(reader.nextString());
        continue;
      } else if (reader.peekNull()) {
        reader.skipValue();
        continue;
      }

      // read any optional fields
      if (nextName.equals("name")) {
        name = reader.nextString();
      } else if (nextName.equals("parentId")) {
        builder.parentId(reader.nextString());
      } else if (nextName.equals("timestamp")) {
        builder.timestamp(reader.nextLong());
      } else if (nextName.equals("duration")) {
        builder.duration(reader.nextLong());
      } else if (nextName.equals("annotations")) {
        reader.beginArray();
        while (reader.hasNext()) {
          reader.beginObject();
          Long timestamp = null;
          String value = null;
          Endpoint endpoint = null;
          while (reader.hasNext()) {
            nextName = reader.nextName();
            if (nextName.equals("timestamp")) {
              timestamp = reader.nextLong();
            } else if (nextName.equals("value")) {
              value = reader.nextString();
            } else if (nextName.equals("endpoint") && !reader.peekNull()) {
              endpoint = ENDPOINT_READER.fromJson(reader);
            } else {
              reader.skipValue();
            }
          }
          if (timestamp == null || value == null) {
            throw new IllegalArgumentException("Incomplete annotation at " + reader.getPath());
          }
          reader.endObject();
          builder.addAnnotation(V1Annotation.create(timestamp, value, endpoint));
        }
        reader.endArray();
      } else if (nextName.equals("binaryAnnotations")) {
        reader.beginArray();
        while (reader.hasNext()) {
          V1BinaryAnnotation b = readBinaryAnnotation(reader);
          if (b != null) builder.addBinaryAnnotation(b);
        }
        reader.endArray();
      } else if (nextName.equals("debug")) {
        if (reader.nextBoolean()) builder.debug(true);
      } else {
        reader.skipValue();
      }
    }
    reader.endObject();
    return builder.name(name).build();
  }

  @Override
  public String toString() {
    return "Span";
  }

  static V1BinaryAnnotation readBinaryAnnotation(JsonReader reader) throws IOException {
    String key = null;
    Endpoint endpoint = null;
    Boolean booleanValue = null;
    String stringValue = null;

    reader.beginObject();
    while (reader.hasNext()) {
      String nextName = reader.nextName();
      if (reader.peekNull()) {
        reader.skipValue();
        continue;
      }

      if (nextName.equals("key")) {
        key = reader.nextString();
      } else if (nextName.equals("value")) {
        if (reader.peekString()) {
          stringValue = reader.nextString();
        } else if (reader.peekBoolean()) {
          booleanValue = reader.nextBoolean();
        } else {
          reader.skipValue();
        }
      } else if (nextName.equals("endpoint")) {
        endpoint = ENDPOINT_READER.fromJson(reader);
      } else {
        reader.skipValue();
      }
    }

    if (key == null) {
      throw new IllegalArgumentException("No key at " + reader.getPath());
    }
    reader.endObject();

    if (stringValue != null) {
      return V1BinaryAnnotation.createString(key, stringValue, endpoint);
    }
    if (booleanValue != null && booleanValue && endpoint != null) {
      return V1BinaryAnnotation.createAddress(key, endpoint);
    }
    return null; // toss unsupported data
  }

  static final JsonReaderAdapter<Endpoint> ENDPOINT_READER =
      new JsonReaderAdapter<Endpoint>() {
        @Override
        public Endpoint fromJson(JsonReader reader) throws IOException {
          Endpoint.Builder result = Endpoint.newBuilder();
          reader.beginObject();
          boolean readField = false;
          while (reader.hasNext()) {
            String nextName = reader.nextName();
            if (reader.peekNull()) {
              reader.skipValue();
              continue;
            }
            if (nextName.equals("serviceName")) {
              result.serviceName(reader.nextString());
              readField = true;
            } else if (nextName.equals("ipv4") || nextName.equals("ipv6")) {
              result.parseIp(reader.nextString());
              readField = true;
            } else if (nextName.equals("port")) {
              result.port(reader.nextInt());
              readField = true;
            } else {
              reader.skipValue();
            }
          }
          reader.endObject();
          return readField ? result.build() : null;
        }

        @Override
        public String toString() {
          return "Endpoint";
        }
      };
}
