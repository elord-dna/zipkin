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
package zipkin2.v1;

import java.util.List;
import zipkin2.Span;
import zipkin2.codec.BytesEncoder;
import zipkin2.codec.Encoding;
import zipkin2.codec.SpanBytesEncoder;
import zipkin2.v1.internal.V1ThriftSpanWriter;

/** Like {@link SpanBytesEncoder} except for v1 format. */
@SuppressWarnings("ImmutableEnumChecker") // because span is immutable
public enum V1SpanBytesEncoder implements BytesEncoder<Span> {
  /** Corresponds to the Zipkin v1 json format (with tags as binary annotations) */
  JSON {
    final SpanBytesEncoder delegate = SpanBytesEncoder.JSON_V1;

    @Override
    public Encoding encoding() {
      return Encoding.JSON;
    }

    @Override
    public int sizeInBytes(Span input) {
      return delegate.sizeInBytes(input);
    }

    @Override
    public byte[] encode(Span span) {
      return delegate.encode(span);
    }

    @Override
    public byte[] encodeList(List<Span> spans) {
      return delegate.encodeList(spans);
    }

    @Override
    public int encodeList(List<Span> spans, byte[] out, int pos) {
      return delegate.encodeList(spans, out, pos);
    }
  },
  THRIFT {
    final V1ThriftSpanWriter codec = new V1ThriftSpanWriter();

    @Override
    public Encoding encoding() {
      return Encoding.THRIFT;
    }

    @Override
    public int sizeInBytes(Span input) {
      return codec.sizeInBytes(input);
    }

    @Override
    public byte[] encode(Span span) {
      return codec.write(span);
    }

    @Override
    public byte[] encodeList(List<Span> spans) {
      return codec.writeList(spans);
    }

    @Override
    public int encodeList(List<Span> spans, byte[] out, int pos) {
      return codec.writeList(spans, out, pos);
    }
  };

  /** Allows you to encode a list of spans onto a specific offset. For example, when nesting */
  public abstract int encodeList(List<Span> spans, byte[] out, int pos);
}
