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

import org.junit.Test;
import zipkin2.Span;
import zipkin2.v1.internal.V1ThriftSpanWriterTest;

import static org.assertj.core.api.Assertions.assertThat;
import static zipkin2.TestObjects.BACKEND;
import static zipkin2.TestObjects.FRONTEND;
import static zipkin2.codec.SpanBytesEncoderTest.LOCAL_SPAN;
import static zipkin2.codec.SpanBytesEncoderTest.NO_ANNOTATIONS_ROOT_SERVER_SPAN;
import static zipkin2.codec.SpanBytesEncoderTest.SPAN;
import static zipkin2.codec.SpanBytesEncoderTest.UTF8_SPAN;

/**
 * Test for {@link V1SpanBytesEncoder#THRIFT} are sanity-check only as the corresponding tests are
 * in {@link V1ThriftSpanWriterTest}.
 */
public class V1SpanBytesEncoderTest {

  Span span = SPAN;

  @Test
  public void span_THRIFT() {
    assertThat(V1SpanBytesEncoder.THRIFT.encode(span)).hasSize(503);
  }

  @Test
  public void localSpan_THRIFT() {
    assertThat(V1SpanBytesEncoder.THRIFT.encode(LOCAL_SPAN)).hasSize(127);
  }

  @Test
  public void span_64bitTraceId_THRIFT() {
    span = span.toBuilder().traceId(span.traceId().substring(16)).build();

    assertThat(V1SpanBytesEncoder.THRIFT.encode(span)).hasSize(492);
  }

  @Test
  public void span_shared_THRIFT() {
    span = span.toBuilder().kind(Span.Kind.SERVER).shared(true).build();

    assertThat(V1SpanBytesEncoder.THRIFT.encode(span)).hasSize(481);
  }

  @Test
  public void specialCharsInJson_THRIFT() {
    span = UTF8_SPAN;

    assertThat(V1SpanBytesEncoder.THRIFT.encode(span)).hasSize(176);
  }

  @Test
  public void span_minimum_THRIFT() {
    span =
        Span.newBuilder()
            .traceId("7180c278b62e8f6a216a2aea45d08fc9")
            .id("5b4185666d50f68b")
            .build();

    assertThat(V1SpanBytesEncoder.THRIFT.encode(span)).hasSize(57);
  }

  @Test
  public void span_noLocalServiceName_THRIFT() {
    span = span.toBuilder().localEndpoint(FRONTEND.toBuilder().serviceName(null).build()).build();

    assertThat(V1SpanBytesEncoder.THRIFT.encode(span)).hasSize(455);
  }

  @Test
  public void span_noRemoteServiceName_THRIFT() {
    span = span.toBuilder().remoteEndpoint(BACKEND.toBuilder().serviceName(null).build()).build();

    assertThat(V1SpanBytesEncoder.THRIFT.encode(span)).hasSize(496);
  }

  @Test
  public void noAnnotations_rootServerSpan_THRIFT() {
    span = NO_ANNOTATIONS_ROOT_SERVER_SPAN;

    assertThat(V1SpanBytesEncoder.THRIFT.encode(span)).hasSize(358);
  }

  @Test
  public void noAnnotations_rootServerSpan_THRIFT_incomplete() {
    span = NO_ANNOTATIONS_ROOT_SERVER_SPAN.toBuilder().duration(null).build();

    assertThat(V1SpanBytesEncoder.THRIFT.encode(span)).hasSize(297);
  }

  @Test
  public void noAnnotations_rootServerSpan_THRIFT_shared() {
    span = NO_ANNOTATIONS_ROOT_SERVER_SPAN.toBuilder().shared(true).build();

    assertThat(V1SpanBytesEncoder.THRIFT.encode(span)).hasSize(336);
  }
}
