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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import zipkin2.Endpoint;
import zipkin2.Span;
import zipkin2.codec.SpanBytesDecoderTest;

import static org.assertj.core.api.Assertions.assertThat;
import static zipkin2.TestObjects.BACKEND;
import static zipkin2.codec.SpanBytesEncoderTest.LOCAL_SPAN;
import static zipkin2.codec.SpanBytesEncoderTest.NO_ANNOTATIONS_ROOT_SERVER_SPAN;
import static zipkin2.codec.SpanBytesEncoderTest.SPAN;
import static zipkin2.codec.SpanBytesEncoderTest.UTF8_SPAN;
import static zipkin2.codec.SpanBytesEncoderTest.UTF_8;

/** Copy of {@link SpanBytesDecoderTest} */
public class V1SpanBytesDecoderTest {
  Span span = SPAN;

  @Rule public ExpectedException thrown = ExpectedException.none();

  @Test
  public void spanRoundTrip_JSON() {
    assertThat(V1SpanBytesDecoder.JSON.decodeOne(V1SpanBytesEncoder.JSON.encode(span)))
        .isEqualTo(span);
  }

  @Test
  public void spanRoundTrip_THRIFT() {
    assertThat(V1SpanBytesDecoder.THRIFT.decodeOne(V1SpanBytesEncoder.THRIFT.encode(span)))
        .isEqualTo(span);
  }

  @Test
  public void localSpanRoundTrip_JSON() {
    assertThat(V1SpanBytesDecoder.JSON.decodeOne(V1SpanBytesEncoder.JSON.encode(LOCAL_SPAN)))
        .isEqualTo(LOCAL_SPAN);
  }

  @Test
  public void localSpanRoundTrip_THRIFT() {
    assertThat(V1SpanBytesDecoder.THRIFT.decodeOne(V1SpanBytesEncoder.THRIFT.encode(LOCAL_SPAN)))
        .isEqualTo(LOCAL_SPAN);
  }

  @Test
  public void spanRoundTrip_64bitTraceId_JSON() {
    span = span.toBuilder().traceId(span.traceId().substring(16)).build();

    assertThat(V1SpanBytesDecoder.JSON.decodeOne(V1SpanBytesEncoder.JSON.encode(span)))
        .isEqualTo(span);
  }

  @Test
  public void spanRoundTrip_64bitTraceId_THRIFT() {
    span = span.toBuilder().traceId(span.traceId().substring(16)).build();

    assertThat(V1SpanBytesDecoder.THRIFT.decodeOne(V1SpanBytesEncoder.THRIFT.encode(span)))
        .isEqualTo(span);
  }

  @Test
  public void spanRoundTrip_shared_JSON() {
    span = span.toBuilder().kind(Span.Kind.SERVER).shared(true).build();

    assertThat(V1SpanBytesDecoder.JSON.decodeOne(V1SpanBytesEncoder.JSON.encode(span)))
        .isEqualTo(span);
  }

  @Test
  public void spanRoundTrip_shared_THRIFT() {
    span = span.toBuilder().kind(Span.Kind.SERVER).shared(true).build();

    assertThat(V1SpanBytesDecoder.THRIFT.decodeOne(V1SpanBytesEncoder.THRIFT.encode(span)))
        .isEqualTo(span);
  }

  /**
   * This isn't a test of what we "should" accept as a span, rather that characters that trip-up
   * json don't fail in codec.
   */
  @Test
  public void specialCharsInJson_JSON() {
    assertThat(V1SpanBytesDecoder.JSON.decodeOne(V1SpanBytesEncoder.JSON.encode(UTF8_SPAN)))
        .isEqualTo(UTF8_SPAN);
  }

  @Test
  public void specialCharsInJson_THRIFT() {
    assertThat(V1SpanBytesDecoder.THRIFT.decodeOne(V1SpanBytesEncoder.THRIFT.encode(UTF8_SPAN)))
        .isEqualTo(UTF8_SPAN);
  }

  @Test
  public void falseOnEmpty_inputSpans_JSON() {
    assertThat(V1SpanBytesDecoder.JSON.decodeList(new byte[0], new ArrayList<>())).isFalse();
  }

  @Test
  public void falseOnEmpty_inputSpans_THRIFT() {
    assertThat(V1SpanBytesDecoder.THRIFT.decodeList(new byte[0], new ArrayList<>())).isFalse();
  }

  /** Particulary, thrift can mistake malformed content as a huge list. Let's not blow up. */
  @Test
  public void niceErrorOnMalformed_inputSpans_JSON() {
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("Malformed reading List<Span> from ");

    V1SpanBytesDecoder.JSON.decodeList(new byte[] {'h', 'e', 'l', 'l', 'o'});
  }

  @Test
  public void niceErrorOnMalformed_inputSpans_THRIFT() {
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage(
        "Truncated: length 1701604463 > bytes remaining 0 reading List<Span> from TBinary");

    V1SpanBytesDecoder.THRIFT.decodeList(new byte[] {'h', 'e', 'l', 'l', 'o'});
  }

  @Test
  public void spansRoundTrip_JSON() {
    List<Span> tenClientSpans = Collections.nCopies(10, span);

    byte[] message = V1SpanBytesEncoder.JSON.encodeList(tenClientSpans);

    assertThat(V1SpanBytesDecoder.JSON.decodeList(message)).isEqualTo(tenClientSpans);
  }

  @Test
  public void spansRoundTrip_THRIFT() {
    List<Span> tenClientSpans = Collections.nCopies(10, span);

    byte[] message = V1SpanBytesEncoder.THRIFT.encodeList(tenClientSpans);

    assertThat(V1SpanBytesDecoder.THRIFT.decodeList(message)).isEqualTo(tenClientSpans);
  }

  @Test
  public void spanRoundTrip_noRemoteServiceName_JSON() {
    span = span.toBuilder().remoteEndpoint(BACKEND.toBuilder().serviceName(null).build()).build();

    assertThat(V1SpanBytesDecoder.JSON.decodeOne(V1SpanBytesEncoder.JSON.encode(span)))
        .isEqualTo(span);
  }

  @Test
  public void spanRoundTrip_noRemoteServiceName_THRIFT() {
    span = span.toBuilder().remoteEndpoint(BACKEND.toBuilder().serviceName(null).build()).build();

    assertThat(V1SpanBytesDecoder.THRIFT.decodeOne(V1SpanBytesEncoder.THRIFT.encode(span)))
        .isEqualTo(span);
  }

  @Test
  public void spanRoundTrip_noAnnotations_rootServerSpan_JSON() {
    span = NO_ANNOTATIONS_ROOT_SERVER_SPAN;

    assertThat(V1SpanBytesDecoder.JSON.decodeOne(V1SpanBytesEncoder.JSON.encode(span)))
        .isEqualTo(span);
  }

  @Test
  public void spanRoundTrip_noAnnotations_rootServerSpan_THRIFT() {
    span = NO_ANNOTATIONS_ROOT_SERVER_SPAN;

    assertThat(V1SpanBytesDecoder.THRIFT.decodeOne(V1SpanBytesEncoder.THRIFT.encode(span)))
        .isEqualTo(span);
  }

  @Test
  public void spanRoundTrip_noAnnotations_rootServerSpan_incomplete_JSON() {
    span = NO_ANNOTATIONS_ROOT_SERVER_SPAN.toBuilder().duration(null).build();

    assertThat(V1SpanBytesDecoder.JSON.decodeOne(V1SpanBytesEncoder.JSON.encode(span)))
        .isEqualTo(span);
  }

  @Test
  public void spanRoundTrip_noAnnotations_rootServerSpan_incomplete_THRIFT() {
    span = NO_ANNOTATIONS_ROOT_SERVER_SPAN.toBuilder().duration(null).build();

    assertThat(V1SpanBytesDecoder.THRIFT.decodeOne(V1SpanBytesEncoder.THRIFT.encode(span)))
        .isEqualTo(span);
  }

  @Test
  public void spanRoundTrip_noAnnotations_rootServerSpan_shared_JSON() {
    span = NO_ANNOTATIONS_ROOT_SERVER_SPAN.toBuilder().shared(true).build();

    assertThat(V1SpanBytesDecoder.JSON.decodeOne(V1SpanBytesEncoder.JSON.encode(span)))
        .isEqualTo(span);
  }

  @Test
  public void spanRoundTrip_noAnnotations_rootServerSpan_shared_THRIFT() {
    span = NO_ANNOTATIONS_ROOT_SERVER_SPAN.toBuilder().shared(true).build();

    assertThat(V1SpanBytesDecoder.THRIFT.decodeOne(V1SpanBytesEncoder.THRIFT.encode(span)))
        .isEqualTo(span);
  }

  @Test @Ignore
  public void niceErrorOnUppercase_traceId_JSON() {
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("48485A3953BB6124 should be lower-hex encoded with no prefix");

    String json =
        "{\n"
            + "  \"traceId\": \"48485A3953BB6124\",\n"
            + "  \"name\": \"get-traces\",\n"
            + "  \"id\": \"6b221d5bc9e6496c\"\n"
            + "}";

    V1SpanBytesDecoder.JSON.decodeOne(json.getBytes(UTF_8));
  }

  @Test
  public void readsTraceIdHighFromTraceIdField() {
    byte[] with128BitTraceId =
        ("{\n"
                + "  \"traceId\": \"48485a3953bb61246b221d5bc9e6496c\",\n"
                + "  \"name\": \"get-traces\",\n"
                + "  \"id\": \"6b221d5bc9e6496c\"\n"
                + "}")
            .getBytes(UTF_8);
    byte[] withLower64bitsTraceId =
        ("{\n"
                + "  \"traceId\": \"6b221d5bc9e6496c\",\n"
                + "  \"name\": \"get-traces\",\n"
                + "  \"id\": \"6b221d5bc9e6496c\"\n"
                + "}")
            .getBytes(UTF_8);

    assertThat(V1SpanBytesDecoder.JSON.decodeOne(with128BitTraceId))
        .isEqualTo(
            V1SpanBytesDecoder.JSON
                .decodeOne(withLower64bitsTraceId)
                .toBuilder()
                .traceId("48485a3953bb61246b221d5bc9e6496c")
                .build());
  }

  @Test
  public void ignoresNull_topLevelFields() {
    String json =
        "{\n"
            + "  \"traceId\": \"6b221d5bc9e6496c\",\n"
            + "  \"parentId\": null,\n"
            + "  \"id\": \"6b221d5bc9e6496c\",\n"
            + "  \"name\": null,\n"
            + "  \"timestamp\": null,\n"
            + "  \"duration\": null,\n"
            + "  \"annotations\": null,\n"
            + "  \"binaryAnnotations\": null,\n"
            + "  \"debug\": null,\n"
            + "  \"shared\": null\n"
            + "}";

    V1SpanBytesDecoder.JSON.decodeOne(json.getBytes(UTF_8));
  }

  @Test
  public void ignoresNull_endpoint_topLevelFields() {
    String json =
        "{\n"
            + "  \"traceId\": \"6b221d5bc9e6496c\",\n"
            + "  \"id\": \"6b221d5bc9e6496c\",\n"
            + "  \"binaryAnnotations\": [\n"
            + "    {\n"
            + "      \"key\": \"lc\",\n"
            + "      \"value\": \"\",\n"
            + "      \"endpoint\": {\n"
            + "        \"serviceName\": null,\n"
            + "    \"ipv4\": \"127.0.0.1\",\n"
            + "        \"ipv6\": null,\n"
            + "        \"port\": null\n"
            + "      }\n"
            + "    }\n"
            + "  ]\n"
            + "}";

    assertThat(V1SpanBytesDecoder.JSON.decodeOne(json.getBytes(UTF_8)).localEndpoint())
        .isEqualTo(Endpoint.newBuilder().ip("127.0.0.1").build());
  }

  @Test
  public void skipsIncompleteEndpoint() {
    String json =
        "{\n"
            + "  \"traceId\": \"6b221d5bc9e6496c\",\n"
            + "  \"id\": \"6b221d5bc9e6496c\",\n"
            + "  \"binaryAnnotations\": [\n"
            + "    {\n"
            + "      \"key\": \"lc\",\n"
            + "      \"value\": \"\",\n"
            + "      \"endpoint\": {\n"
            + "        \"serviceName\": null,\n"
            + "        \"ipv4\": null,\n"
            + "        \"ipv6\": null,\n"
            + "        \"port\": null\n"
            + "      }\n"
            + "    }\n"
            + "  ]\n"
            + "}";
    assertThat(V1SpanBytesDecoder.JSON.decodeOne(json.getBytes(UTF_8)).localEndpoint()).isNull();
    json =
        "{\n"
            + "  \"traceId\": \"6b221d5bc9e6496c\",\n"
            + "  \"id\": \"6b221d5bc9e6496c\",\n"
            + "  \"binaryAnnotations\": [\n"
            + "    {\n"
            + "      \"key\": \"lc\",\n"
            + "      \"value\": \"\",\n"
            + "      \"endpoint\": {\n"
            + "      }\n"
            + "    }\n"
            + "  ]\n"
            + "}";
    assertThat(V1SpanBytesDecoder.JSON.decodeOne(json.getBytes(UTF_8)).localEndpoint()).isNull();
  }

  @Test
  public void niceErrorOnIncomplete_annotation() {
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("Incomplete annotation at $.annotations[0].timestamp");

    String json =
        "{\n"
            + "  \"traceId\": \"6b221d5bc9e6496c\",\n"
            + "  \"name\": \"get-traces\",\n"
            + "  \"id\": \"6b221d5bc9e6496c\",\n"
            + "  \"annotations\": [\n"
            + "    { \"timestamp\": 1472470996199000}\n"
            + "  ]\n"
            + "}";

    V1SpanBytesDecoder.JSON.decodeOne(json.getBytes(UTF_8));
  }

  @Test
  public void niceErrorOnNull_traceId() {
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("Expected a string but was NULL");

    String json =
        "{\n"
            + "  \"traceId\": null,\n"
            + "  \"name\": \"get-traces\",\n"
            + "  \"id\": \"6b221d5bc9e6496c\"\n"
            + "}";

    V1SpanBytesDecoder.JSON.decodeOne(json.getBytes(UTF_8));
  }

  @Test
  public void niceErrorOnNull_id() {
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("Expected a string but was NULL");

    String json =
        "{\n"
            + "  \"traceId\": \"6b221d5bc9e6496c\",\n"
            + "  \"name\": \"get-traces\",\n"
            + "  \"id\": null\n"
            + "}";

    V1SpanBytesDecoder.JSON.decodeOne(json.getBytes(UTF_8));
  }

  @Test
  public void niceErrorOnNull_annotationValue() {
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("$.annotations[0].value");

    String json =
        "{\n"
            + "  \"traceId\": \"6b221d5bc9e6496c\",\n"
            + "  \"name\": \"get-traces\",\n"
            + "  \"id\": \"6b221d5bc9e6496c\",\n"
            + "  \"annotations\": [\n"
            + "    { \"timestamp\": 1472470996199000, \"value\": NULL}\n"
            + "  ]\n"
            + "}";

    V1SpanBytesDecoder.JSON.decodeOne(json.getBytes(UTF_8));
  }

  @Test
  public void niceErrorOnNull_annotationTimestamp() {
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("$.annotations[0].timestamp");

    String json =
        "{\n"
            + "  \"traceId\": \"6b221d5bc9e6496c\",\n"
            + "  \"name\": \"get-traces\",\n"
            + "  \"id\": \"6b221d5bc9e6496c\",\n"
            + "  \"annotations\": [\n"
            + "    { \"timestamp\": NULL, \"value\": \"foo\"}\n"
            + "  ]\n"
            + "}";

    V1SpanBytesDecoder.JSON.decodeOne(json.getBytes(UTF_8));
  }

  @Test
  public void readSpan_localEndpoint_noServiceName() {
    String json =
        "{\n"
            + "  \"traceId\": \"6b221d5bc9e6496c\",\n"
            + "  \"name\": \"get-traces\",\n"
            + "  \"id\": \"6b221d5bc9e6496c\",\n"
            + "  \"localEndpoint\": {\n"
            + "    \"ipv4\": \"127.0.0.1\"\n"
            + "  }\n"
            + "}";

    assertThat(V1SpanBytesDecoder.JSON.decodeOne(json.getBytes(UTF_8)).localServiceName()).isNull();
  }

  @Test
  public void readSpan_remoteEndpoint_noServiceName() {
    String json =
        "{\n"
            + "  \"traceId\": \"6b221d5bc9e6496c\",\n"
            + "  \"name\": \"get-traces\",\n"
            + "  \"id\": \"6b221d5bc9e6496c\",\n"
            + "  \"remoteEndpoint\": {\n"
            + "    \"ipv4\": \"127.0.0.1\"\n"
            + "  }\n"
            + "}";

    assertThat(V1SpanBytesDecoder.JSON.decodeOne(json.getBytes(UTF_8)).remoteServiceName())
        .isNull();
  }
}
