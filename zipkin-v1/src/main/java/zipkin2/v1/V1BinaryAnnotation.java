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

import zipkin2.Endpoint;
import zipkin2.Span;
import zipkin2.internal.Nullable;

/**
 * This only supports binary annotations that map to {@link Span v2 span} data. Namely, this
 * supports {@link Span#tags()}, {@link Span#localEndpoint()} and {@link Span#remoteEndpoint()}.
 *
 * <p>Specifically, this maps String and Boolean binary annotations, ignoring others.
 */
public final class V1BinaryAnnotation implements Comparable<V1BinaryAnnotation> {

  /** Creates an address annotation, which is the same as {@link Span#remoteEndpoint()} */
  public static V1BinaryAnnotation createAddress(String key, Endpoint endpoint) {
    if (endpoint == null) throw new NullPointerException("endpoint == null");
    return new V1BinaryAnnotation(key, null, true, endpoint);
  }

  /**
   * Creates a tag annotation, which is the same as {@link Span#tags()} except duplicating the
   * endpoint.
   *
   * <p>A special case is when the key is "lc" and value is empty: This substitutes for the {@link
   * Span#localEndpoint()}.
   */
  public static V1BinaryAnnotation createString(String key, String value, Endpoint endpoint) {
    if (value == null) throw new NullPointerException("value == null");
    return new V1BinaryAnnotation(key, value, false, endpoint);
  }

  /** The same as the key of a {@link Span#tags()} v2 span tag} */
  public String key() {
    return key;
  }

  /** The same as the value of a {@link Span#tags()} v2 span tag} or null if this is an address */
  @Nullable
  public String stringValue() {
    return stringValue;
  }

  /** Indicates the {@link #endpoint} is likely the {@link Span#remoteEndpoint()} */
  public boolean booleanValue() {
    return booleanValue;
  }

  /**
   * When {@link #stringValue()} is present, this is the same as the {@link Span#localEndpoint()}
   * Otherwise, it is the same as the {@link Span#remoteEndpoint()}.
   */
  public Endpoint endpoint() {
    return endpoint;
  }

  final String key, stringValue;
  final boolean booleanValue;
  final Endpoint endpoint;

  V1BinaryAnnotation(String key, String stringValue, boolean booleanValue, Endpoint endpoint) {
    if (key == null) throw new NullPointerException("key == null");
    this.key = key;
    this.stringValue = stringValue;
    this.booleanValue = booleanValue;
    this.endpoint = endpoint;
  }

  /** Provides consistent iteration by {@link #key} */
  @Override
  public int compareTo(V1BinaryAnnotation that) {
    if (this == that) return 0;
    return key.compareTo(that.key);
  }
}
