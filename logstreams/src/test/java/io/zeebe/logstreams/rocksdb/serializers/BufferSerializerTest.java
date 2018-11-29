/*
 * Copyright © 2017 camunda services GmbH (info@camunda.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.zeebe.logstreams.rocksdb.serializers;

import static io.zeebe.util.buffer.BufferUtil.wrapString;
import static org.assertj.core.api.Assertions.assertThat;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import org.agrona.DirectBuffer;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

@RunWith(Parameterized.class)
public class BufferSerializerTest<T> {

  @Parameter(0)
  public String name;

  @Parameter(1)
  public T value;

  @Parameter(2)
  public BufferSerializer<T> serializer;

  @Parameter(3)
  public int length;

  @Parameters(name = "{0}")
  public static Collection<Object[]> data() {
    final List<Object[]> parameters = new ArrayList<>();

    parameters.add(
        new Object[] {
          "direct buffer", wrapString("foo"), Serializers.DIRECT_BUFFER, Serializer.VARIABLE_LENGTH
        });

    final ByteBuffer byteBufferValue = ByteBuffer.wrap("foo".getBytes());
    final ByteBuffer byteBufferInstance = ByteBuffer.allocate(byteBufferValue.limit());
    parameters.add(
        new Object[] {
          "byte buffer",
          byteBufferValue,
          Serializers.newByteBuffer(byteBufferInstance),
          byteBufferInstance.limit()
        });

    final byte[] byteArrayValue = "foo".getBytes();
    final byte[] byteArrayInstance = new byte[byteArrayValue.length];
    parameters.add(
        new Object[] {
          "byte array",
          byteArrayValue,
          Serializers.newByteArray(byteArrayInstance),
          byteArrayInstance.length
        });

    final UnsafeBuffer fixedUnsafeBufferValue = new UnsafeBuffer("foo".getBytes());
    final UnsafeBuffer fixedUnsafeBufferInstance =
        new UnsafeBuffer(new byte[fixedUnsafeBufferValue.capacity()]);
    parameters.add(
        new Object[] {
          "fixed unsafe buffer",
          fixedUnsafeBufferValue,
          Serializers.newMutableBuffer(fixedUnsafeBufferInstance),
          fixedUnsafeBufferInstance.capacity()
        });

    final byte[] expandableArrayBufferBytes = "foo".getBytes();
    final ExpandableArrayBuffer expandableArrayBufferValue = new ExpandableArrayBuffer();
    final ExpandableArrayBuffer expandableArrayBuffer = new ExpandableArrayBuffer();
    expandableArrayBufferValue.putBytes(0, expandableArrayBufferBytes);
    parameters.add(
        new Object[] {
          "expandable array buffer",
          expandableArrayBufferValue,
          Serializers.newMutableBuffer(expandableArrayBuffer),
          Serializer.VARIABLE_LENGTH
        });

    return parameters;
  }

  @Test
  public void shouldReturnExpectedLength() {
    // then
    assertThat(serializer.getLength()).isEqualTo(length);
  }

  @Test
  public void shouldSerializeAndDeserializePrimitive() {
    // given
    final MutableDirectBuffer buffer = new ExpandableArrayBuffer();

    // when
    final DirectBuffer serialized = serializer.serialize(value, buffer, 0);
    final T deserialized = serializer.deserialize(serialized, 0, serialized.capacity());

    // then
    assertThat(deserialized).isEqualTo(value);
  }
}