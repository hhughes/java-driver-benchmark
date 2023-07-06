/*
 * Copyright (c) 2014, Oracle America, Inc.
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 *  * Redistributions of source code must retain the above copyright notice,
 *    this list of conditions and the following disclaimer.
 *
 *  * Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in the
 *    documentation and/or other materials provided with the distribution.
 *
 *  * Neither the name of Oracle nor the names of its contributors may be used
 *    to endorse or promote products derived from this software without
 *    specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF
 * THE POSSIBILITY OF SUCH DAMAGE.
 */

package com.datastax.oss;

import com.datastax.oss.driver.api.core.ProtocolVersion;
import com.datastax.oss.driver.api.core.data.CqlVector;
import com.datastax.oss.driver.api.core.type.codec.ExtraTypeCodecs;
import com.datastax.oss.driver.api.core.type.codec.TypeCodec;
import com.datastax.oss.driver.api.core.type.codec.TypeCodecs;
import com.datastax.oss.driver.internal.core.type.codec.VectorCodec;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.math3.random.JDKRandomGenerator;
import org.apache.commons.math3.random.RandomGenerator;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

@Warmup(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@BenchmarkMode(Mode.AverageTime)
@State(Scope.Thread)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
public class VectorEncodingBenchmark {

    @Param({"2", "512", "4096"})
    int input_length;
    float[] INPUT_ARRAY;

    @Setup
    public void setUp() {
        INPUT_ARRAY = new float[input_length];
        RandomGenerator r = new JDKRandomGenerator();
        for (int i = 0; i< INPUT_ARRAY.length; i++) {
            INPUT_ARRAY[i] = r.nextFloat();
        }
    }


    @Benchmark
    public void commons_to_object() {
        VectorCodec<Float> codec = new VectorCodec<>(input_length, TypeCodecs.FLOAT);
        CqlVector<Float> vector = CqlVector.newInstance(ArrayUtils.toObject(INPUT_ARRAY));
        codec.encode(vector, ProtocolVersion.V5);
    }

    @Benchmark
    public void copy_to_boxed_array() {
        VectorCodec<Float> codec = new VectorCodec<>(input_length, TypeCodecs.FLOAT);
        Float[] boxed = new Float[input_length];
        for (int i=0; i<input_length; i++) {
            boxed[i] = INPUT_ARRAY[i];
        }
        CqlVector<Float> vector = CqlVector.newInstance(boxed);
        codec.encode(vector, ProtocolVersion.V5);
    }

    @Benchmark
    public void copy_to_list() {
        VectorCodec<Float> codec = new VectorCodec<>(input_length, TypeCodecs.FLOAT);
        List<Float> list = new ArrayList<>(input_length);
        for (int i=0; i<input_length; i++) {
            list.add(INPUT_ARRAY[i]);
        }
        CqlVector<Float> vector = CqlVector.newInstance(list);
        codec.encode(vector, ProtocolVersion.V5);
    }

    @Benchmark
    public void custom_type() {
        TypeCodec<float[]> codec = ExtraTypeCodecs.floatVectorToArray(input_length);
        codec.encode(INPUT_ARRAY, ProtocolVersion.V5);
    }
}
