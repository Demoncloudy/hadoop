/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.metrics2.impl;

import org.apache.hadoop.metrics2.AbstractMetric;
import org.apache.hadoop.metrics2.MetricsException;
import org.apache.hadoop.metrics2.MetricsRecord;
import org.apache.hadoop.metrics2.MetricsTag;
import org.apache.hadoop.metrics2.sink.GraphiteSink;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.internal.util.reflection.Whitebox;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.*;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.*;

public class TestGraphiteMetrics {
    private AbstractMetric makeMetric(String name, Number value) {
        AbstractMetric metric = mock(AbstractMetric.class);
        when(metric.name()).thenReturn(name);
        when(metric.value()).thenReturn(value);
        return metric;
    }

    @Test
    public void testPutMetrics() {
        GraphiteSink sink = new GraphiteSink();
        List<MetricsTag> tags = new ArrayList<MetricsTag>();
        tags.add(new MetricsTag(MsInfo.Context, "all"));
        tags.add(new MetricsTag(MsInfo.Hostname, "host"));
        Set<AbstractMetric> metrics = new HashSet<AbstractMetric>();
        metrics.add(makeMetric("foo1", 1.25));
        metrics.add(makeMetric("foo2", 2.25));
        MetricsRecord record = new MetricsRecordImpl(MsInfo.Context, (long) 10000, tags, metrics);

        OutputStreamWriter mockWriter = mock(OutputStreamWriter.class);
        ArgumentCaptor<String> argument = ArgumentCaptor.forClass(String.class);
        Whitebox.setInternalState(sink, "writer", mockWriter);
        sink.putMetrics(record);

        try {
            verify(mockWriter).write(argument.capture());
        } catch (IOException e) {
            e.printStackTrace();
        }

        String result = argument.getValue().toString();

        assertEquals(true,
                result.equals("null.all.Context.Context=all.Hostname=host.foo1 1.25 10\n" +
                        "null.all.Context.Context=all.Hostname=host.foo2 2.25 10\n") ||
                        result.equals("null.all.Context.Context=all.Hostname=host.foo2 2.25 10\n" +
                                "null.all.Context.Context=all.Hostname=host.foo1 1.25 10\n"));
    }

    @Test
    public void testPutMetrics2() {
        GraphiteSink sink = new GraphiteSink();
        List<MetricsTag> tags = new ArrayList<MetricsTag>();
        tags.add(new MetricsTag(MsInfo.Context, "all"));
        tags.add(new MetricsTag(MsInfo.Hostname, null));
        Set<AbstractMetric> metrics = new HashSet<AbstractMetric>();
        metrics.add(makeMetric("foo1", 1));
        metrics.add(makeMetric("foo2", 2));
        MetricsRecord record = new MetricsRecordImpl(MsInfo.Context, (long) 10000, tags, metrics);

        OutputStreamWriter mockWriter = mock(OutputStreamWriter.class);
        ArgumentCaptor<String> argument = ArgumentCaptor.forClass(String.class);
        Whitebox.setInternalState(sink, "writer", mockWriter);
        sink.putMetrics(record);

        try {
            verify(mockWriter).write(argument.capture());
        } catch (IOException e) {
            e.printStackTrace();
        }

        String result = argument.getValue().toString();

        assertEquals(true,
                result.equals("null.all.Context.Context=all.foo1 1 10\n" +
                        "null.all.Context.Context=all.foo2 2 10\n") ||
                        result.equals("null.all.Context.Context=all.foo2 2 10\n" +
                                "null.all.Context.Context=all.foo1 1 10\n"));
    }

    /**
     * Assert that timestamps are converted correctly, ticket HADOOP-11182
     */
    @Test
    public void testPutMetrics3() {

        // setup GraphiteSink
        GraphiteSink sink = new GraphiteSink();
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        Whitebox.setInternalState(sink, "writer", new OutputStreamWriter(out));

        // given two metrics records with timestamps 1000 milliseconds apart.
        List<MetricsTag> tags = Collections.emptyList();
        Set<AbstractMetric> metrics = new HashSet<AbstractMetric>();
        metrics.add(makeMetric("foo1", 1));
        MetricsRecord record1 = new MetricsRecordImpl(MsInfo.Context, 1000000000000L, tags, metrics);
        MetricsRecord record2 = new MetricsRecordImpl(MsInfo.Context, 1000000001000L, tags, metrics);

        sink.putMetrics(record1);
        sink.putMetrics(record2);

        sink.flush();
        try {
            sink.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

        // then the timestamps in the graphite stream should differ by one second.
        String expectedOutput
                = "null.default.Context.foo1 1 1000000000\n"
                + "null.default.Context.foo1 1 1000000001\n";
        assertEquals(expectedOutput, out.toString());
    }


    @Test(expected = MetricsException.class)
    public void testCloseAndWrite() throws IOException {
        GraphiteSink sink = new GraphiteSink();
        List<MetricsTag> tags = new ArrayList<MetricsTag>();
        tags.add(new MetricsTag(MsInfo.Context, "all"));
        tags.add(new MetricsTag(MsInfo.Hostname, "host"));
        Set<AbstractMetric> metrics = new HashSet<AbstractMetric>();
        metrics.add(makeMetric("foo1", 1.25));
        metrics.add(makeMetric("foo2", 2.25));
        MetricsRecord record = new MetricsRecordImpl(MsInfo.Context, (long) 10000, tags, metrics);

        OutputStreamWriter writer = mock(OutputStreamWriter.class);

        Whitebox.setInternalState(sink, "writer", writer);
        sink.close();
        sink.putMetrics(record);
    }

    @Test
    public void testClose() {
        GraphiteSink sink = new GraphiteSink();
        Writer mockWriter = mock(Writer.class);
        Whitebox.setInternalState(sink, "writer", mockWriter);
        try {
            sink.close();
        } catch (IOException ioe) {
            ioe.printStackTrace();
        }

        try {
            verify(mockWriter).close();
        } catch (IOException ioe) {
            ioe.printStackTrace();
        }
    }
}
