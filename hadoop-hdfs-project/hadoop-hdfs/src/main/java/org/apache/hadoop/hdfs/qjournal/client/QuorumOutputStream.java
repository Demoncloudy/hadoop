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
package org.apache.hadoop.hdfs.qjournal.client;

import org.apache.hadoop.hdfs.server.namenode.EditLogOutputStream;
import org.apache.hadoop.hdfs.server.namenode.EditsDoubleBuffer;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp;
import org.apache.hadoop.io.DataOutputBuffer;

import java.io.IOException;

/**
 * EditLogOutputStream implementation that writes to a quorum of
 * remote journals.
 */
class QuorumOutputStream extends EditLogOutputStream {
    private final AsyncLoggerSet loggers;
    private final long segmentTxId;
    private final int writeTimeoutMs;
    private EditsDoubleBuffer buf;

    public QuorumOutputStream(AsyncLoggerSet loggers,
                              long txId, int outputBufferCapacity,
                              int writeTimeoutMs) throws IOException {
        super();
        this.buf = new EditsDoubleBuffer(outputBufferCapacity);
        this.loggers = loggers;
        this.segmentTxId = txId;
        this.writeTimeoutMs = writeTimeoutMs;
    }

    @Override
    public void write(FSEditLogOp op) throws IOException {
        // 也是双缓冲写入
        buf.writeOp(op);
    }

    @Override
    public void writeRaw(byte[] bytes, int offset, int length) throws IOException {
        buf.writeRaw(bytes, offset, length);
    }

    @Override
    public void create(int layoutVersion) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void close() throws IOException {
        if (buf != null) {
            buf.close();
            buf = null;
        }
    }

    @Override
    public void abort() throws IOException {
        QuorumJournalManager.LOG.warn("Aborting " + this);
        buf = null;
        close();
    }

    @Override
    public void setReadyToFlush() throws IOException {
        buf.setReadyToFlush();
    }

    @Override
    protected void flushAndSync(boolean durable) throws IOException {
        int numReadyBytes = buf.countReadyBytes();
        if (numReadyBytes > 0) {
            int numReadyTxns = buf.countReadyTxns();
            long firstTxToFlush = buf.getFirstReadyTxId();

            assert numReadyTxns > 0;

            // Copy from our double-buffer into a new byte array. This is for
            // two reasons:
            // 1) The IPC code has no way of specifying to send only a slice of
            //    a larger array.
            // 2) because the calls to the underlying nodes are asynchronous, we
            //    need a defensive copy to avoid accidentally mutating the buffer
            //    before it is sent.
            // 将缓冲中的数据拷贝到字节数组中去
            // 1. IPC代码, rpc调用就是需要一次性将缓冲区里的数据全部发送到journalnode上去,
            // 通过rpc接口请求没有办法每次就发送一个大的数组的一部分
            // 2. journal node的调用是异步的, 需要一个防御性的拷贝区避免在发送这段数据之前buffer被修改
            DataOutputBuffer bufToSend = new DataOutputBuffer(numReadyBytes);
            // 将buffer里面的数据, 先写入了一个新得dataoutputbuffer里面去
            buf.flushTo(bufToSend);
            assert bufToSend.getLength() == numReadyBytes;
            byte[] data = bufToSend.getData();
            assert data.length == bufToSend.getLength();
            // 通过AsyncLoggerSet这个组件, 异步发送edits log到大多数journal node上去
            QuorumCall<AsyncLogger, Void> qcall = loggers.sendEdits(
                    segmentTxId, firstTxToFlush,
                    numReadyTxns, data);
            // 等待到大多数journal node都成功以后, 在继续执行
            loggers.waitForWriteQuorum(qcall, writeTimeoutMs, "sendEdits");

            // Since we successfully wrote this batch, let the loggers know. Any future
            // RPCs will thus let the loggers know of the most recent transaction, even
            // if a logger has fallen behind.
            loggers.setCommittedTxId(firstTxToFlush + numReadyTxns - 1);
        }
    }

    @Override
    public String generateReport() {
        StringBuilder sb = new StringBuilder();
        sb.append("Writing segment beginning at txid " + segmentTxId + ". \n");
        loggers.appendReport(sb);
        return sb.toString();
    }

    @Override
    public String toString() {
        return "QuorumOutputStream starting at txid " + segmentTxId;
    }
}
