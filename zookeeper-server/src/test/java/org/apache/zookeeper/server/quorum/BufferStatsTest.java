/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.zookeeper.server.quorum;

import org.apache.jute.BinaryInputArchive;
import org.apache.jute.BinaryOutputArchive;
import org.apache.jute.Record;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.metrics.Counter;
import org.apache.zookeeper.proto.ReplyHeader;
import org.apache.zookeeper.proto.WatcherEvent;
import org.apache.zookeeper.server.*;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

import static org.junit.Assert.assertEquals;

public class BufferStatsTest {

    @Test
    public void testSetProposalSizeSetMinMax() {
        BufferStats stats = new BufferStats();
        assertEquals(-1, stats.getLastBufferSize());
        assertEquals(-1, stats.getMinBufferSize());
        assertEquals(-1, stats.getMaxBufferSize());
        stats.setLastBufferSize(10);
        assertEquals(10, stats.getLastBufferSize());
        assertEquals(10, stats.getMinBufferSize());
        assertEquals(10, stats.getMaxBufferSize());
        stats.setLastBufferSize(20);
        assertEquals(20, stats.getLastBufferSize());
        assertEquals(10, stats.getMinBufferSize());
        assertEquals(20, stats.getMaxBufferSize());
        stats.setLastBufferSize(5);
        assertEquals(5, stats.getLastBufferSize());
        assertEquals(5, stats.getMinBufferSize());
        assertEquals(20, stats.getMaxBufferSize());
    }

    @Test
    public void testReset() {
        BufferStats stats = new BufferStats();
        stats.setLastBufferSize(10);
        assertEquals(10, stats.getLastBufferSize());
        assertEquals(10, stats.getMinBufferSize());
        assertEquals(10, stats.getMaxBufferSize());
        stats.reset();
        assertEquals(-1, stats.getLastBufferSize());
        assertEquals(-1, stats.getMinBufferSize());
        assertEquals(-1, stats.getMaxBufferSize());
    }

    @Test
    public void testArchive() throws IOException {
        ReplyHeader h = new ReplyHeader(ClientCnxn.NOTIFICATION_XID, -1L, 0);
        WatchedEvent event = new WatchedEvent(Watcher.Event.EventType.NodeDataChanged, Watcher.Event.KeeperState.SyncConnected, "/test");
        WatcherEvent e = event.getWrapper();
        ByteBuffer[] notifications = serialize(h, e, "notification", null, null, ZooDefs.OpCode.error);
        ByteBuffer directBuffer = NIOServerCnxnFactory.getDirectBuffer();
        directBuffer.clear();
        for (ByteBuffer b : notifications) {
            if (directBuffer.remaining() < b.remaining()) {
                /*
                 * When we call put later, if the directBuffer is to
                 * small to hold everything, nothing will be copied,
                 * so we've got to slice the buffer if it's too big.
                 */
                b = (ByteBuffer) b.slice().limit(directBuffer.remaining());
            }
            /*
             * put() is going to modify the positions of both
             * buffers, put we don't want to change the position of
             * the source buffers (we'll do that after the send, if
             * needed), so we save and reset the position after the
             * copy
             */
            int p = b.position();
            directBuffer.put(b);
            b.position(p);
            if (directBuffer.remaining() == 0) {
                break;
            }
        }
        /*
         * Do the flip: limit becomes position, position gets set to
         * 0. This sets us up for the write.
         */
        directBuffer.flip();

        // =============================

        ByteBufferInputStream bbis = new ByteBufferInputStream(directBuffer);
        BinaryInputArchive bbia = BinaryInputArchive.getArchive(bbis);
        ReplyHeader replyHdr = new ReplyHeader();

        // info: 为什么这里指定了 header？ 实际上 server 端发送过来的数据里没有这个 tag
        // info：这个 header 没啥用，实现类的方法是空的，关键是在序列化时每个字段都写了 tag
        // info: 于是反序列化时可以根据相同的 tag 来反序列化当前对象的属性值
        replyHdr.deserialize(bbia, "header");

        System.out.println(replyHdr.toString());

        WatcherEvent we = new WatcherEvent();
        we.deserialize(bbia, "response");

        System.out.println(we.toString());

    }

    protected ByteBuffer[] serialize(ReplyHeader h, Record r, String tag,
                                     String cacheKey, Stat stat, int opCode) throws IOException {
        byte[] header = serializeRecord(h);
        byte[] data = null;
        if (r != null) {
            data = serializeRecord(r);
        }
        int dataLength = data == null ? 0 : data.length;
        int packetLength = header.length + dataLength;
        ByteBuffer lengthBuffer = ByteBuffer.allocate(4).putInt(packetLength);
        lengthBuffer.rewind();

        int bufferLen = 2;
        ByteBuffer[] buffers = new ByteBuffer[bufferLen];

        buffers[0] = ByteBuffer.wrap(header);;
        buffers[1] = ByteBuffer.wrap(data);
        return buffers;
    }
    protected byte[] serializeRecord(Record record) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream(ZooKeeperServer.intBufferStartingSizeBytes);
        BinaryOutputArchive bos = BinaryOutputArchive.getArchive(baos);
        // info: 为什么是 null ？
        bos.writeRecord(record, null);
        return baos.toByteArray();
    }



}
