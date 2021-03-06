/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.drill.exec.server;

import static org.junit.Assert.assertTrue;
import io.netty.buffer.ByteBuf;

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import mockit.Injectable;

import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.common.expression.ExpressionPosition;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.common.types.Types;
import org.apache.drill.exec.expr.TypeHelper;
import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.proto.BitData.FragmentRecordBatch;
import org.apache.drill.exec.proto.BitData.RpcType;
import org.apache.drill.exec.proto.CoordinationProtos.DrillbitEndpoint;
import org.apache.drill.exec.proto.ExecProtos.FragmentHandle;
import org.apache.drill.exec.proto.GeneralRPCProtos.Ack;
import org.apache.drill.exec.proto.UserBitShared.QueryId;
import org.apache.drill.exec.record.FragmentWritableBatch;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.record.WritableBatch;
import org.apache.drill.exec.rpc.Acks;
import org.apache.drill.exec.rpc.RemoteConnection;
import org.apache.drill.exec.rpc.Response;
import org.apache.drill.exec.rpc.RpcException;
import org.apache.drill.exec.rpc.RpcOutcomeListener;
import org.apache.drill.exec.rpc.control.WorkEventBus;
import org.apache.drill.exec.rpc.data.DataConnectionManager;
import org.apache.drill.exec.rpc.data.DataResponseHandler;
import org.apache.drill.exec.rpc.data.DataServer;
import org.apache.drill.exec.rpc.data.DataTunnel;
import org.apache.drill.exec.vector.Float8Vector;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.drill.exec.work.WorkManager.WorkerBee;
import org.apache.drill.exec.work.fragment.FragmentManager;
import org.junit.Test;

import com.google.common.base.Stopwatch;
import com.google.common.collect.Lists;

public class TestBitRpc {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(TestBitRpc.class);

  @Test
  public void testConnectionBackpressure(@Injectable WorkerBee bee, @Injectable WorkEventBus workBus) throws Exception {
    int port = 1234;
    BootStrapContext c = new BootStrapContext(DrillConfig.create());
    BootStrapContext c2 = new BootStrapContext(DrillConfig.create());

    DataResponseHandler drp = new BitComTestHandler();
    DataServer server = new DataServer(c, workBus, drp);

    port = server.bind(port);
    DrillbitEndpoint ep = DrillbitEndpoint.newBuilder().setAddress("localhost").setDataPort(port).build();
    DataConnectionManager manager = new DataConnectionManager(FragmentHandle.getDefaultInstance(), ep, c2);
    DataTunnel tunnel = new DataTunnel(manager);
    AtomicLong max = new AtomicLong(0);
    for (int i = 0; i < 40; i++) {
      long t1 = System.currentTimeMillis();
      tunnel.sendRecordBatch(new TimingOutcome(max), new FragmentWritableBatch(false, QueryId.getDefaultInstance(), 1,
          1, 1, 1, getRandomBatch(c.getAllocator(), 5000)));
      System.out.println(System.currentTimeMillis() - t1);
      // System.out.println("sent.");
    }
    System.out.println(String.format("Max time: %d", max.get()));
    assertTrue(max.get() > 2700);
    Thread.sleep(5000);
  }

  private static WritableBatch getRandomBatch(BufferAllocator allocator, int records) {
    List<ValueVector> vectors = Lists.newArrayList();
    for (int i = 0; i < 5; i++) {
      Float8Vector v = (Float8Vector) TypeHelper.getNewVector(
          MaterializedField.create(new SchemaPath("a", ExpressionPosition.UNKNOWN), Types.required(MinorType.FLOAT8)),
          allocator);
      v.allocateNew(records);
      v.getMutator().generateTestData();
      v.getMutator().setValueCount(records);
      vectors.add(v);
    }
    return WritableBatch.getBatchNoHV(records, vectors, false);
  }

  private class TimingOutcome implements RpcOutcomeListener<Ack> {
    private AtomicLong max;
    private Stopwatch watch = new Stopwatch().start();

    public TimingOutcome(AtomicLong max) {
      super();
      this.max = max;
    }

    @Override
    public void failed(RpcException ex) {
      ex.printStackTrace();
    }

    @Override
    public void success(Ack value, ByteBuf buffer) {
      long micros = watch.elapsed(TimeUnit.MILLISECONDS);
      System.out.println(String.format("Total time to send: %d, start time %d", micros, System.currentTimeMillis() - micros));
      while (true) {
        long nowMax = max.get();
        if (nowMax < micros) {
          if (max.compareAndSet(nowMax, micros))
            break;
        } else {
          break;
        }
      }
    }

  }

  private class BitComTestHandler implements DataResponseHandler {

    int v = 0;

    @Override
    public Response handle(RemoteConnection connection, FragmentManager manager, FragmentRecordBatch fragmentBatch, ByteBuf data)
        throws RpcException {
      // System.out.println("Received.");
      try {
        v++;
        if (v % 10 == 0) {
          System.out.println("sleeping.");
          Thread.sleep(3000);
        }
      } catch (InterruptedException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
      return new Response(RpcType.ACK, Acks.OK);
    }

  }
}
