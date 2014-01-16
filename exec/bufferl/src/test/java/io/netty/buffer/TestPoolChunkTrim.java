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
package io.netty.buffer;

import static org.junit.Assert.*;

import java.nio.ByteBuffer;

import io.netty.buffer.PoolArenaL.DirectArena;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * @author jmorris
 *
 */
public class TestPoolChunkTrim {
	
	static PooledByteBufAllocatorL arena = PooledByteBufAllocatorL.DEFAULT;
	
	static ByteBuf allocate(int min, int max) {
		return arena.newDirectBuffer(min, max);
	}
	
	
	/** Display our thread's arena. The other arenas are not impacted by this test. */
	public String toString() {
		return arena.threadCache.get().directArena.toString();
	}

	

	@Test
	public void test() {
		
		int pagesize=8192; // default
		
		ByteBuf block;

		// Allocate the smallest normal block
		block = allocate(pagesize/2+1, pagesize);
	    System.out.printf("Allocated a normal block:  size=%d\n %s",  block.capacity(), toString());
		block.release();
		

		
		// Allocate and free the tiniest block
		block = allocate(1, 1);
		System.out.printf("Just allocated smallest tiny block size=%d\n%s", block.capacity(), toString());
		block.release();
		
		// Allocate and free a tiny block
		block = allocate(255, 255);
		System.out.printf("Just allocated a tiny block size=%d\n%s", block.capacity(), toString());
		block.release();
		
		// Allocate and free a small block
		block = allocate(pagesize/2-1, pagesize/2-1);
		System.out.printf("Just allocated largest small block size=%d\n%s", block.capacity(), toString());
		block.release();

		// Allocate a large block and trim to a single page
		block = allocate(25*pagesize, 256*pagesize);
		block = block.capacity(pagesize/2+1);
		System.out.printf("Just resized to a single page: size=%d\n%s", block.capacity(), toString());
		
		// Trim the block to a tiny size.
		block = block.capacity(31);
		System.out.printf("Just resized to tiny block: size=%d\n%s", block.capacity(), toString());
		
		// Resize the tiny block to two pages
		block = block.capacity(pagesize+1);
		System.out.printf("Just resized to normal allocation: size=%d\n%s", block.capacity(), toString());
		block.release();
	}

}
