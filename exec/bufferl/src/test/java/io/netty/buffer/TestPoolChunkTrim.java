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

	@Test
	public void test() {
		
		// Create an arena for testing
		DirectArena arena = new DirectArena(null, 1024, 10, 10, 1024*1024);
		PooledByteBufL<ByteBuffer> block;

		
		// Allocate a normal block
		block = arena.allocate(null, 513, 1024);
	    System.out.printf("Allocated a normal block\n %s",  arena.toString());
		block.deallocate();
		
		// Allocate a large block
		block = arena.allocate(null, 120*1024, 128*1024);
		System.out.printf("Just allocated a large block\n%s", arena.toString());
		
		// Trim the block to a smaller size
		block.trim(600);
		System.out.printf("Just resized to a smaller block\n%s", arena.toString());
		
		// Free the block
		block.deallocate();
		System.out.printf("Freed the block: \n%s", arena.toString());
		
		
		// Allocate and free a subpage block
		block = arena.allocate(null, 511, 512);
		System.out.printf("Just allocated a subpage block\n%s", arena.toString());
		block.deallocate();
		
		// Allocate and free a small block
		block = arena.allocate(null, 256, 256);
		System.out.printf("Just allocated a subpage block\n%s", arena.toString());
		block.deallocate();
	}

}
