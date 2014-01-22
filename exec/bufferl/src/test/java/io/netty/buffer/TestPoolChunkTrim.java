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

import java.nio.ByteBuffer;

import org.junit.Assert;
import org.junit.Test;

/**
 * @author jmorris
 *
 */
public class TestPoolChunkTrim {
	
    /** Points to the default allocator */
	static PooledByteBufAllocatorL allocator = PooledByteBufAllocatorL.DEFAULT;
	
    /** Helper to allocate a buffer with capacity between min and max */
	static ByteBuf allocate(int min, int max) {
		return allocator.newDirectBuffer(min, max);
	}
	
    /** Helper to allocate a buffer with fixed size capacity */
	static ByteBuf allocate(int size) {
		return allocate(size, size);
	}
	


	/**
	 * Unit test the memory allocator and trim() function.
	 * The results are confirmed by examining the state of the memory allocator.
	 * In this test, we are working with a single chunk and a 
	 * small set of subpage allocations.
	 * 
	 * Chunk status can be verified by matching with 
	 *      "Chunk ... <bytes consumed>/".
	 * The subpage allocators are verified by matching with
	 *      "<nr allocated>/ ... elemSize: <size>"
	 *      	 *   
	 *   A cleaner approach would create new methods to query
	 *      - how many pages (total) have been allocated, and
	 *      - how many elements of a particular size have been allocated.   
	 */
	@Test
	public void test() {
		
		int pageSize=8192; // default
		
		ByteBuf block;

		// Allocate and free a single page
		block = allocate(pageSize/2+1);
	    assertMatch("Chunk.* 8192/");   // Verify a single page allocated.
		block.release();
		assertMatch("Chunk.* 0/");      // Verify the single page freed, and chunk still exists.
		
		// Allocate and free the tiniest tiny subpage
		block = allocate(1);
		assertMatch("1/.*elemSize: 16"); // Verify one element of size 16 has been allocated.
		block.release();
		assertMatch("0/.*elemSize: 16"); // Verify element has been freed, but page is still in pool.
		
		// Allocate and free the largest tiny subpage
		block = allocate(512-16-15);
		assertMatch("1/.*elemSize: 496");
		block.release();
		assertMatch("0/.*elemSize: 496");
		
		// Allocate and free smallest small subpage
		block = allocate(512-15);
		assertMatch("1/.*elemSize: 512");
		block.release();
		assertMatch("0/.*elemSize: 512");
		
		// Allocate and free largest small subpage
		block = allocate(pageSize/2-1);
		assertMatch("1/.*elemSize: 4096");
		block.release();
		assertMatch("0/.*elemSize: 4096");

		// Allocate a large block and trim to a single page
		block = allocate(25*pageSize, 256*pageSize);
		Assert.assertTrue(25*pageSize <= block.capacity() && block.capacity() <= 256*pageSize);
		block.capacity(pageSize/2+1);
		Assert.assertTrue(block.capacity() == pageSize/2+1);
		assertMatch("Chunk.* 40960/");
		
		// Trim the single page to a tiny size.
		block.capacity(31);
		assertMatch("1/.* elemSize: 32"); assertMatch("Chunk.* 40960/");
		
		// Resize the tiny block to two pages
		block = block.capacity(pageSize+1);
		Assert.assertTrue(block.capacity() == pageSize+1);
		assertMatch("0/.* elemSize: 32"); assertMatch("Chunk.* 57344/");
	
		// Resize two pages to four pages. Should throw exception after copying more than one page.
		try {
		    block.capacity(4*pageSize);
		    Assert.fail("ERROR: should have thrown exception\n");
		} catch (TooMuchCopyingException e) {}
		assertMatch("Chunk.* 73728/");
		
		// Drop the four pages. At this point, we have 5 pages consumed for subpage buffers.
		block.release();
		assertMatch("Chunk.* 40960/");
		
		System.out.printf("All memory has been released:\n%s\n", toString());
	}
	
	
	
	/** 
	 * Verify our current state matches the pattern. 
	 * 
	 * Note: Uses the existing "toString()" method and extracts information
	 *   by matching a pattern to one of the output lines.
	 */
    void assertMatch(String pattern) {
		
        // Get our current state as a string
		String s = toString();
	
		// Do for each line in the string
		for (int f=0, l=s.indexOf('\n',f); l != -1; f=l+1, l=s.indexOf('\n',f)) {
			
			// if the line contains pattern, then success.
			if (s.substring(f,l).matches(".*"+pattern+".*")) return;
		}
		
		// We didn't find a matching line, so fail the test
		Assert.fail("Test failed to match pattern " + pattern);
	}

	
	/** Display our thread's arena. The other arenas are not impacted by this test. */
	public String toString() {
		return allocator.threadCache.get().directArena.toString();
	}

	
}
