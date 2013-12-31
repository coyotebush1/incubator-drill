/**
 * 
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
		
		// Allocate and free a subpage block
		block = arena.allocate(null, 511, 512);
		System.out.printf("Just allocated a subpage block\n%s", arena.toString());
		block.deallocate();
		
		// Allocate and free a small block
		block = arena.allocate(null, 256, 256);
		System.out.printf("Just allocated a subpage block\n%s", arena.toString());
		block.deallocate();
		
		// Allocate a normal block
		block = arena.allocate(null, 600, 1024);
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
	}

}
