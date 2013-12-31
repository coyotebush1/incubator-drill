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


/** A PoolChunk is a large piece of memory used by the Pool/Arena allocator. 
 * The chunk is subdivided into pages organized as a binary tree for Buddy System allocation,
 * and some pages are divided into arrays of fixed size subpages for subpage allocation.
 *
 * @param <T> the type of memory buffer to be allocated to user
 */
final class PoolChunkL<T> {
    private static final int ST_UNUSED = 0;
    private static final int ST_BRANCH = 1;
    private static final int ST_ALLOCATED = 2;
    private static final int ST_ALLOCATED_SUBPAGE = ST_ALLOCATED | 1;

    private static final long multiplier = 0x5DEECE66DL;
    private static final long addend = 0xBL;
    private static final long mask = (1L << 48) - 1;

    final PoolArenaL<T> arena;
    final T memory;
    final boolean unpooled;

    private final int[] memoryMap;
    private final PoolSubpageL<T>[] subpages;
    /** Used to determine if the requested capacity is equal to or greater than pageSize. */
    private final int subpageOverflowMask;
    private final int pageSize;
    private final int pageShifts;

    private final int chunkSize;
    private final int maxSubpageAllocs;

    private long random = (System.nanoTime() ^ multiplier) & mask;

    private int freeBytes;

    PoolChunkListL<T> parent;
    PoolChunkL<T> prev;
    PoolChunkL<T> next;

    // TODO: Test if adding padding helps under contention
    //private long pad0, pad1, pad2, pad3, pad4, pad5, pad6, pad7;

    /** Create a new "chunk" of memory to be used in the given arena.
     *
     * @param arena - the arena this chunk belongs to
     * @param memory
     * @param pageSize - the size of a page (known to arena, why here?)
     * @param maxOrder - how many pages to a chunk  (2^^maxOrder pages)
     * @param pageShifts - page size as number of shifts  (2^^pageShifts)
     * @param chunkSize - the size of a chunk   (pagesize*2^^maxOrder(known to arena)
     */
    PoolChunkL(PoolArenaL<T> arena, T memory, int pageSize, int maxOrder, int pageShifts, int chunkSize) {
        unpooled = false;
        this.arena = arena;
        this.memory = memory;
        this.pageSize = pageSize;
        this.pageShifts = pageShifts;
        this.chunkSize = chunkSize;
        subpageOverflowMask = ~(pageSize - 1);
        freeBytes = chunkSize;

        int chunkSizeInPages = chunkSize >>> pageShifts;
        maxSubpageAllocs = 1 << maxOrder;

        // Generate the memory map.
        memoryMap = new int[maxSubpageAllocs << 1];
        int memoryMapIndex = 1;
        for (int i = 0; i <= maxOrder; i ++) {
            int runSizeInPages = chunkSizeInPages >>> i;
            for (int j = 0; j < chunkSizeInPages; j += runSizeInPages) {
                //noinspection PointlessBitwiseExpression
                memoryMap[memoryMapIndex ++] = j << 17 | runSizeInPages << 2 | ST_UNUSED;
            }
        }

        subpages = newSubpageArray(maxSubpageAllocs);
    }

    /** Creates a special chunk that is not pooled. */
    PoolChunkL(PoolArenaL<T> arena, T memory, int size) {
        unpooled = true;
        this.arena = arena;
        this.memory = memory;
        memoryMap = null;
        subpages = null;
        subpageOverflowMask = 0;
        pageSize = 0;
        pageShifts = 0;
        chunkSize = size;
        maxSubpageAllocs = 0;
    }

    @SuppressWarnings("unchecked")
    private PoolSubpageL<T>[] newSubpageArray(int size) {
        return new PoolSubpageL[size];
    }

    
    /** returns the percentage of the chunk which has been allocated */
    int usage() {
        if (freeBytes == 0) {
            return 100;
        }

        int freePercentage = (int) (freeBytes * 100L / chunkSize);
        if (freePercentage == 0) {
            return 99;
        }
        return 100 - freePercentage;
    }

    
    /** 
     * Allocates a buffer of the given size from current chunk
     * @param normCapacity - max capacity of the buffer
     * @return - handle to the buffer, -1 if failed
     */
    long allocate(int normCapacity) {
    	
    	// If requested size is >= one page, then allocate a run of pages
        int firstVal = memoryMap[1];
        if ((normCapacity & subpageOverflowMask) != 0) { // >= pageSize
            return allocateRun(normCapacity, 1, firstVal);
            
        // otherwise, allocate a subpage    
        } else {
            return allocateSubpage(normCapacity, 1, firstVal);
        }
    }

    
    /**
     * Allocate a run of pages, starting at current location in tree  (1=root)
     * 
     * @param normCapacity - requested capacity adjusted to what will actually be allocated.
     * @param curIdx - the starting node in the buddy tree
     * @param val - the contents of the starting node
     * @return a handle to the allocated run of pages
     */
    private long allocateRun(int normCapacity, int curIdx, int val) {
        for (;;) {
            if ((val & ST_ALLOCATED) != 0) { // state == ST_ALLOCATED || state == ST_ALLOCATED_SUBPAGE
                return -1;
            }

            if ((val & ST_BRANCH) != 0) { // state == ST_BRANCH
                int nextIdx = curIdx << 1 ^ nextRandom();
                long res = allocateRun(normCapacity, nextIdx, memoryMap[nextIdx]);
                if (res > 0) {
                    return res;
                }

                curIdx = nextIdx ^ 1;
                val = memoryMap[curIdx];
                continue;
            }

            // state == ST_UNUSED
            return allocateRunSimple(normCapacity, curIdx, val);
        }
    }

    /**
     * Allocate a run of pages from the current subtree, where the subtree is UNUSED.
     * Takes advantage optimizations for unused case.
     * @param normCapacity - the desired size, rounded up to what will be actually allocated
     * @param curIdx - the memory map index we're starting from
     * @param val - the memory map value corresponding to the index
     * @return - a handle to the the new allocation, -1 if not possible.
     */
    private long allocateRunSimple(int normCapacity, int curIdx, int val) {
    	
    	// Verify our subtree can hold the desired capacity.
        int runLength = runLength(val);
        if (normCapacity > runLength) {
            return -1;
        }

        // Work down the tree until we have the desired node size
        for (;;) {
            if (normCapacity == runLength) {
                // Found the run that fits.
                // Note that capacity has been normalized already, so we don't need to deal with
                // the values that are not power of 2.
                memoryMap[curIdx] = val & ~3 | ST_ALLOCATED;
                freeBytes -= runLength;
                return curIdx;
            }

            // Pick a child at random. The run will come from the child.
            int nextIdx = curIdx << 1 ^ nextRandom();
            int unusedIdx = nextIdx ^ 1;

            // We are the parent of the child, so we are now a BRANCH
            memoryMap[curIdx] = val & ~3 | ST_BRANCH;
            
            // The sibling of the child remains unused 
            memoryMap[unusedIdx] = memoryMap[unusedIdx] & ~3 | ST_UNUSED;   // TODO: Already UNUSED?

            // End "Work down the tree until ..."
            runLength >>>= 1;
            curIdx = nextIdx;
            val = memoryMap[curIdx];
        }
    }

    
    
    
    /**
     * Allocate a new page to be used to store subpage buffers.
     *   Note: not sure why it doesn't call allocateRun to do the page allocation.
     * @param normCapacity - the actual size of the buffer we will allocate
     * @param curIdxn - the node where our search stargs
     * @param val - contents of the current node
     * @return a handle to the allocated buffer.
     */
    private long allocateSubpage(int normCapacity, int curIdx, int val) {
        int state = val & 3;
        if (state == ST_BRANCH) {
            int nextIdx = curIdx << 1 ^ nextRandom();
            long res = branchSubpage(normCapacity, nextIdx);
            if (res > 0) {
                return res;
            }

            return branchSubpage(normCapacity, nextIdx ^ 1);
        }

        if (state == ST_UNUSED) {
            return allocateSubpageSimple(normCapacity, curIdx, val);
        }

        if (state == ST_ALLOCATED_SUBPAGE) {
            PoolSubpageL<T> subpage = subpages[subpageIdx(curIdx)];
            int elemSize = subpage.elemSize;
            if (normCapacity != elemSize) {
                return -1;
            }

            return subpage.allocate();
        }

        return -1;
    }

    
    
    /**
     * Allocate a page to be used for subpage buffers, knowing the subtree is UNUSED
     * @param normCapacity
     * @param curIdx
     * @param val
     * @return
     */
    private long allocateSubpageSimple(int normCapacity, int curIdx, int val) {
        int runLength = runLength(val);
        for (;;) {
            if (runLength == pageSize) {
                memoryMap[curIdx] = val & ~3 | ST_ALLOCATED_SUBPAGE;
                freeBytes -= runLength;

                int subpageIdx = subpageIdx(curIdx);
                PoolSubpageL<T> subpage = subpages[subpageIdx];
                if (subpage == null) {
                    subpage = new PoolSubpageL<T>(this, curIdx, runOffset(val), pageSize, normCapacity);
                    subpages[subpageIdx] = subpage;
                } else {
                    subpage.init(normCapacity);
                }
                return subpage.allocate();
            }

            int nextIdx = curIdx << 1 ^ nextRandom();
            int unusedIdx = nextIdx ^ 1;

            memoryMap[curIdx] = val & ~3 | ST_BRANCH;
            //noinspection PointlessBitwiseExpression
            memoryMap[unusedIdx] = memoryMap[unusedIdx] & ~3 | ST_UNUSED;

            runLength >>>= 1;
            curIdx = nextIdx;
            val = memoryMap[curIdx];
        }
    }

    private long branchSubpage(int normCapacity, int nextIdx) {
        int nextVal = memoryMap[nextIdx];
        if ((nextVal & 3) != ST_ALLOCATED) {
            return allocateSubpage(normCapacity, nextIdx, nextVal);
        }
        return -1;
    }
    
    /**
     * Reduce the size of a buffer to the desired size, freeing up excess memory if possible.
     * @param handle
     * @param smallerSize
     * @return a new handle to the smaller buffer, or -1 if can't resize
     */
    long trim(long handle, int smallerSize) {
    	int memoryMapIdx = (int) handle;
    	int bitmapIx = (int)(handle >>> 32);
    	int originalRunLength = runLength(memoryMap[memoryMapIdx]);
    	
    	// If the buffer was a HUGE allocation, then we leave it alone
    	if (this.unpooled || memoryMapIdx == 0 || handle < 0) {
    		return -1;
    	}
    	
    	// If the buffer was a subpage, then we also leave it alone.
    	if (bitmapIx != 0 || (memoryMap[memoryMapIdx] & 3) == ST_ALLOCATED_SUBPAGE) {
    		return -1;
    	}
 
    	// We can't trim if the result will become a subpage
    	if (smallerSize <= pageSize/2) {
    		return -1;
    	}
    	
    	// Starting at current node, follow left hand children, until reaching node of desired size.
    	//   Note that runLength and memoryMapIdx move in unison. 
    	int runLength;
    	for (runLength = originalRunLength;  smallerSize*2 <= runLength;  runLength /= 2, memoryMapIdx = memoryMapIdx<<1) {
    		
    		// Current node is a parent of the desired node. It now becomes a "BRANCH".
    		memoryMap[memoryMapIdx] = (memoryMap[memoryMapIdx] & ~3) | ST_BRANCH;
    		
    		// Right hand child is now an unused buddy. Mark it "UNUSED".
    		//  (done - should already be marked "UNUSED")
    	}
    	
    	// We are now at the desired node. Mark it allocated.
    	memoryMap[memoryMapIdx] = (memoryMap[memoryMapIdx] & ~3) | ST_ALLOCATED;
    	freeBytes += (originalRunLength - runLength);
    	
    	// return new handle to the reduced size buffer.
    	return memoryMapIdx;
    }

    /**
     * Return a buffer back to the memory pool.
     * @param handle
     */
    void free(long handle) {
        int memoryMapIdx = (int) handle;
        int bitmapIdx = (int) (handle >>> 32);

        // If buffer was allocated from a subpage, then free it.
        int val = memoryMap[memoryMapIdx];
        int state = val & 3;
        if (state == ST_ALLOCATED_SUBPAGE) {
            assert bitmapIdx != 0;
            PoolSubpageL<T> subpage = subpages[subpageIdx(memoryMapIdx)];
            assert subpage != null && subpage.doNotDestroy;
            if (subpage.free(bitmapIdx & 0x3FFFFFFF)) {
                return;
            }
            
        // Otherwise, it should have been allocated from the chunk    
        } else {
            assert state == ST_ALLOCATED : "state: " + state;
            assert bitmapIdx == 0;
        }

        // Update the nr of bytes free
        freeBytes += runLength(val);

        // start at current node and work up the tree
        for (;;) {
        	
            // Mark the node as "unused"
            memoryMap[memoryMapIdx] = val & ~3 | ST_UNUSED;
            
            // If at top of tree, done
            if (memoryMapIdx == 1) {
                assert freeBytes == chunkSize;
                return;
            }

            // If the buddy is allocated, we can stop since no more merging can occur
            if ((memoryMap[siblingIdx(memoryMapIdx)] & 3) != ST_UNUSED) {
                break;
            }

            // move to current node's parent, effectively merging with UNUSED buddy
            memoryMapIdx = parentIdx(memoryMapIdx);
            val = memoryMap[memoryMapIdx];
        }
    }

    
    
    void initBuf(PooledByteBufL<T> buf, long handle, int reqCapacity) {
        int memoryMapIdx = (int) handle;
        int bitmapIdx = (int) (handle >>> 32);
        if (bitmapIdx == 0) {
            int val = memoryMap[memoryMapIdx];
            assert (val & 3) == ST_ALLOCATED : String.valueOf(val & 3);
            buf.init(this, handle, runOffset(val), reqCapacity, runLength(val));
        } else {
            initBufWithSubpage(buf, handle, bitmapIdx, reqCapacity);
        }
    }

    void initBufWithSubpage(PooledByteBufL<T> buf, long handle, int reqCapacity) {
        initBufWithSubpage(buf, handle, (int) (handle >>> 32), reqCapacity);
    }

    private void initBufWithSubpage(PooledByteBufL<T> buf, long handle, int bitmapIdx, int reqCapacity) {
        assert bitmapIdx != 0;

        int memoryMapIdx = (int) handle;
        int val = memoryMap[memoryMapIdx];
        assert (val & 3) == ST_ALLOCATED_SUBPAGE;

        PoolSubpageL<T> subpage = subpages[subpageIdx(memoryMapIdx)];
        assert subpage.doNotDestroy;
        assert reqCapacity <= subpage.elemSize;

        buf.init(
                this, handle,
                runOffset(val) + (bitmapIdx & 0x3FFFFFFF) * subpage.elemSize, reqCapacity, subpage.elemSize);
    }

    private static int parentIdx(int memoryMapIdx) {
        return memoryMapIdx >>> 1;
    }

    private static int siblingIdx(int memoryMapIdx) {
        return memoryMapIdx ^ 1;
    }

    private int runLength(int val) {
        return (val >>> 2 & 0x7FFF) << pageShifts;
    }

    private int runOffset(int val) {
        return val >>> 17 << pageShifts;
    }

    private int subpageIdx(int memoryMapIdx) {
        return memoryMapIdx - maxSubpageAllocs;
    }

    private int nextRandom() {
        random = random * multiplier + addend & mask;
        return (int) (random >>> 47) & 1;
    }

    public String toString() {
        StringBuilder buf = new StringBuilder();
        buf.append("Chunk(");
        buf.append(Integer.toHexString(System.identityHashCode(this)));
        buf.append(": ");
        buf.append(usage());
        buf.append("%, ");
        buf.append(chunkSize - freeBytes);
        buf.append('/');
        buf.append(chunkSize);
        buf.append(')');
        return buf.toString();
    }
}
