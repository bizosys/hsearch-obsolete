package com.bizosys.hsearch.filter;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class MergedBlocks {

	public static Block add(int pos, byte[] data) {
		return add(null, pos, data);
	}

	/**
	 * Add a new element to the block
	 * @param existing
	 * @param pos
	 * @param data
	 * @return Block with added record
	 */
	public static Block add(Block existing, int pos, byte[] data) {
		if ( pos <= 0 ) throw new ArrayIndexOutOfBoundsException("Starts from 1, Received" + pos);
		
		int headerLocation = pos*4;
		byte[] header = null;
		int dataPos = 0;
		if ( null == existing || null == existing.header) {
			header = new byte[headerLocation+4];
			Arrays.fill(header,(byte)-1);			
		} else if (existing.header.length < headerLocation) {
			header  = new byte[headerLocation];
			Arrays.fill(header,(byte)-1);
			System.arraycopy(existing.header, 0, header, 0, existing.header.length);
			dataPos = existing.data.length ;
		} else {
			header = existing.header;
			if ( null != existing.data ) dataPos = existing.data.length ;
		}
		
		writeHeader(header, pos, dataPos);
		
		byte[] newData = null;
		if ( dataPos != 0 ) {
			newData  = new byte[dataPos + data.length];
			System.arraycopy(existing.data, 0, newData, 0, existing.data.length);
			System.arraycopy(data, 0, newData, existing.data.length, data.length);
		} else {
			newData  = data;
		}
		return new Block(header, newData);
	}
	
	/**
	 * Mass addition. This avoids unnecessary array copy.
	 * @param records	Records
	 * @param headerSpace	We passed a header byte array space, which can be reused.
	 * @return Block with added record
	 */
	public static Block add(Map<Integer, byte[]> records, byte[] headerSpace) {
		//Total Bytes data needed
		int dataLen = 0;
		for (byte[] de : records.values()) {
			dataLen = dataLen + de.length;
		}
		
		Block newBlock = new Block(headerSpace, new byte[dataLen]);
		int dataPos = 0;
		int maxDocPos = headerSpace.length / 4;
		for ( int i=1; i<= maxDocPos; i++) {
			if ( !records.containsKey(i)) continue;
			writeHeader(newBlock.header, i, dataPos);
			byte[] data = records.get(i);
			System.arraycopy(data, 0, newBlock.data, dataPos, data.length);
			dataPos = dataPos + data.length;
		}
		return newBlock;
	}
	

	/**
	 * Update an existing entry
	 * @param block
	 * @param blank
	 * @param updated
	 * @param docPos
	 * @return Block with updated record
	 */
	public static Block update(Block block, IStorable blank, IStorable updated, int docPos) {
		
		byte[] modifiedB =  updated.toBytes();
		return update(block,blank,modifiedB,docPos);
	}
	
	public static Block update(Block block, IStorable blank, byte[] modifiedB, int docPos) {
		
		IStorable existing = get(blank, block, docPos);
		//TODO :: Check this Logic of returning null
		if ( null == existing) return null;
		
		byte[] existingB =  existing.toBytes();
		int modifiedLen = modifiedB.length;
		int existingLen = existingB.length;
		int blockLen = block.data.length;
		
		if ( modifiedLen > existingLen) { //Delete and append in End
			byte[] newBlocks  = new byte[blockLen + modifiedLen];
			System.arraycopy(block.data, 0, newBlocks, 0, blockLen);
			System.arraycopy(modifiedB, 0, newBlocks, blockLen, modifiedLen);
			writeHeader(block.header, docPos, blockLen);
			block.data = newBlocks;
		} else { //Overwrite
			System.arraycopy(modifiedB, 0, block.data, 
				readHeader(block.header, docPos), modifiedLen);
		}
		
		return block;
	}	
	
	/**
	 * Delete a specific data zone.
	 * @param existing
	 * @param docPos
	 * @return Block with deleted record
	 */
	public static Block delete(Block existing, int docPos) {
		writeHeader(existing.header, docPos, -1);
		return existing;
	}
	
	public static void delete(byte[] header, int docPos) {
		writeHeader(header, docPos, -1);
	}	
	
	/**
	 * Get a record from the specified position of the block
	 * @param storable
	 * @param block
	 * @param docPos
	 * @return IStorable Object with storable interface 
	 */
	public static IStorable get(IStorable storable, Block block, int docPos) {
		int dataLoc = readHeader(block.header, docPos);
		if ( -1 == dataLoc) return null; 
		storable.fromBytes(block.data, dataLoc);
		return storable;
	}
	
	/**
	 *	Compact the existing blocks 
	 * @param block
	 * @param storable
	 * @return Block Compacted format
	 */
	public static Block compact(Block block, IStorable storable) {
		Map<Integer, byte[]> deList = fromBytes(block, storable);
		block.data = null;
		byte[] headerB = block.header;
		return add(deList, headerB);
	}
	
	public static Block merge(Block existingBlock, 
		Map<Integer, byte[]> addedList, IStorable storable) {
		
		int existingMax = ( null == existingBlock.header ) ?
			0 : (existingBlock.header.length / 4);
		
		int newMax = Integer.MIN_VALUE;
		for (Integer pos : addedList.keySet()) {
			if ( pos > newMax ) newMax = pos; 
		}
		
		if ( existingMax < newMax) { //Header Expansion
			byte[] header = new byte[newMax*4];
			if ( 0 != existingMax) {
				System.arraycopy(existingBlock.header, 0, 
						header, 0, existingBlock.header.length);
			}
			existingBlock.header = header;			
		}
		
		int totalBytes = 0;
		
		for (Integer docPos : addedList.keySet()) {
			byte[] data = addedList.get(docPos);
			if ( docPos < existingMax) {
				existingBlock = update(existingBlock,storable, data, docPos);
				continue;
			} else {
				totalBytes = totalBytes + data.length;
			}
		}
		
		int dataPos = (null == existingBlock) ? 0 : 
			( null == existingBlock.data) ? 0 : existingBlock.data.length;
		
		if ( totalBytes > 0 ) {
			byte[] newB = new byte[dataPos + totalBytes];
			Arrays.fill(newB,(byte)-1);
			if ( dataPos > 0 ) System.arraycopy(
				existingBlock.data, 0, newB, 0, dataPos);
			existingBlock.data = newB;
		}
		byte[] header = ( null == existingBlock ) ? new byte[0] : existingBlock.header;
		byte[] data = ( null == existingBlock ) ? new byte[0] :existingBlock.data;
		for (Integer docPos : addedList.keySet()) {
			if ( docPos < existingMax) continue;
			byte[] objB = addedList.get(docPos);
			if ( null == objB) {
				writeHeader(header, docPos, -1);
				continue;
			}
			writeHeader(header, docPos, dataPos);
			System.arraycopy(objB, 0, data, dataPos, objB.length);
			dataPos = dataPos + objB.length;
		}
		return existingBlock;
	}
	
	public static int getTotalDocuments(Block block) {
		if ( null == block.header) return 0;
		else return block.header.length / 4;
	}
	

	public static Map<Integer, byte[]> fromBytes(Block block, IStorable storable) {
		int totalDocs = block.header.length / 4;

		Map<Integer, byte[]> deList = new HashMap<Integer, byte[]>();
		for ( int i=1; i<= totalDocs; i++) {
			int dataLoc = readHeader(block.header, i);
			if ( -1 == dataLoc) continue;
			IStorable de = get(storable, block, i);
			if ( null == de) continue;
			deList.put(i, de.toBytes());
		}
		return deList;
	}


	public static void writeHeader(byte[] header, int docPos, int dataPos) {
		int headerLoc = docPos * 4;
		if ( headerLoc > header.length) {
			System.err.println (
				"Warning > MergedBlocks:ArrayIndexOutOfBoundsException: Header Size:" + header.length + " , docPos:" + docPos + 
				" , dataPos:" + dataPos);
			return;
		}
		header[headerLoc-4] = (byte)(dataPos >> 24); 
		header[headerLoc-3] =   (byte)(dataPos >> 16 );
		header[headerLoc-2] = (byte)(dataPos >> 8 );
		header[headerLoc-1] = (byte)(dataPos);
	}
	
	public static int readHeader(byte[] header, int docPos) {
		int headerLoc = docPos * 4;
		if ( headerLoc > header.length ) return -1;
		return Storable.getInt(headerLoc-4, header);
	}
	

	public static class Block {
		public byte[] header;
		public byte[] data;
		
		public Block() {
		}

		public Block(byte[] header, byte[] data) {
			this.header = header;
			this.data = data;
		}
	}

}
