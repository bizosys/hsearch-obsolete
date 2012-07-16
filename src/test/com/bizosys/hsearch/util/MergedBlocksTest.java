package com.bizosys.hsearch.util;

import junit.framework.TestCase;
import junit.framework.TestFerrari;

import com.bizosys.hsearch.filter.Access;
import com.bizosys.hsearch.filter.MergedBlocks;
import com.bizosys.hsearch.filter.Storable;
import com.bizosys.hsearch.index.DocAcl;
import com.bizosys.hsearch.index.DocMeta;
import com.bizosys.hsearch.index.DocTeaser;

public class MergedBlocksTest extends TestCase {

	public static void main(String[] args) throws Exception {
		MergedBlocksTest t = new MergedBlocksTest();
        TestFerrari.testRandom(t);
	}
	
	public void testAdd(Short position) throws Exception{
		if ( position < 0) position = (short) (position + Short.MIN_VALUE);
		if ( position == 0 ) return;
		DocMeta meta = new DocMeta();
		meta.docType = "Abinash";
		MergedBlocks.Block block = MergedBlocks.add(position, meta.toBytes());
		
		DocMeta deMeta = new DocMeta();
		MergedBlocks.get(deMeta, block, position);
		assertEquals(meta.docType, deMeta.docType);
	}
	
	public void testAdd(Short position1, Short position2, Short position3, Short position4) throws Exception{
		int[] positions = new int[]{position1,position2,position3,position4};
		
		for (int i=0; i<4; i++) {
			positions[i] = positions[i] + (-1 * Short.MIN_VALUE) + 1;
		}

		String[] names = new String[]{"Abinash", "Jyoti", "Niru", "Birakishore"};
		
		MergedBlocks.Block block = null;
		for (int i=0; i<4; i++) {
			DocMeta meta = new DocMeta();
			meta.docType = names[i];
			block = MergedBlocks.add(block, positions[i], meta.toBytes());
		}
		
		for (int i=0; i<4; i++) {
			DocMeta deMeta = new DocMeta();
			MergedBlocks.get(deMeta, block, positions[i]);
			assertEquals(names[i], deMeta.docType);
		}
		for (int i=0; i<4; i++) System.out.print(positions[i] + "/"); 
		
	}
	
	public void testDelete(Short position1, Short position2, Short position3, Short position4) throws Exception{
		int[] positions = new int[]{position1,position2,position3,position4};
		
		for (int i=0; i<4; i++) {
			positions[i] = positions[i] + (-1 * Short.MIN_VALUE) + 1;
		}

		String[] names = new String[]{"Abinash", "Jyoti", "Niru", "Birakishore"};
		
		MergedBlocks.Block block = null;
		for (int i=0; i<4; i++) {
			DocMeta meta = new DocMeta();
			meta.docType = names[i];
			block = MergedBlocks.add(block, positions[i], meta.toBytes());
		}
		
		for ( int i=0; i<4; i++) {
			assertNotNull(MergedBlocks.get(new DocMeta(), block, positions[i]));
			block = MergedBlocks.delete(block, positions[i]);
			assertNull(MergedBlocks.get(new DocMeta(), block, positions[i]));
			for ( int j=i+1; j<4; j++) {
				DocMeta deMeta = new DocMeta();
				MergedBlocks.get(deMeta, block, positions[j]);
				assertEquals(names[j], deMeta.docType);
			}
		}
	}
	
	public void testUpdatedBigger(Short position1, Short position2, Short position3, Short position4,
		String type1, String type2, String type3, String type4 ) throws Exception{
		
		int[] positions = new int[]{position1,position2,position3,position4};
		String[] types = new String[]{type1,type2,type3,type4};
		for (int i=0; i<4; i++) {
			positions[i] = positions[i] + (-1 * Short.MIN_VALUE) + 1;
		}

		String[] names = new String[]{"Abinash", "Jyoti", "Niru", "Birakishore"};
		
		MergedBlocks.Block block = null;
		for (int i=0; i<4; i++) {
			DocMeta meta = new DocMeta();
			meta.docType = names[i];
			block = MergedBlocks.add(block, positions[i], meta.toBytes());
		}
		System.out.println("Pass1");

		for ( int i=0; i<4; i++) {
			DocMeta updated = new DocMeta();
			MergedBlocks.get(updated, block, positions[i]);
			updated.docType = types[i];
			block = MergedBlocks.update(block, new DocMeta(), updated, positions[i]);
			
			DocMeta de = new DocMeta();
			MergedBlocks.get(de, block, positions[i]);
			assertEquals(updated.docType, de.docType);
			for ( int j=i+1; j<4; j++) {
				DocMeta deMeta = new DocMeta();
				MergedBlocks.get(deMeta, block, positions[j]);
				assertEquals(names[j], deMeta.docType);
			}
		}		
	}
	
	public void testDeleteAndUpdate(Short position1, Short position2,
			String type1, String type2) throws Exception{
			
			int[] positions = new int[]{position1,position2};
			String[] types = new String[]{type1,type2};
			for (int i=0; i<2; i++) {
				positions[i] = positions[i] + (-1 * Short.MIN_VALUE) + 1;
			}

			MergedBlocks.Block block = null;
			for (int i=0; i<2; i++) {
				DocMeta meta = new DocMeta();
				meta.docType = types[i];
				block = MergedBlocks.add(block, positions[i], meta.toBytes());
			}
			block = MergedBlocks.delete(block, positions[0]);
			DocMeta updated = new DocMeta();
			MergedBlocks.get(updated, block, positions[1]);
			updated.docType = types[1];

			block = MergedBlocks.update(block, new DocMeta(), updated, positions[1]);
			DocMeta deMeta = new DocMeta();
			MergedBlocks.get(deMeta, block, positions[1]);
			assertEquals(updated.docType, deMeta.docType);
	}	
	
	public void testMetaCompact(Short position1, Short position2,
			String type1, String type2) throws Exception{
			
			int[] positions = new int[]{position1,position2};
			String[] types = new String[]{type1,type2};
			for (int i=0; i<2; i++) {
				positions[i] = positions[i] + (-1 * Short.MIN_VALUE) + 1;
			}

			MergedBlocks.Block block = null;
			for (int i=0; i<2; i++) {
				DocMeta meta = new DocMeta();
				meta.docType = types[i];
				block = MergedBlocks.add(block, positions[i], meta.toBytes());
			}
			block = MergedBlocks.delete(block, positions[0]);
			DocMeta updated = new DocMeta();
			MergedBlocks.get(updated, block, positions[1]);
			updated.docType = types[1];

			block = MergedBlocks.update(block, new DocMeta(), updated, positions[1]);
			DocMeta deMeta = new DocMeta();
			MergedBlocks.get(deMeta, block, positions[1]);
			assertEquals(updated.docType, deMeta.docType);
			
			int dataLenO = block.data.length;
			block = MergedBlocks.compact(block, deMeta);
			int dataLenN = block.data.length;
			assertTrue ( dataLenO > dataLenN);
			
			MergedBlocks.get(deMeta, block, positions[1]);
			assertEquals(updated.docType, deMeta.docType);
	}	
	
	public void testTeaserCompact(Short position1, Short position2,
			String type1, String type2) throws Exception{
			
			int[] positions = new int[]{position1,position2};
			String[] types = new String[]{type1,type2};
			for (int i=0; i<2; i++) {
				positions[i] = positions[i] + (-1 * Short.MIN_VALUE) + 1;
			}

			MergedBlocks.Block block = null;
			for (int i=0; i<2; i++) {
				DocTeaser meta = new DocTeaser();
				meta.id = types[i];
				block = MergedBlocks.add(block, positions[i], meta.toBytes());
			}
			block = MergedBlocks.delete(block, positions[0]);
			DocTeaser updated = new DocTeaser();
			MergedBlocks.get(updated, block, positions[1]);
			updated.id = types[1];

			block = MergedBlocks.update(block, new DocTeaser(), updated, positions[1]);
			DocTeaser deMeta = new DocTeaser();
			MergedBlocks.get(deMeta, block, positions[1]);
			assertEquals(updated.id, deMeta.id);
			
			int dataLenO = block.data.length;
			block = MergedBlocks.compact(block, deMeta);
			int dataLenN = block.data.length;
			assertTrue ( dataLenO > dataLenN);
			
			MergedBlocks.get(deMeta, block, positions[1]);
			assertEquals(updated.id, deMeta.id);
	}	
	
	public void testAclCompact(Short position1, Short position2,
			String type1, String type2) throws Exception{
			
			int[] positions = new int[]{position1,position2};
			String[] types = new String[]{type1,type2};
			for (int i=0; i<2; i++) {
				positions[i] = positions[i] + (-1 * Short.MIN_VALUE) + 1;
			}

			MergedBlocks.Block block = null;
			for (int i=0; i<2; i++) {
				DocAcl meta = new DocAcl();
				Access access = new Access();
				access.addUid(types[i]);
				meta.editPermission = access;
				block = MergedBlocks.add(block, positions[i], meta.toBytes());
			}
			block = MergedBlocks.delete(block, positions[0]);
			DocAcl updated = new DocAcl();
			MergedBlocks.get(updated, block, positions[1]);
			Access access = new Access();
			access.addUid(types[1]);
			updated.editPermission = access;

			block = MergedBlocks.update(block, new DocAcl(), updated, positions[1]);
			DocAcl deMeta = new DocAcl();
			MergedBlocks.get(deMeta, block, positions[1]);
			assertTrue(
				Storable.compareBytes(updated.editPermission.toStorable().toBytes(), 
				deMeta.editPermission.toStorable().toBytes()));
			
			int dataLenO = block.data.length;
			block = MergedBlocks.compact(block, deMeta);
			int dataLenN = block.data.length;
			assertTrue ( dataLenO > dataLenN);
			
			MergedBlocks.get(deMeta, block, positions[1]);
			assertTrue(
				Storable.compareBytes(updated.editPermission.toStorable().toBytes(), 
				deMeta.editPermission.toStorable().toBytes()));
	}	
}
