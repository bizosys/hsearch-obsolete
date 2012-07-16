/*
* Copyright 2010 Bizosys Technologies Limited
*
* Licensed to the Bizosys Technologies Limited (Bizosys) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The Bizosys licenses this file
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
package com.bizosys.hsearch.index;

import java.util.HashSet;
import java.util.Set;

import junit.framework.TestCase;
import junit.framework.TestFerrari;

import com.bizosys.hsearch.filter.FilterIds;
import com.bizosys.hsearch.filter.Storable;

public class TermListTest extends TestCase {

	public static void main(String[] args) throws Exception {
		TermListTest t = new TermListTest();
        TestFerrari.testRandom(t);
	}
	
	public void testSingleTermDoc(String keyword, Byte termwt, Short docpos, Byte wt) throws Exception {
		TermList tl = new TermList();
		Character s = 'T';
		byte termType = 12;
		Term aTerm = new Term(keyword,s,termType, 123,docpos,wt);
		tl.add(aTerm);
		byte[] bytes = tl.toBytes();
		
		TermList tl2 = new TermList();
		byte[] hashcode = Storable.putInt(keyword.hashCode());
		byte[] onlyTerms = FilterIds.isMatchingColBytes(bytes, hashcode);
		tl2.loadTerms(onlyTerms);
		assertEquals(1, tl2.totalTerms);
		assertEquals(docpos.shortValue(), tl2.docPos[0]);
		assertEquals(wt.byteValue(), tl2.termWeight[0]);
	}
	
	public void testSingleTermDocs(String keyword, Byte termType,
		Byte w1, Short pos1, Byte w2, Short pos2) throws Exception {
		
		TermList termList1 = new TermList();
		Term aTerm = new Term(keyword,'T',termType, 123,pos1,w1);
		termList1.add(aTerm);
		Term bTerm = new Term(keyword,'T',termType, 123,pos2,w2);
		termList1.add(bTerm);
		
		byte[] hashcode = Storable.putInt(keyword.hashCode());
		byte[] termList1B = termList1.toBytes();
		byte[] onlyTerms = FilterIds.isMatchingColBytes(termList1B, hashcode);

		TermList tl2 = new TermList();
		tl2.loadTerms(onlyTerms);
		assertEquals(2, tl2.totalTerms);
		assertEquals(pos1.shortValue(), tl2.docPos[0]);
		assertEquals(w1.byteValue(), tl2.termWeight[0]);
		assertEquals(pos2.shortValue(), tl2.docPos[1]);
		assertEquals(w2.byteValue(), tl2.termWeight[1]);
	}	
	
	public void testSingleTermMergedDocs(String keyword,Byte tt1, Byte w1,
		Short pos1, Byte w2, Short pos2, Byte dt1) throws Exception {
		
		Character s = 'T';
		if ( pos1 == pos2) pos2 = (short)( pos1 + 1);

		Term aTerm = new Term(keyword,s,tt1,123,pos1,w1);
		aTerm.setDocumentTypeCode(dt1);
		Term bTerm = new Term(keyword,s,tt1,123,pos2,w2);
		bTerm.setDocumentTypeCode(dt1);
		
		TermList termList1 = new TermList();
		termList1.add(aTerm);
		byte[] termList1B = termList1.toBytes();
		
		TermList termList2 = new TermList();
		termList2.setExistingBytes(termList1B); //existing bytes
		termList2.add(bTerm); //new term
		byte[] termList2B = termList2.toBytes();
		
		TermList res = new TermList();
		byte[] hashcode = Storable.putInt(keyword.hashCode());
		byte[] onlyTerms = FilterIds.isMatchingColBytes(termList2B, hashcode);
		res.loadTerms(onlyTerms);
		System.out.println(res.toString());
		assertEquals(2, res.totalTerms);
		assertEquals(pos2.shortValue(), res.docPos[0]);
		assertEquals(w2.byteValue(), res.termWeight[0]);
		assertEquals(dt1.byteValue(), res.docTypesCodes[0]);
		assertEquals(pos1.shortValue(), res.docPos[1]);
		assertEquals(w1.byteValue(), res.termWeight[1]);
		assertEquals(dt1.byteValue(), res.docTypesCodes[1]);
	}
	
	public void testMultipleTermDoc(String keyword1, 
		String keyword2, Byte pos1, Byte w1, Byte w2) throws Exception {
		
		TermList tl = new TermList();
		Character s = 'T';
		byte termType = 12;
		Term aTerm = new Term(keyword1,s,termType, 123, pos1, w1);
		tl.add(aTerm);
		
		Term bTerm = new Term(keyword2,s,termType, 123, pos1, w2);
		tl.add(bTerm);
		
		byte[] bytes = tl.toBytes();
		
		TermList tl2 = new TermList();
		byte[] hashcode = Storable.putInt(keyword1.hashCode());
		byte[] onlyTerms = FilterIds.isMatchingColBytes(bytes, hashcode);
		tl2.loadTerms(onlyTerms);
		assertEquals(1, tl2.totalTerms);
		assertEquals(pos1.shortValue(), tl2.docPos[0]);
		assertEquals(w1.byteValue(), tl2.termWeight[0]);
	}	
	
	public void testMultipleTermDocs(String keyword1, 
			String keyword2, String keyword3) throws Exception {
		TermList tl = new TermList();
		Character s = 'T';
		byte termType = 12;
		Term aTerm = new Term(keyword1,s,termType, 123);
		aTerm.setDocumentPosition((short)111);
		aTerm.setTermWeight((byte)79);
		tl.add(aTerm);
		
		Term bTerm = new Term(keyword2,s,termType, 123);
		bTerm.setDocumentPosition((short)111);
		bTerm.setTermWeight((byte)79);
		tl.add(bTerm);
		
		Term cTerm = new Term(keyword3,s,termType, 123);
		cTerm.setDocumentPosition((short)112);
		cTerm.setTermWeight((byte)45);
		tl.add(cTerm);
		
		Term dTerm = new Term(keyword2,s,termType, 123);
		dTerm.setDocumentPosition((short)112);
		dTerm.setTermWeight((byte)23);
		tl.add(dTerm);

		byte[] bytes = tl.toBytes();
		
		TermList tl2 = new TermList();
		byte[] hashcode = Storable.putInt(keyword2.hashCode());
		byte[] onlyTerms = FilterIds.isMatchingColBytes(bytes, hashcode);
		tl2.loadTerms(onlyTerms);
		System.out.println(tl2.toString());
		assertEquals(2, tl2.totalTerms);

		assertEquals(111, tl2.docPos[0]);
		assertEquals(79, tl2.termWeight[0]);

		assertEquals(112, tl2.docPos[1]);
		assertEquals(23, tl2.termWeight[1]);
		
		
		hashcode = Storable.putInt(keyword3.hashCode());
		onlyTerms = FilterIds.isMatchingColBytes(bytes, hashcode);
		tl2.loadTerms(onlyTerms);
		System.out.println(tl2.toString());
		assertEquals(1, tl2.totalTerms);

		assertEquals(112, tl2.docPos[0]);
		assertEquals(45, tl2.termWeight[0]);
		
	}	
	
	
	public void testMultipleTermMergedDocs(
		String keyword1, String keyword2, String keyword3,
		Short pos1, Short pos2, Byte w1, Byte w2, Byte w3) throws Exception {
		
		Character s = 'T';
		byte termType = 12;
		
		Term aTerm = new Term(keyword1,s,termType, 123, pos1, w1);
		Term bTerm = new Term(keyword2,s,termType, 123, pos1, w2);
		Term cTerm = new Term(keyword3,s,termType, 123, pos2, w3);
		Term dTerm = new Term(keyword2,s,termType, 123, pos2, w3);
		
		TermList tl = new TermList();
		tl.add(aTerm);
		tl.add(bTerm);
		
		TermList t2 = new TermList();
		t2.setExistingBytes(tl.toBytes());
		tl.add(cTerm);
		tl.add(dTerm);

		byte[] bytes = tl.toBytes();
		
		TermList tl2 = new TermList();
		byte[] hashcode = Storable.putInt(keyword2.hashCode());
		byte[] onlyTerms = FilterIds.isMatchingColBytes(bytes, hashcode);
		tl2.loadTerms(onlyTerms);

		assertEquals(2, tl2.totalTerms);
		assertEquals(pos1.shortValue(), tl2.docPos[0]);
		assertEquals(w2.byteValue(), tl2.termWeight[0]);

		assertEquals(pos2.shortValue(), tl2.docPos[1]);
		assertEquals(w3.byteValue(), tl2.termWeight[1]);
		
		
		TermList tl3 = new TermList();
		hashcode = Storable.putInt(keyword3.hashCode());
		onlyTerms = FilterIds.isMatchingColBytes(bytes, hashcode);
		tl3.loadTerms(onlyTerms);
		assertEquals(1, tl3.totalTerms);
		assertEquals(pos2.shortValue(), tl3.docPos[0]);
		assertEquals(w3.byteValue(), tl3.termWeight[0]);
	}
	
	public void testUpdatesDocs(String keyword1, String keyword2, 
		String keyword3, String keyword4, Short pos1, Byte w1, 
		Short pos2, Byte w2, Byte w3) throws Exception {
		
		Character s = 'T';
		byte termType = 12;

		Term aTerm = new Term(keyword1,s,termType, 123, pos1, w1);
		Term bTerm = new Term(keyword2,s,termType, 123, pos2, w2);
		Term cTerm = new Term(keyword3,s,termType, 123, pos2, w3);
		Term dTerm = new Term(keyword4,s,termType, 123, pos2, w3);

		TermList tl = new TermList();
		tl.add(aTerm);
		tl.add(bTerm);
		byte[] origB = tl.toBytes();
		
		TermList tx = new TermList();
		tx.setExistingBytes(origB);
		tx.add(cTerm);
		tx.add(dTerm);

		byte[] hashcode = Storable.putInt(keyword2.hashCode());
		byte[] onlyTerms = FilterIds.isMatchingColBytes(tx.toBytes(), hashcode);
		TermList ty = new TermList();
		ty.loadTerms(onlyTerms);
		assertEquals(0, ty.totalTerms);
		
		hashcode = Storable.putInt(keyword1.hashCode());
		onlyTerms = FilterIds.isMatchingColBytes(tx.toBytes(), hashcode);
		TermList tz = new TermList();
		tz.loadTerms(onlyTerms);
		assertEquals(1, tz.totalTerms);
		assertEquals(pos1.shortValue(), tz.docPos[0]);
		assertEquals(w1.byteValue(), tz.termWeight[0]);
		
		hashcode = Storable.putInt(keyword4.hashCode());
		onlyTerms = FilterIds.isMatchingColBytes(tx.toBytes(), hashcode);
		TermList tu = new TermList();
		tu.loadTerms(onlyTerms);
		assertEquals(1, tu.totalTerms);
		assertEquals(pos2.shortValue(), tu.docPos[0]);
		assertEquals(w3.byteValue(), tu.termWeight[0]);
		
	}	
	
	public void testFilterByWord(
			String keyword1, String keyword2, String keyword3,
			Short pos1, Short pos2, Byte w1, Byte w2, Byte w3) throws Exception {
			
			Character s = 'T';
			byte termType = 12;
			
			Term aTerm = new Term(keyword1,s,TermType.NONE_TYPECODE, 123, pos1, w1);
			Term bTerm = new Term(keyword2,s,TermType.NONE_TYPECODE, 123, pos1, w2);
			Term cTerm = new Term(keyword3,s,termType, 123, pos2, w3);
			Term dTerm = new Term(keyword2,s,TermType.NONE_TYPECODE, 123, pos2, w3);
			
			TermList tl = new TermList();
			tl.add(aTerm);
			tl.add(bTerm);
			
			TermList t2 = new TermList();
			t2.setExistingBytes(tl.toBytes());
			tl.add(cTerm);
			tl.add(dTerm);

			byte[] bytes = tl.toBytes();
			
			TermList tl2 = new TermList();
			byte[] hashcode = Storable.putInt(keyword3.hashCode());
			byte[] onlyTerms = FilterIds.isMatchingColBytes(bytes, hashcode);
			Set<Integer> ignorePos = new HashSet<Integer>();
			
			tl2.loadTerms(onlyTerms,ignorePos, DocumentType.NONE_TYPECODE,termType);

			assertEquals(1, tl2.totalTerms);
			assertEquals(pos2.shortValue(), tl2.docPos[0]);
			assertEquals(w3.byteValue(), tl2.termWeight[0]);
		}	
}