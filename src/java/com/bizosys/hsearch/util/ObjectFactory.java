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

package com.bizosys.hsearch.util;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Stack;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RowLock;

import com.bizosys.hsearch.common.ByteField;
import com.bizosys.hsearch.dictionary.DictEntry;
import com.bizosys.hsearch.hbase.NV;
import com.bizosys.hsearch.index.Doc;
import com.bizosys.hsearch.index.Term;
import com.bizosys.hsearch.index.TermStream;
import com.bizosys.hsearch.index.TermTables;
import com.bizosys.hsearch.query.DocWeight;
import com.bizosys.hsearch.query.QueryTerm;

public class ObjectFactory {
	
	private static int MINIMUM_CACHE = 10;
	private static int MAXIMUM_CACHE = 4096;
	
	private static ObjectFactory thisInstance = new ObjectFactory();
	public static ObjectFactory getInstance() {
		return thisInstance;
	}
	
	Stack<List<Put>> putsLists = new Stack<List<Put>>();
	Stack<List<RowLock>> locksLists = new Stack<List<RowLock>>();
	Stack<List<Term>> termLists = new Stack<List<Term>>();
	Stack<List<byte[]>> byteArrLists = new Stack<List<byte[]>>();
	Stack<List<TermStream>> streamLists = new Stack<List<TermStream>>();
	Stack<List<ByteField>> fldLists = new Stack<List<ByteField>>();
	Stack<List<Doc>> docsLists = new Stack<List<Doc>>();
	Stack<Map<String, DictEntry>> dictEntries = new Stack<Map<String, DictEntry>>();
	Stack<Map<Long,Short>> blockMap = new Stack<Map<Long,Short>>();
	Stack<Map<Doc, TermTables>> docTermTables = new Stack<Map<Doc, TermTables>>();
	Stack<Map<Long, TermTables>> bucketTermTables = new Stack<Map<Long, TermTables>>();
	Stack<List<Record>> recordsLists = new Stack<List<Record>>();
	Stack<List<RecordScalar>> scalarRecordsLists = new Stack<List<RecordScalar>>();
	Stack<List<QueryTerm>> queryTermsList = new Stack<List<QueryTerm>>();
	Stack<Map<String,String>> stringMaps = new Stack<Map<String,String>>();
	Stack<Map<String, DocWeight>> docWeightMaps = new Stack<Map<String,DocWeight>>();
	Stack<Map<Integer, byte[]>> blockMaps =   new Stack<Map<Integer,byte[]>>();
	Stack<List<NV>> nvLists = new Stack<List<NV>>();
	Stack<Set<String>> stringSets = new Stack<Set<String>>();
	Stack<List<String>> stringLists = new Stack<List<String>>();
	Stack<List<Short>> shortLists = new Stack<List<Short>>();
	Stack<Set<Long>> longSets = new Stack<Set<Long>>();
	Stack<List<Integer>> integerLists = new Stack<List<Integer>>();
	
	public  Map<String, DictEntry> getDictEntryHash() {
		Map<String, DictEntry> entry = null;
		if (dictEntries.size() > MINIMUM_CACHE ) entry = dictEntries.pop();
		if ( null != entry ) return entry;
		return new Hashtable<String, DictEntry>();
	}
	
	public  void putDictEntryHash(Map<String, DictEntry> entry ) {
		if ( null == entry) return;
		entry.clear();
		if (dictEntries.size() > MAXIMUM_CACHE ) return;
		if (dictEntries.contains(entry) ) return;
		dictEntries.add(entry);
	}
	
	public  List<Doc> getDocumentList() {
		List<Doc> docs = null;
		if (docsLists.size() > MINIMUM_CACHE ) docs = docsLists.pop();
		if ( null != docs ) return docs;
		return new ArrayList<Doc>();
	}
	
	public  void putDocumentList(List<Doc> docs ) {
		if ( null == docs) return;
		docs.clear();
		if (docsLists.size() > MAXIMUM_CACHE ) return;
		if ( docsLists.contains(docs)) return;
		docsLists.push(docs);
	}	
	
	public  List<ByteField> getFieldList() {
		List<ByteField> flds = null;
		if (fldLists.size() > MINIMUM_CACHE ) flds = fldLists.pop();
		if ( null != flds ) return flds;
		return new ArrayList<ByteField>();
	}
	
	public  void putFieldList(List<ByteField> flds ) {
		if ( null == flds) return;
		flds.clear();
		if (fldLists.size() > MAXIMUM_CACHE ) return;
		if ( fldLists.contains(flds)) return;
		fldLists.push(flds);
	}		
	
	public  List<RowLock> getRowLockList() {
		List<RowLock> locks = null;
		if (locksLists.size() > MINIMUM_CACHE ) locks = locksLists.pop();
		if ( null != locks ) return locks;
		return new ArrayList<RowLock>(256);
	}
	
	public  void putRowLockList(List<RowLock> locks ) {
		if ( null == locks) return;
		locks.clear();
		if (locksLists.size() > MAXIMUM_CACHE ) return;
		if ( locksLists.contains(locks)) return;
		locksLists.push(locks);
	}	
	
	public  List<Put> getPutList() {
		List<Put> puts = null;
		if (putsLists.size() > MINIMUM_CACHE ) puts = putsLists.pop();
		if ( null != puts ) return puts;
		return new ArrayList<Put>(256);
	}
	
	public  void putPutsList(List<Put> puts ) {
		if ( null == puts) return;
		puts.clear();
		if (putsLists.size() > MAXIMUM_CACHE ) return;
		if ( putsLists.contains(puts)) return;
		putsLists.push(puts);
	}		
	
	public  List<byte[]> getByteArrList() {
		List<byte[]> bytesA = null;
		if (byteArrLists.size() > MINIMUM_CACHE ) bytesA = byteArrLists.pop();
		if ( null != bytesA ) return bytesA;
		return new ArrayList<byte[]>(32);
	}
	
	public  void putByteArrList(List<byte[]> bytesA ) {
		if ( null == bytesA) return;
		bytesA.clear();
		if (byteArrLists.size() > MAXIMUM_CACHE ) return;
		if ( byteArrLists.contains(bytesA)) return;
		byteArrLists.push(bytesA);
	}		
	
	public  List<Term> getTermList() {
		List<Term> terms = null;
		if (termLists.size() > MINIMUM_CACHE ) terms = termLists.pop();
		if ( null != terms ) return terms;
		return new ArrayList<Term>(100);
	}
	
	public  void putTermList(List<Term> terms ) {
		if ( null == terms) return;
		terms.clear();
		if (termLists.size() > MAXIMUM_CACHE ) return;
		if ( termLists.contains(terms)) return;
		termLists.push(terms);
	}	

	public  List<TermStream> getStreamList() {
		List<TermStream> streams = null;
		if (streamLists.size() > MINIMUM_CACHE ) streams = streamLists.pop();
		if ( null != streams ) return streams;
		return new ArrayList<TermStream>();
	}
	
	public  void putStreamList(List<TermStream> streams ) {
		if ( null == streams) return;
		streams.clear();
		if (streamLists.size() > MAXIMUM_CACHE ) return;
		if ( streamLists.contains(streams)) return;
		streamLists.push(streams);
	}	

	public  List<NV> getNVList() {
		List<NV> nvs = null;
		if (nvLists.size() > MINIMUM_CACHE ) nvs = nvLists.pop();
		if ( null != nvs ) return nvs;
		return new ArrayList<NV>();
	}
	
	public  void putNVList(List<NV> nvs ) {
		if ( null == nvs) return;
		nvs.clear();
		if (nvLists.size() > MAXIMUM_CACHE ) return;
		if ( nvLists.contains(nvs)) return;
		nvLists.push(nvs);
	}		
	
	public  Map<Long, Short> getBytesList() {
		Map<Long,Short> lstB = null;
		if (blockMap.size() > MINIMUM_CACHE ) lstB = blockMap.pop();
		if ( null != lstB ) return lstB;
		return new HashMap<Long,Short>();
	}
	
	public  void putBytesList(Map<Long,Short> lstB ) {
		if ( null == lstB) return;
		lstB.clear();
		if (blockMap.size() > MAXIMUM_CACHE ) return;
		if ( blockMap.contains(lstB)) return;
		blockMap.push(lstB);
	}		
	
	public  Map<Doc, TermTables> getDocTermTable() {
		Map<Doc, TermTables> dtt = null;
		if (docTermTables.size() > MINIMUM_CACHE ) dtt = docTermTables.pop();
		if ( null != dtt ) return dtt;
		return new HashMap<Doc, TermTables>();
	}
	
	public  void putDocTermTable(Map<Doc, TermTables> dtt ) {
		if ( null == dtt) return;
		dtt.clear();
		if (docTermTables.size() > MAXIMUM_CACHE ) return;
		if ( docTermTables.contains(dtt)) return;
		docTermTables.push(dtt);
	}		
	
	public  Map<Long, TermTables> getBucketTermTable() {
		Map<Long, TermTables> dtt = null;
		if (bucketTermTables.size() > MINIMUM_CACHE ) dtt = bucketTermTables.pop();
		if ( null != dtt ) return dtt;
		return new HashMap<Long, TermTables>();
	}
	
	public  void putBucketTermTable(Map<Long, TermTables> dtt ) {
		if ( null == dtt) return;
		dtt.clear();
		if (bucketTermTables.size() > MAXIMUM_CACHE ) return;
		if ( bucketTermTables.contains(dtt)) return;
		bucketTermTables.push(dtt);
	}			
	
	public  List<Record> getRecordList() {
		List<Record> records = null;
		if (recordsLists.size() > MINIMUM_CACHE ) records = recordsLists.pop();
		if ( null != records ) return records;
		return new ArrayList<Record>(256);
	}
	
	public  void putRecordList(List<Record> records ) {
		if ( null == records) return;
		records.clear();
		if (recordsLists.size() > MAXIMUM_CACHE ) return;
		if ( recordsLists.contains(records)) return;
		recordsLists.push(records);
	}
	
	public  List<RecordScalar> getScalarRecordList() {
		List<RecordScalar> records = null;
		if (recordsLists.size() > MINIMUM_CACHE ) {
			records = scalarRecordsLists.pop();
		}
		if ( null != records ) return records;
		return new ArrayList<RecordScalar>();
	}
	
	public  void putScalarRecordList(List<RecordScalar> records ) {
		if ( null == records) return;
		records.clear();
		if (scalarRecordsLists.size() > MAXIMUM_CACHE ) return;
		if ( scalarRecordsLists.contains(records)) return;
		scalarRecordsLists.push(records);
	}
	
	public  List<QueryTerm> getQueryTermsList() {
		List<QueryTerm> qts = null;
		if (queryTermsList.size() > MINIMUM_CACHE ) qts = queryTermsList.pop();
		if ( null != qts ) return qts;
		return new ArrayList<QueryTerm>();
	}
	
	public  void putQueryTermsList(List<QueryTerm> qts ) {
		if ( null == qts) return;
		qts.clear();
		if (queryTermsList.size() > MAXIMUM_CACHE ) return;
		if ( queryTermsList.contains(qts)) return;
		queryTermsList.push(qts);
	}	
	
	
	public Map<String,String> getStringMap(){
		Map<String, String> obj = null;
		if (stringMaps.size() > MINIMUM_CACHE  ) obj = stringMaps.pop();
		if ( null != obj ) return obj;
		return new HashMap<String, String>();		
	}
	
	public void putStringMap(Map<String,String> item){
		if ( null == item) return;
		item.clear();
		if (stringMaps.size() > MAXIMUM_CACHE ) return;
		if ( stringMaps.contains(item)) return;
		stringMaps.push(item);
	}
	
	public Map<String,DocWeight> getDocWeightMap(){
		Map<String,DocWeight> obj = null;
		if ( docWeightMaps.size() > MINIMUM_CACHE  ) obj = docWeightMaps.pop();
		if ( null != obj ) return obj;
		return new Hashtable<String, DocWeight>();		
	}
	
	public void putDocWeightMap(Map<String,DocWeight> obj){
		if ( null == obj) return;
		obj.clear();
		if (docWeightMaps.size() > MAXIMUM_CACHE ) return;
		if ( docWeightMaps.contains(obj)) return;
		docWeightMaps.push(obj);
	}	
	
	public Map<Integer,byte[]> getByteBlockMap(){
		Map<Integer,byte[]> obj = null;
		if (blockMaps.size() > MINIMUM_CACHE ) obj = blockMaps.pop();
		if ( null != obj ) return obj;
		return new Hashtable<Integer,byte[]>();		
	}
	
	public void putByteBlockMap(Map<Integer,byte[]> obj){
		if ( null == obj) return;
		obj.clear();
		if (blockMaps.size() > MAXIMUM_CACHE ) return;
		if ( blockMaps.contains(obj)) return;
		blockMaps.push(obj);
	}	
	
	public Set<String> getStringSet(){
		Set<String> obj = null;
		if (stringSets.size() > MINIMUM_CACHE ) obj = stringSets.pop();
		if ( null != obj ) return obj;
		return new HashSet<String>();		
	}
	
	public void putStringSet(Set<String> obj){
		if ( null == obj) return;
		obj.clear();
		if (stringSets.size() > MAXIMUM_CACHE ) return;
		if ( stringSets.contains(obj)) return;
		stringSets.push(obj);
	}			
		
	public List<String> getStringList(){
		List<String> obj = null;
		if (stringLists.size() > MINIMUM_CACHE ) obj = stringLists.pop();
		if ( null != obj ) return obj;
		return new ArrayList<String>();		
	}
	
	public void putStringList(List<String> obj){
		if ( null == obj) return;
		obj.clear();
		if (stringLists.size() > MAXIMUM_CACHE ) return;
		if ( stringLists.contains(obj)) return;
		stringLists.push(obj);
	}		
	
	public List<Short> getShortList(){
		List<Short> obj = null;
		if (shortLists.size() > MINIMUM_CACHE ) obj = shortLists.pop();
		if ( null != obj ) return obj;
		return new ArrayList<Short>();		
	}
	
	public void putShortList(List<Short> obj){
		if ( null == obj) return;
		obj.clear();
		if (shortLists.size() > MAXIMUM_CACHE ) return;
		if ( shortLists.contains(obj)) return;
		shortLists.push(obj);
	}		
	
	public List<Integer> getIntegerList(){
		List<Integer> obj = null;
		if (integerLists.size() > MINIMUM_CACHE ) obj = integerLists.pop();
		if ( null != obj ) return obj;
		return new ArrayList<Integer>();		
	}
	
	public void putIntegerList(List<Integer> obj){
		if ( null == obj) return;
		obj.clear();
		if (integerLists.size() > MAXIMUM_CACHE ) return;
		if ( integerLists.contains(obj)) return;
		integerLists.push(obj);
	}			
	

	public Set<Long> getLongSet(){
		Set<Long> obj = null;
		if (longSets.size() > MINIMUM_CACHE ) obj = longSets.pop();
		if ( null != obj ) return obj;
		return new HashSet<Long>();		
	}
	
	public void putLongSet(Set<Long> obj){
		if ( null == obj) return;
		obj.clear();
		if (longSets.size() > MAXIMUM_CACHE ) return;
		if ( longSets.contains(obj)) return;
		longSets.push(obj);
	}
	
	public String getStatus() {
		StringBuilder sb = new StringBuilder(476);
		sb.append("<o>");
		sb.append("termLists:").append(termLists.size()).append('|');
		sb.append("streamLists:").append(streamLists.size()).append('|');
		sb.append("docsLists:").append(docsLists.size()).append('|');
		sb.append("dictEntries:").append(dictEntries.size()).append('|');
		sb.append("blockMap:").append(blockMap.size()).append('|');
		sb.append("docTermTables:").append(docTermTables.size()).append('|');
		sb.append("bucketTermTables:").append(bucketTermTables.size()).append('|');
		sb.append("recordsLists:").append(recordsLists.size()).append('|');
		sb.append("scalarRecordsLists:").append(scalarRecordsLists.size()).append('|');
		sb.append("queryTermsList:").append(queryTermsList.size()).append('|');
		sb.append("stringMaps:").append(stringMaps.size()).append('|');
		sb.append("docWeightMaps:").append(docWeightMaps.size()).append('|');
		sb.append("blockMaps:").append(blockMaps.size()).append('|');
		sb.append("nvLists:").append(nvLists.size()).append('|');
		sb.append("stringSets:").append(stringSets.size()).append('|');
		sb.append("stringLists:").append(stringLists.size()).append('|');
		sb.append("shortLists:").append(shortLists.size()).append('|');
		sb.append("longSets:").append(longSets.size()).append('|');
		sb.append("integerLists:").append(integerLists.size());
		sb.append("</o>");
		return sb.toString();
	}
	
}
