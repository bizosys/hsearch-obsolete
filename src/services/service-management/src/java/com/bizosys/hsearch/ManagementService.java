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
package com.bizosys.hsearch;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;

import com.bizosys.hsearch.common.Account;
import com.bizosys.hsearch.common.ByteField;
import com.bizosys.hsearch.common.Account.AccountInfo;
import com.bizosys.hsearch.hbase.NVBytes;
import com.bizosys.hsearch.index.DocumentType;
import com.bizosys.hsearch.index.IndexWriter;
import com.bizosys.hsearch.index.InvertedIndex;
import com.bizosys.hsearch.index.TermType;
import com.bizosys.hsearch.index.WeightType;
import com.bizosys.hsearch.inpipe.util.StopwordManager;
import com.bizosys.hsearch.inpipe.util.StopwordRefresh;
import com.bizosys.hsearch.util.Hash;
import com.bizosys.oneline.ApplicationFault;
import com.bizosys.oneline.SystemFault;
import com.bizosys.oneline.conf.Configuration;
import com.bizosys.oneline.services.Request;
import com.bizosys.oneline.services.Response;
import com.bizosys.oneline.services.Service;
import com.bizosys.oneline.services.ServiceMetaData;
import com.bizosys.oneline.util.StringUtils;
import com.bizosys.oneline.util.XmlUtils;

public class ManagementService implements Service {
	public static Logger l = Logger.getLogger(ManagementService.class.getName());

	private static final boolean DEBUG_ENABLED = l.isDebugEnabled();
	
	Configuration conf = null;
	private String secretKey = StringUtils.Empty;

	public void process(Request req, Response res) {
		
		String action = req.action;
	

		try {
			if ( "account.create".equals(action) ) {
				this.createAccount(req, res);
				
			} else if ( "account.get".equals(action) ) {
				this.getAccountInformation(req, res);

			} else if ( "account.modify".equals(action) ) {
				this.modifyAccountInformation(req, res);

			} else if ( "account.buckets.set".equals(action) ) {
				this.setBuckets(req, res);

			} else if ( "account.bucket.detail".equals(action) ) {
				this.getInvertedIndex(req, res);

			} else if ( "account.bucket.truncate".equals(action) ) {
				this.deleteBucket(req, res);

			} else if ( "account.truncate".equals(action) ) {
				this.truncate(req, res);
				
			} else if ( "account.doccodes.get".equals(action) ) {
				this.getDocumentTypeCodes(req, res);

			} else if ( "account.doccodes.set".equals(action) ) {
				this.setDocumentTypeCodes(req, res);

			} else if ( "account.doccodes.append".equals(action) ) {
				this.appendDocumentTypeCodes(req, res);

			} else if ( "account.termcodes.get".equals(action) ) {
				this.getTermTypeCodes(req, res);

			} else if ( "account.termcodes.set".equals(action) ) {
				this.setTermTypeCodes(req, res);
				
			} else if ( "account.termcodes.append".equals(action) ) {
				this.appendTermTypeCodes(req, res);
				
			} else if ( "account.fieldweight.get".equals(action) ) {
				this.getWeightCodes(req, res);

			} else if ( "account.fieldweight.set".equals(action) ) {
				this.setWeightCodes(req, res);
				
			} else if ( "account.fieldweight.append".equals(action) ) {
				this.appendWeightCodes(req, res);

			}  else if ( "hsearch.stopwords.add".equals(action) ) {
				this.addStopwords(req, res);

			}  else if ( "hsearch.stopwords.get".equals(action) ) {
				this.getStopwords(req, res);

			} else {
				res.error("Failed Unknown operation : " + action);
			}
		} catch (Exception ix) {
			l.fatal("Failure > ", ix);
			res.error("Failure : ManagementService: " + ix.getMessage() + "\n" + req.toString() );
		}
	}

	/**
	 * Creates an account and provides the API KEy.
	 * @param req
	 * @param res
	 * @throws ApplicationFault
	 * @throws IOException
	 * @throws SystemFault
	 */
	private void createAccount(Request req, Response res) 
		throws ApplicationFault, IOException, SystemFault{
		
		if ( DEBUG_ENABLED) l.debug("Account creation Start");
		String accName = req.getString("name", false, true, false);
		String accDetail = req.getString("detail", false, true, false);
		int maxBuckets = req.getInteger("buckets", 1);
		
		String hashKey = Hash.createHex(this.secretKey, accName);
		AccountInfo existingAccount = Account.getAccount(hashKey);
		if ( null != existingAccount) {
			res.error("Account already exists.");
			if ( l.isInfoEnabled() ) l.info(existingAccount.toXml());
			return;
		}
		
		AccountInfo acc = new AccountInfo(hashKey);
		acc.active = true;
		acc.name = accName;
		acc.notes = accDetail;
		acc.maxbuckets = maxBuckets;
		Account.storeAccount(acc);
		if ( DEBUG_ENABLED) l.debug("Account creation Sucessful");
		Account.getCurrentBucket(acc);
		if ( DEBUG_ENABLED) l.debug("Bucket creation Sucessful");
		
		res.writeXml("<APIKEY>" + hashKey + "</APIKEY>");
	}
	
	/**
	 * Get detail information about the account.
	 * @param req
	 * @param res
	 * @throws ApplicationFault
	 * @throws IOException
	 * @throws SystemFault
	 */
	private void getAccountInformation(Request req, Response res) 
	throws ApplicationFault, IOException, SystemFault{

		AccountInfo existingAcc = Account.getActiveAccountInfo(req, res);
		if ( null == existingAcc) return;
		res.writeXml(existingAcc.toXml());
	}		
	
	/**
	 * Modify the account detail information. 
	 * @param req
	 * @param res
	 * @throws ApplicationFault
	 * @throws IOException
	 * @throws SystemFault
	 */
	private void modifyAccountInformation(Request req, Response res) 
	throws ApplicationFault, IOException, SystemFault{

		AccountInfo existingAcc = Account.getActiveAccountInfo(req, res);
		if ( null == existingAcc) return;
		
		String notes = req.getString("notes", false, true, true);
		existingAcc.notes = notes;
		
		Account.storeAccount(existingAcc);
		res.writeXml(existingAcc.toXml());
	}		
		
	/**
	 * Allocates bucket to an account
	 * @param req
	 * @param res
	 * @throws ApplicationFault
	 * @throws IOException
	 * @throws SystemFault
	 */
	private void setBuckets(Request req, Response res) 
	throws ApplicationFault, SystemFault{
	
		AccountInfo acc = Account.getActiveAccountInfo(req, res);
		if ( null == acc) throw new ApplicationFault("Account is not found");
		
		int maxBuckets = req.getInteger("buckets", -1);
		if ( maxBuckets < 1) throw new ApplicationFault("Bucket should be 1 or above");
		if ( null != acc.buckets) {
			int usedBuckets = acc.buckets.size();
			if ( usedBuckets > maxBuckets) throw new ApplicationFault(
			"You have already used " + 	usedBuckets + 
			" buckets and setting max buckets to " + maxBuckets);
		}
		acc.maxbuckets = maxBuckets;
		Account.storeAccount(acc);
		res.writeXml("<account>" + acc.toXml() + "</account>");
	}
	
	private void deleteBucket(Request req, Response res) 
	throws ApplicationFault, SystemFault{
	
		AccountInfo acc = Account.getActiveAccountInfo(req, res);
		if ( null == acc) throw new ApplicationFault("Account is not found");
		
		Long bucketId = req.getLong("bucket", true);
		if ( null == bucketId) throw new ApplicationFault("Bucket Id is not found.");
		
		byte[] bucketIdB = ByteField.putLong(bucketId);
		boolean isBucketOwner = false;
		for (byte[] aBucket : acc.buckets) {
			if ( ByteField.compareBytes(aBucket, bucketIdB)) {
				isBucketOwner = true;
				break;
			}
		}
		
		if ( !isBucketOwner) {
			throw new ApplicationFault("Tenant does not own the bucket.");
		}
		
		IndexWriter.getInstance().truncate(acc.APIKEY, bucketId);
		res.writeXml("OK");
	}	
	
	private void truncate(Request req, Response res) 
	throws ApplicationFault, SystemFault{
	
		AccountInfo acc = Account.getActiveAccountInfo(req, res);
		if ( null == acc) throw new ApplicationFault("Account is not found");
		
		IndexWriter.getInstance().truncate(acc.APIKEY);
		res.writeXml(Account.getAccount(acc.APIKEY).toXml());
	}		
	
	/**
	 * Get the index of the account for a given bucket 
	 * @param req
	 * @param res
	 * @throws ApplicationFault
	 * @throws IOException
	 * @throws SystemFault
	 */
	private void getInvertedIndex(Request req, Response res) 
	throws ApplicationFault, IOException, SystemFault{
		
		AccountInfo acc = Account.getActiveAccountInfo(req, res);
		if ( null == acc) return;

		long bucketId = req.getLong("bucketid", true);
		if ( null == acc) return;
		
		boolean isMyBucket = false;
		long allocatedBucketId = 0L;
		for (byte[] bucketIdB : acc.buckets) {
			allocatedBucketId = ByteField.getLong(0, bucketIdB);
			if ( allocatedBucketId == bucketId) {
				isMyBucket = true;
				break; 
			}
		}
		
		if ( !isMyBucket) {
			res.error("You do not own the requested bucket id." );
			return;
		}

		PrintWriter pw = res.getWriter();
		pw.write(Response.XML_VERSION_LINE);
		res.writeHeader();
		
		List<NVBytes> nvs = Account.get(bucketId);
		if ( null == nvs) {
			res.writeFooter();
			return;
		}
		
		for (NVBytes nv : nvs) {
			if ( null == nv.data) continue;
			List<InvertedIndex> indexes = InvertedIndex.read(nv.data);
			if ( null == indexes) continue;
			for (InvertedIndex index : indexes) {
				pw.write("<invindex>");
				pw.write(index.toString());
				pw.write("<invindex>");
			}
		}
		res.writeFooter();
	}

	private void getDocumentTypeCodes(Request req, Response res) 
	throws ApplicationFault, IOException, SystemFault{

		AccountInfo acc = Account.getActiveAccountInfo(req, res);
		if ( null == acc) return;

		DocumentType dt = DocumentType.getInstance();
		Map<String, Byte> codes = dt.load(acc.name);
		res.writeXml(dt.toXml(codes));
	}
	
	
	@SuppressWarnings("unchecked")
	private void setDocumentTypeCodes(Request req, Response res) 
	throws ApplicationFault, IOException, SystemFault{
		
		AccountInfo acc = Account.getActiveAccountInfo(req, res);
		if ( null == acc) return;

		Map<String,Byte> codes = (Map<String,Byte>) req.getObject("typecodes", true);
		DocumentType dt = DocumentType.getInstance();
		dt.persist(acc.name, codes);
		res.writeXml("OK");
	}
	
	@SuppressWarnings("unchecked")
	private void appendDocumentTypeCodes(Request req, Response res) 
	throws ApplicationFault, IOException, SystemFault{
		
		AccountInfo acc = Account.getActiveAccountInfo(req, res);
		if ( null == acc) return;

		Map<String,Byte> codes = (Map<String,Byte>) req.getObject("typecodes", true);
		DocumentType dt = DocumentType.getInstance();
		dt.append(acc.name, codes);
		res.writeXml("OK");
	}	
	
	private void getTermTypeCodes(Request req, Response res) 
	throws ApplicationFault, IOException, SystemFault{

		AccountInfo acc = Account.getActiveAccountInfo(req, res);
		if ( null == acc) return;

		TermType tt = TermType.getInstance(true);
		Map<String, Byte> codes = tt.load(acc.name);
		res.writeXml(tt.toXml(codes));
	}
	
	
	@SuppressWarnings("unchecked")
	private void setTermTypeCodes(Request req, Response res) 
	throws ApplicationFault, IOException, SystemFault{
		
		AccountInfo acc = Account.getActiveAccountInfo(req, res);
		if ( null == acc) return;

		Map<String,Byte> codes = (Map<String,Byte>) req.getObject("typecodes", true);
		TermType dt = TermType.getInstance(true);
		dt.persist(acc.name, codes);
		res.writeXml("OK");
	}
	
	@SuppressWarnings("unchecked")
	private void appendTermTypeCodes(Request req, Response res) 
	throws ApplicationFault, IOException, SystemFault{
		
		AccountInfo acc = Account.getActiveAccountInfo(req, res);
		if ( null == acc) return;

		Map<String,Byte> codes = (Map<String,Byte>) req.getObject("typecodes", true);
		TermType dt = TermType.getInstance(true);
		dt.append(acc.name, codes);
		res.writeXml("OK");
	}
	
	private void getWeightCodes(Request req, Response res) 
	throws ApplicationFault, IOException, SystemFault{

		AccountInfo acc = Account.getActiveAccountInfo(req, res);
		if ( null == acc) return;

		WeightType wt = WeightType.getInstance(true);
		Map<String, Byte> codes = wt.load(acc.name);
		res.writeXml(wt.toXml(codes));
	}
	
	
	@SuppressWarnings("unchecked")
	private void setWeightCodes(Request req, Response res) 
	throws ApplicationFault, IOException, SystemFault{
		
		AccountInfo acc = Account.getActiveAccountInfo(req, res);
		if ( null == acc) return;

		Map<String,Byte> codes = (Map<String,Byte>) req.getObject("weightcodes", true);
		WeightType wt = WeightType.getInstance(true);
		wt.persist(acc.name, codes);
		res.writeXml("OK");
	}
	
	@SuppressWarnings("unchecked")
	private void appendWeightCodes(Request req, Response res) 
	throws ApplicationFault, IOException, SystemFault{
		
		AccountInfo acc = Account.getActiveAccountInfo(req, res);
		if ( null == acc) return;

		Map<String,Byte> codes = (Map<String,Byte>) req.getObject("weightcodes", true);
		WeightType wt = WeightType.getInstance(true);
		wt.append(acc.name, codes);
		res.writeXml("OK");
	}		
	
	
	private void addStopwords(Request req, Response res) throws ApplicationFault, SystemFault{
		String stopwords = req.getString("stopwords", true, true, true);
		List<String> stopwordL = StringUtils.fastSplit(stopwords, ',');
		StopwordManager.getInstance().setStopwords(stopwordL);
		new StopwordRefresh().process(); //A 30mins job is done in sync mode
		res.writeXml("OK");
	}
	
	private void getStopwords(Request req, Response res) throws ApplicationFault, SystemFault{
		Set<String> words = StopwordManager.getInstance().getStopwords();
		StringBuilder sb = new StringBuilder();
		sb.append("<words>");
		for (String word : words) {
			sb.append(word).append('\n');
		}
		sb.append("</words>");
		res.writeXml(sb.toString());
	}

	public boolean init(Configuration conf, ServiceMetaData meta) {
		XmlUtils.xstream.alias("account", AccountInfo.class);
		this.conf = conf;
		this.secretKey =  conf.get("privateKey", "E64FCAE0CBC836F034A0FE4BBF6726007FCAB08BE16EB729D92FE22A219FB7EC");
		l.info("Hex code initialized.");
		return true;
	}

	public void stop() {
	}
	
	public String getName() {
		return "ManagementService";
	}	
}
