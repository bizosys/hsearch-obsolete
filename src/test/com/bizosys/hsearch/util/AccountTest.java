package com.bizosys.hsearch.util;

import java.util.ArrayList;

import junit.framework.TestCase;
import junit.framework.TestFerrari;

import com.bizosys.hsearch.common.Account;
import com.bizosys.hsearch.common.ByteField;
import com.bizosys.hsearch.common.Account.AccountInfo;
import com.bizosys.oneline.ApplicationFault;
import com.bizosys.oneline.conf.Configuration;
import com.bizosys.oneline.services.ServiceFactory;

public class AccountTest extends TestCase {

	static String secretKey = "^&$$&My Secret Key@#!!@";
	
	public static void main(String[] args) throws Exception {
		AccountTest t = new AccountTest();
        //TestFerrari.testRandom(t);
		t.testSerialIdCreation();
	}
	
	@Override
	protected void setUp() throws Exception {
		Configuration conf = new Configuration();
		ServiceFactory.getInstance().init(conf, null);
	}
	
	@Override
	protected void tearDown() throws Exception {
		ServiceFactory.getInstance().stop();
	}
	
	
	public void testSerialIdCreation() throws Exception {

		for (int i=0; i< Short.MAX_VALUE + 10; i++) {
			Short docSerialId = Account.generateADocumentSerialId(11);
			System.out.println(docSerialId);
		}
	}
	
	
	public void testCreateAccount(String accName, 
		String notes, Boolean active) throws Exception {
	
		String hashKey = Hash.createHex (accName, secretKey);
		
		if ( null != Account.getAccount(hashKey)) {
			throw new ApplicationFault ("Account already exists");
		}
	
		AccountInfo acc = new AccountInfo(hashKey);
		acc.active = active;
		acc.name = accName;
		acc.notes = notes;
		int totalBuckets = 1;
		acc.buckets = new ArrayList<byte[]>();
		for (int i=0; i< totalBuckets; i++) {
			Account.getNextBucket(acc);
		}
		
		//Eveything should just run fine.
	}
	
	public void testGetAccount(String accName, 
		String notes, Boolean active) throws Exception {
		
		testCreateAccount(accName, notes, active);
		String hashKey = Hash.createHex (accName, secretKey);
		AccountInfo accInfo = Account.getAccount(hashKey);
		assertEquals(accInfo.name, accName);
		assertEquals(accInfo.notes, notes);
		assertEquals(accInfo.buckets.size(), 1);
		assertEquals(accInfo.maxbuckets, 1);
		assertEquals(accInfo.active, active.booleanValue());
	}	
	
	public void testModifyAccount(String accName, 
		String notes, Boolean active) throws Exception {

		testCreateAccount(accName, notes, active);
		String hashKey = Hash.createHex (accName, secretKey);

		AccountInfo accInfo = Account.getAccount(hashKey);
		accInfo.active = !active;
		accInfo.notes = notes + "(Modified)";
		long nextBucket = Account.getNextBucket(accInfo);
		
		AccountInfo modifiedAcc = Account.getAccount(hashKey);
		
		assertEquals(modifiedAcc.name, accName);
		assertEquals(modifiedAcc.notes, accInfo.notes);
		
		long lastBucket = ByteField.getLong(0, 
			modifiedAcc.buckets.get(modifiedAcc.buckets.size() -1));
		assertEquals(nextBucket, lastBucket);
		assertEquals(modifiedAcc.maxbuckets, 1);
		assertEquals(modifiedAcc.active, accInfo.active);
	}
	
}
