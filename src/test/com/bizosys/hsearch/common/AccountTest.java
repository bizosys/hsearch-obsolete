package com.bizosys.hsearch.common;

import junit.framework.TestCase;
import junit.framework.TestFerrari;

import com.bizosys.hsearch.TestAll;
import com.bizosys.hsearch.common.Account.AccountInfo;
import com.bizosys.hsearch.util.Hash;
import com.bizosys.oneline.conf.Configuration;
import com.bizosys.oneline.services.ServiceFactory;

public class AccountTest extends TestCase {

	public static void main(String[] args) throws Exception {
		AccountTest t = new AccountTest();
		String[] modes = new String[] { "all", "random", "method"};
		String mode = modes[2];
		
		if ( modes[0].equals(mode) ) {
			TestAll.run(new TestCase[]{t});
		} else if  ( modes[1].equals(mode) ) {
	        TestFerrari.testRandom(t);
	        
		} else if  ( modes[2].equals(mode) ) {
			t.setUp();
			//t.testCreateAccount("bizosys", "abinash@bizosys.com", 1);
			t.testNonExistsAccount();
			t.tearDown();
		}
	}

	@Override
	protected void setUp() throws Exception {
		Configuration conf = new Configuration();
		secretKey =  conf.get("privateKey", "E64FCAE0CBC836F034A0FE4BBF6726007FCAB08BE16EB729D92FE22A219FB7EC");
		ServiceFactory.getInstance().init(conf, null);
	}
	
	@Override
	protected void tearDown() throws Exception {
		ServiceFactory.getInstance().stop();
	}
	
	private String secretKey = 
		"E64FCAE0CBC836F034A0FE4BBF6726007FCAB08BE16EB729D92FE22A219FB7EC";

	public void testNonExistsAccount() throws Exception {
		testExistsAccount("sjkfhask2892378a()ioiojk390{{}$", null);
		System.out.println("testNonExistsAccount account Sucessful");

	}

	public void testCreateAccount(String accName, 
			String accDetail, int maxBuckets) throws Exception {
		
		String hashKey = Hash.createHex(this.secretKey, accName);
		AccountInfo acc = new AccountInfo(hashKey);
		acc.active = true;
		acc.name = accName;
		acc.notes = accDetail;
		acc.maxbuckets = maxBuckets;
		Account.storeAccount(acc);
		testExistsAccount(accName, accName);
		System.out.println("testCreateAccount sucessful");
	}
	
	private void testExistsAccount(String account, String loadedAccount) throws Exception {
		String hashKey = Hash.createHex(secretKey, account);
		AccountInfo persistedAc = Account.getAccount(hashKey);
		if ( null == loadedAccount) assertNull(persistedAc);
		else assertEquals(loadedAccount, Account.getAccount(hashKey).name);
	}

}
