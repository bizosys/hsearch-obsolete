package com.bizosys.hsearch.common;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;

import com.bizosys.hsearch.filter.IStorable;
import com.bizosys.hsearch.filter.Storable;
import com.bizosys.hsearch.hbase.HReader;
import com.bizosys.hsearch.hbase.HWriter;
import com.bizosys.hsearch.hbase.NV;
import com.bizosys.hsearch.hbase.NVBytes;
import com.bizosys.hsearch.schema.ILanguageMap;
import com.bizosys.hsearch.schema.IOConstants;
import com.bizosys.hsearch.util.RecordScalar;
import com.bizosys.oneline.ApplicationFault;
import com.bizosys.oneline.SystemFault;
import com.bizosys.oneline.services.Request;
import com.bizosys.oneline.services.Response;
import com.bizosys.oneline.util.StringUtils;

public class Account {

	public static Logger l = CommonLog.l;
	private static final boolean INFO_ENABLED = l.isInfoEnabled();
	private static final boolean DEBUG_ENABLED = l.isDebugEnabled();
	
	
	public static final String ACCOUNT_KEY_NAME = "hkey"; 
	public static final byte[] BUCKET_COUNTER_BYTES = "BUCKET_COUNTER".getBytes();
	
	/**
	 * Gives the account details for the given API KEY
	 * @param APIKEY
	 * @return
	 * @throws IOException
	 * @throws SystemFault
	 */
	public static AccountInfo getAccount(String APIKEY) throws SystemFault{

		Account.init();
		NV nv = new NV(IOConstants.NAME_VALUE_BYTES,IOConstants.NAME_VALUE_BYTES);
		RecordScalar scalar = new RecordScalar(new Storable(APIKEY), nv);
		
		HReader.getScalar(IOConstants.TABLE_CONFIG,scalar);
		if ( null == nv.data ) return null;
		return new AccountInfo(APIKEY, nv.data.toBytes(),0);
	}
	
	/**
	 * store the account information (create/modify)
	 * @param APIKEY
	 * @param acc
	 * @throws IOException
	 */
	public static void storeAccount(AccountInfo acc) throws SystemFault {
		Account.init();
		NV nv = new NV(IOConstants.NAME_VALUE_BYTES, 
				IOConstants.NAME_VALUE_BYTES, acc);
		RecordScalar record = new RecordScalar(new Storable(acc.APIKEY), nv);
		try {
			HWriter.getInstance(true).insertScalar(IOConstants.TABLE_CONFIG, record);
			if ( INFO_ENABLED ) {
				int buckets = ( null == acc.buckets) ? 0 : acc.buckets.size();
				l.info(acc.name + " stored with total buckets " + buckets);
			}
		} catch (IOException ex) {
			throw new SystemFault(ex);
		}
	}

	/**
	 * Get the current bucket for this account
	 * There could be no bucket created
	 * There are buckets 
	 * @return
	 * @throws ApplicationFault
	 */
	public static long getCurrentBucket(AccountInfo acc) throws ApplicationFault, SystemFault {
		
		if ( null != acc.curBucket) return acc.curBucket; 
		
		//	Refresh before creating a bucket.
		AccountInfo freshAccount = Account.getAccount(acc.APIKEY);
		if ( null == freshAccount) throw new ApplicationFault("Account is not found.");
		if ( null == acc.buckets) acc.refresh(freshAccount);

		//No buckets created yet.
		if ( null == acc.buckets) acc.buckets = new ArrayList<byte[]>();
		if ( 0 == acc.buckets.size()) {
			if ( DEBUG_ENABLED) l.debug("No buckets allocated.");
			if ( 0 == acc.maxbuckets) throw new ApplicationFault ("Allowed maximum buckets is 0.");
			acc.curBucket = createBucketId(1);
			if ( DEBUG_ENABLED) l.debug("Bucket " + acc.curBucket + " created for " + acc.name );
			acc.buckets.add( ByteField.putLong(acc.curBucket));
			Account.storeAccount(acc);
			return acc.curBucket;
		}
		
		acc.curBucket = ByteField.getLong(0, acc.buckets.get(acc.buckets.size() - 1)); 
		return acc.curBucket;
	}
	
	/**
	 * Get the Next Bucket. If not available create it.
	 * @return
	 * @throws ApplicationFault
	 */
	public static long getNextBucket(AccountInfo acc) throws ApplicationFault, SystemFault {
		acc.refresh(Account.getAccount(acc.APIKEY));
		if ( null == acc.buckets) {
			acc.buckets = new ArrayList<byte[]>();
		}
		int used = acc.buckets.size();
		if ( used >= acc.maxbuckets) 
			throw new ApplicationFault (
				"Reached allocated bucket limit. Used/max is "
				+ used + "/" + acc.maxbuckets);
		
		acc.curBucket = createBucketId(1);
		acc.buckets.add( ByteField.putLong(acc.curBucket));
		Account.storeAccount(acc);
		return acc.curBucket;
	}				

	
	/**
	 * This creates bucket Id, unique across machines.
	 * @return	The bucket Id
	 * @throws SystemFault
	 */
	private static long createBucketId(int amount) throws SystemFault {
		
		Account.init();
		if ( DEBUG_ENABLED ) l.debug("Account > Creating a new bucket Zone");
		
		/**
		 * Get next bucket Id
		 */
		NV nv = new NV(IOConstants.NAME_VALUE_BYTES,IOConstants.NAME_VALUE_BYTES);
		
		RecordScalar scalar = new RecordScalar(BUCKET_COUNTER_BYTES, nv); 
		long bucketId = HReader.idGenerationByAutoIncr(IOConstants.TABLE_CONFIG,scalar,amount);

		/**
		 * Put the bucket as a row for counting document serials. 
		 */
		if ( DEBUG_ENABLED ) l.debug("Account > The bucket counter is moved till :" + bucketId);

		/**
		 * This document will contain many documents. We need to generate IDs for these
		 * documents too.. This will be an AUTO INCREMENTAL way too. Means, we need to
		 * enter row for each generate Bucket Ids
		 */
		
		long startPos = 1;
		nv.data = new Storable(startPos);
		try {
			for (int i=0; i< amount; i++) {
				long curBucket =  bucketId - i; /** Counting from back */
				RecordScalar docSerial = new RecordScalar(Storable.putLong(curBucket), nv); 
				HWriter.getInstance(true).insertScalar(IOConstants.TABLE_CONFIG, docSerial);
				if ( DEBUG_ENABLED) l.debug("Account > bucket [" + curBucket + "], document serial counter set at 1.");
			}
			return bucketId;
		
		} catch (IOException ex) {
			StringBuilder buckets = new StringBuilder();
			for (int i=0; i< amount; i++) {
				buckets.append('[').append(bucketId - i).append(']');
			}
			l.fatal("Account > Problem during setting " + 
					"document serial counter : " + buckets.toString(), ex);
			throw new SystemFault(ex);
		}
	}
	
	public static void resetDocumentCounter(long bucketId) throws SystemFault {
		NV nv = new NV(IOConstants.NAME_VALUE_BYTES,IOConstants.NAME_VALUE_BYTES);		
		long startPos = 1;
		nv.data = new Storable(startPos);
		RecordScalar docSerial = new RecordScalar(Storable.putLong(bucketId), nv);
		try {
			HWriter.getInstance(true).insertScalar(IOConstants.TABLE_CONFIG, docSerial);
		} catch (IOException ex) {
			StringBuilder buckets = new StringBuilder();
			l.fatal("Account > Problem during resetting document serial counter : " + buckets.toString(), ex);
			throw new SystemFault(ex);
		}
		if ( INFO_ENABLED ) l.info("Account > bucket [" + bucketId + "], document serial counter set at 1.");	
	}
	
	/**
	 * This create document serial no inside a bucket id, unique across machines
	 * @param bucketId	The current bucket id
	 * @return	Moved document serial position
	 * @throws SystemFault
	 * @throws BucketIsFullException
	 */
	public static short generateADocumentSerialId(long bucketId) 
		throws SystemFault, BucketIsFullException {
		
		/**
		 * Generate Ids for this bucket
		 */
		
		Account.init();
		if ( DEBUG_ENABLED ) l.debug("Generating buckets keys");
		NV nv = new NV(IOConstants.NAME_VALUE_BYTES,IOConstants.NAME_VALUE_BYTES);
		byte[] pkBucketId = Storable.putLong(bucketId);
		RecordScalar scalar = new RecordScalar(pkBucketId, nv);
		long bucketMaxPos =  
			HReader.idGenerationByAutoIncr(IOConstants.TABLE_CONFIG,scalar,1);
		if ( DEBUG_ENABLED) l.debug("Buckets keys generated :" + bucketMaxPos);

		if (  bucketMaxPos > IOConstants.BUCKET_PACKING_LIMIT) {
			l.warn("Crossed the bucket limit of storage :" + bucketMaxPos);
			BucketIsFullException bife = new BucketIsFullException(bucketMaxPos);
			throw bife;
		}
		return new Long(bucketMaxPos).shortValue();
	}
	
	/**
	 * This create document serial no inside a bucket id, unique across machines
	 * @param bucketId	The current bucket id
	 * @return	Moved document serial position
	 * @throws SystemFault
	 * @throws BucketIsFullException
	 */
	public static AutoIncrIdRange generateAvailableDocumentSerialIds(
		long bucketId, int askedAmount) throws SystemFault {
		
		/**
		 * Generate Ids for this bucket
		 */
		
		Account.init();
		if ( DEBUG_ENABLED ) l.debug("Generating buckets keys");
		NV nv = new NV(IOConstants.NAME_VALUE_BYTES,IOConstants.NAME_VALUE_BYTES);
		byte[] pkBucketId = Storable.putLong(bucketId);
		RecordScalar scalar = new RecordScalar(pkBucketId, nv);
		long bucketMaxPos =  
			HReader.idGenerationByAutoIncr(IOConstants.TABLE_CONFIG,scalar,askedAmount);
		if ( DEBUG_ENABLED ) l.debug("Buckets keys incremented to :" + bucketMaxPos);

		int maxValue = IOConstants.BUCKET_PACKING_LIMIT;
		Long startPosition = bucketMaxPos - askedAmount;
		
		AutoIncrIdRange keyRanges = null;
		if (  bucketMaxPos >= maxValue) {
			Long avlAmount = maxValue - startPosition + 1;
			l.warn("Crossed the bucket limit of storage :" + bucketMaxPos);
			keyRanges = new AutoIncrIdRange(
				startPosition.shortValue(), avlAmount.shortValue() );
		} else {
			keyRanges = new AutoIncrIdRange( startPosition.shortValue(),
				new Integer(askedAmount).shortValue());
		}
			

		if ( INFO_ENABLED ) l.info(keyRanges.toString());
		return keyRanges;

	}	
	
	/**
	 * Initializes the term buckets
	 * Initial System: There will be no bucket. Start from Long.MIN_VALUE
	 * Second time onwards : Continue 
	 */
	private static boolean isInitialized = false;
	public static void init() {
		if (isInitialized) return;
		
		isInitialized = true;
		try {
			NV nv = new NV(IOConstants.NAME_VALUE_BYTES,IOConstants.NAME_VALUE_BYTES);
			if ( ! HReader.exists(IOConstants.TABLE_CONFIG, BUCKET_COUNTER_BYTES)) {
				if ( DEBUG_ENABLED) l.debug("Bucket Counter setup is not there. Setting up bucket id counter.");
				RecordScalar bucketCounter = new RecordScalar(new Storable(BUCKET_COUNTER_BYTES), nv);
				nv.data = new Storable(Long.MIN_VALUE);
				HWriter.getInstance(true).insertScalar(IOConstants.TABLE_CONFIG, bucketCounter);
				if ( INFO_ENABLED ) l.info("Bucket Counter setup is complete.");
			}
		} catch (IOException ex) {
			l.fatal("TermBucket > Bucket Counter Creation Failure:", ex);
			System.exit(1);
		} catch (SystemFault ex) {
			l.fatal("TermBucket > Bucker Bucket Counter Creation Failure:", ex);
			System.exit(1);
		}
	}	
	

	/**
	 * This gives all the rows from all tables.
	 * @param bucketId	Bucket Id
	 * @return	List of name-value bytes
	 * @throws SystemFault
	 */
	public static List<NVBytes> get(long bucketId) throws SystemFault {
		Account.init();
		List<NVBytes> allFields = null; 
		for (Character c : ILanguageMap.ALL_TABLES) {
			List<NVBytes> nvs = HReader.getCompleteRow(c.toString(),Storable.putLong(bucketId));
			if ( null == allFields) allFields = nvs;
			if ( null != nvs) allFields.addAll(nvs);
		}
		return allFields;
	}
	
	public static AccountInfo getActiveAccountInfo(Request req, Response res) throws SystemFault {
		if ( !req.isAuthenticated) {
			res.error( "Bad Key or no " + ACCOUNT_KEY_NAME + " parameter for API Access");
			return null;
		}
		
		AccountInfo account = null;
		if ( null == req.user) {
			String hKey = req.getString(ACCOUNT_KEY_NAME, false, true, true);
			account = getAccount(hKey);
		} else {
			account = (AccountInfo) req.user;
		}

		if ( account.active) return account;
		res.error( "Your account is not active.");
		return null;
	}
	
	public static class AccountInfo implements IStorable {
		
		public String APIKEY = StringUtils.Empty;
		public boolean active = true;
		public String name = StringUtils.Empty;
		public String notes = StringUtils.Empty;
		public int maxbuckets = 1;
		public List<byte[]> buckets = null;
		public Long curBucket = null;
		
		public AccountInfo(String APIKEY) {
			this.APIKEY = APIKEY;
		}
		
		public AccountInfo(String APIKEY, byte[] data, int pos) {
			this.APIKEY = APIKEY;
			this.fromBytes(data, pos);
		}
		
		/**
		 * Load from the byte values
		 */
		public int fromBytes(byte[] data, int pos) {
			
			if ( null == data) return -1;
			this.active = (data[pos++] == (byte)1);
			
			int idLen = ByteField.getInt(pos, data);
			pos = pos + 4;
			
			int keyLen = ByteField.getInt(pos, data);
			pos = pos + 4;

			int bucketLen = ByteField.getInt(pos, data);
			pos = pos + 4;

			byte[] idB = new byte[idLen];
			System.arraycopy(data, pos, idB, 0, idLen);
			name = ByteField.getString(idB);
			pos = pos + idLen;
			
			byte[] keyB = new byte[keyLen];
			System.arraycopy(data, pos, keyB, 0, keyLen);
			notes = ByteField.getString(keyB);
			pos = pos + keyLen;

			int totalBuckets = bucketLen/8;
			buckets = new ArrayList<byte[]>(totalBuckets);
			for ( int i=0; i< totalBuckets; i++) {
				byte[] bucketNo = new byte[8];
				System.arraycopy(data,pos,bucketNo,0,8) ;
				buckets.add(bucketNo);
				pos = pos + 8;
			}
			
			this.maxbuckets = ByteField.getInt(pos, data);
			pos = pos + 4;
			
			return pos;
			
		}
		
		public byte[] toBytes() {

			byte[] idB = ByteField.putString(name);
			byte[] keyB = ByteField.putString(notes);
			byte[] bucketsB = null;
			
			if ( null == this.buckets) {
				bucketsB = new byte[0];
			} else {
				bucketsB = new byte[this.buckets.size() * 8];
				for (int i=0; i< this.buckets.size(); i++) {
					System.arraycopy(
						this.buckets.get(i), 0,bucketsB, i*8, 8);
				}
			}

			int idLen = idB.length;
			int keyLen = keyB.length;
			int bucketLen = bucketsB.length;
			int totalB = 1 + idLen + keyLen + bucketLen + 4 /** bucket Index */;
			
			byte[] bytes = new byte[12 + totalB];
			bytes[0] = (active) ? (byte)1 : (byte)0;
			int pos = 1;
			pos = writeSize(bytes,ByteField.putInt(idLen),pos);
			pos = writeSize(bytes,ByteField.putInt(keyLen),pos);
			pos = writeSize(bytes,ByteField.putInt(bucketLen),pos);
			
			System.arraycopy(idB, 0, bytes, pos, idLen);
			pos = pos + idLen;
			
			System.arraycopy(keyB, 0, bytes, pos, keyLen);
			pos = pos + keyLen;
			
			System.arraycopy(bucketsB, 0, bytes, pos, bucketLen);
			pos = pos + bucketLen;
			
			pos = writeSize(bytes,ByteField.putInt(maxbuckets),pos);
			pos = pos + 4;
			return bytes;
		}
		
		public int writeSize(byte[] bytes, byte[] lenB, int pos) {
			bytes[pos++] = lenB[0];
			bytes[pos++] = lenB[1];
			bytes[pos++] = lenB[2];
			bytes[pos++] = lenB[3];
			return pos;
		}
		
		public void refresh(AccountInfo freshAccount) throws ApplicationFault{
			if ( null == freshAccount) throw new ApplicationFault("Account is deleted.");
			this.buckets = freshAccount.buckets;
			this.maxbuckets = freshAccount.maxbuckets;
			this.notes = freshAccount.notes;
			this.active = freshAccount.active;
		}
		
		public void refresh() throws ApplicationFault, SystemFault{
			AccountInfo freshAccount = Account.getAccount(APIKEY);
			refresh(freshAccount);
		}

		@Override
		public String toString() {
			return this.name; 
		}

		public String toXml() {
			StringBuilder sb = new StringBuilder();
			sb.append("<account><name>").append(name).append("</name>");
			sb.append("<active>").append(active).append("</active>");
			sb.append("<buckets>");
			for (byte[] bucket : buckets) {
				sb.append("<bucket>").append(ByteField.getLong(0, bucket)).append("</bucket>");
			}
			sb.append("</buckets>");
			sb.append("<maxbuckets>").append(maxbuckets).append("</maxbuckets>");
			sb.append("<notes>").append(notes).append("</notes></account>");
			return sb.toString(); 
		}
		
	}
}
