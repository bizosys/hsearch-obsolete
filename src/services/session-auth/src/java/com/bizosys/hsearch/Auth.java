package com.bizosys.hsearch; 

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.log4j.Logger;

import com.bizosys.hsearch.common.Account;
import com.bizosys.hsearch.common.Account.AccountInfo;
import com.bizosys.oneline.conf.Configuration;
import com.bizosys.oneline.services.Request;
import com.bizosys.oneline.services.Response;
import com.bizosys.oneline.session.Session;
import com.bizosys.oneline.util.StringUtils;

/**
 * Authenticates requests - first and everything there after.
 * @author Abinash
 *
 */
public class Auth implements Session {

	private static Logger LOG = Logger.getLogger(Auth.class);
	private String clazz = Auth.class.getName();

    private Map<String, Long> validKeys = new ConcurrentHashMap<String, Long>(500,10,5);    

    public Auth() {
	}
	
	public String getClazz() {
		return this.clazz;
	}

	/**
	 * Following steps are carries out for processing the request.
	 * <lu>
	 * <li>Stop the processing for the login sensor request.</li>
	 * <li>Check the token in the request context.</li>
	 * <li>Append the KEY+DN+ROLES+GROUPS</li>
	 * </lu>
	 */
	public Object onStart(Request request, Response response) {
		
		LOG.info("> Authentication on start.......");
		
		request.isAuthenticated = false;
		request.user = null;
		
		String hKey = request.getString(
			Account.ACCOUNT_KEY_NAME, false, true, true);
		
		// Step#1 Authentication digest is not there
		if (StringUtils.isEmpty(hKey) ) {
			LOG.info("Leaving authentication. HKEY is Empty.");
			return null;
		}
		
		long curTime = System.currentTimeMillis();
		//Step#2 Already visitted
		if ( validKeys.containsKey(hKey)) {
			long lastAccess = validKeys.get(hKey);
			long interval = curTime - lastAccess; 
			if ( interval < 1800000 ) {
				//Accessing in less than 30 mins
				validKeys.put(hKey, curTime); //Refresh time
				request.isAuthenticated = true;
				return null;
			}
		}
		
		//Step#3 Read from disk
		LOG.info("First time / Expired visit. Reading from Storage");
		AccountInfo accInfo = null;
		try {
			accInfo = Account.getAccount(hKey);
			if ( null == accInfo ) {
				if ( validKeys.containsKey(hKey)) validKeys.remove(hKey);
				return null;
			}
			validKeys.put(hKey, curTime);
			request.isAuthenticated = true;
			request.user = accInfo;
			return null;
			
		} catch ( Exception ex) {
			LOG.error("Auth first time user loading failed" + hKey, ex);
			return null;
		}
	}

	public void onFinish(Object startInfo, Request request, Response response) {
	}
	
	/**
	 * This will help us to serve the request from multiple boxes once he logs 
	 * into one server. So we can achieve single sign-on once in the server
	 * they start sharing the private key. More over; this also helps us to have
	 * no session. 
	 */
	public void setConfig(Configuration config) {
	}
}
