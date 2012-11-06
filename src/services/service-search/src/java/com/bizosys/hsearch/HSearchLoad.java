package com.bizosys.hsearch;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.Map;

import com.bizosys.hsearch.common.Account;
import com.bizosys.hsearch.common.Account.AccountInfo;
import com.bizosys.hsearch.common.HDocument;
import com.bizosys.hsearch.index.IndexWriter;
import com.bizosys.oneline.conf.Configuration;
import com.bizosys.oneline.services.Request;
import com.bizosys.oneline.services.Response;
import com.bizosys.oneline.services.ServiceFactory;
import com.bizosys.oneline.util.StringUtils;
import com.bizosys.oneline.util.XmlUtils;


public class HSearchLoad {

	boolean isMultiClient = true;
	AccountInfo acc = null;
	boolean concurrency = true;
	String APIKEY = "anonymous";
	
	private void init() throws Exception {
		Configuration conf = new Configuration();
		ServiceFactory.getInstance().init(conf, null);

		this.APIKEY = runOptions.get("apikey");
		this.acc = Account.getAccount(APIKEY);
		if ( null == acc) {
			acc = new AccountInfo(APIKEY);
			acc.name = APIKEY;
			acc.maxbuckets = 15;
			Account.storeAccount(acc);
			System.out.println("New account is created");
		} else {
			IndexWriter.getInstance().truncate(this.APIKEY);
			this.acc = Account.getAccount(APIKEY);
			acc.maxbuckets = 15;
			Account.storeAccount(acc);
			System.out.println("Account already exist");
		}
	}
	
	private void load() {
		XmlUtils.xstream.alias("hdoc", HDocument.class);
		SearchService ss = new SearchService();
		ss.init(null, null); 
		
		HashMap<String, String> input = new HashMap<String, String>();
		input.put("document.prestine", "<hdoc><locale>en</locale><preview>youtube</preview></hdoc>");
		input.put("document.url", runOptions.get("document.url"));
		input.put("document.type", runOptions.get("document.type"));
		input.put("id.column", runOptions.get("id.column"));
		input.put("url.column", runOptions.get("url.column"));
		input.put("weight.column", runOptions.get("weight.column"));
		input.put("columns.desc", runOptions.get("columns.desc"));
		input.put("columns.separator", runOptions.get("columns.separator"));
		input.put("linebreak", runOptions.get("linebreak"));
		input.put("columns.format", runOptions.get("columns.format"));
		input.put("columns.nonempty", runOptions.get("columns.nonempty"));
		input.put("columns.title", runOptions.get("columns.title"));
		input.put("columns.preview", runOptions.get("columns.preview"));
		input.put("keyword.column", runOptions.get("keyword.column"));
		input.put("index.start", runOptions.get("index.start"));
				
		input.put("columns.indexable", runOptions.get("columns.indexable")); 
		input.put("index.batch.size", runOptions.get("index.batch.size"));
		input.put("index.runplan", runOptions.get("index.runplan"));
		
		Request req = new Request("search", "document.load", input);
		req.isAuthenticated = true;
		req.user = acc;
		
		PrintWriter pw = new PrintWriter(System.out, true);
		Response res = new Response(pw);
		ss.process(req, res);
		System.out.println("Processing Completed");
	}
	
	/**
	 * @param args
	 */
	public static Map<String, String> runOptions = new HashMap<String, String>(12);
	
	public static void main(String[] args) throws Exception {
		if ( null != args) {
			for (String arg : args) {
				String[] parts = StringUtils.getStrings(arg, '=');
				if ( parts.length == 1) runOptions.put(parts[0], "");
				else if ( parts.length == 2) runOptions.put(parts[0], parts[1]);
				else {
					System.err.println("Wrong input, " + arg);
					System.exit(1);
				}
			}
		}
		
		System.out.println("Run Options :"); 
		for (String option : runOptions.keySet()) {
			System.out.println(" " + option + "\t\t" + runOptions.get(option));
		}
		
		HSearchLoad loader = new HSearchLoad();
		loader.init();
		loader.load();
	}

}
