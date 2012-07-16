package com.bizosys.hsearch.benchmark;

public class LuceneRead {

	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception{
		if ( null == args) return;
		for (String query : args) {
			LuceneIndexManager.getInstance().search(query);
		}
	}

}
