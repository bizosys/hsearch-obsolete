package com.bizosys.hsearch.loader;

import com.bizosys.oneline.util.StringUtils;

public class RowEventProcessorStdOut implements RowEventProcessor {

	public void onDataRow(String[] cells) {
		System.out.println(StringUtils.arrayToString(cells, '|'));
	}

	public void onHeaderRow(String[] cells) {
		System.out.println(StringUtils.arrayToString(cells, '|'));
		System.out.println("======================================================================================================");
	}

	public void onEnd() {
		System.out.println("======================================================================================================");
	}

}
