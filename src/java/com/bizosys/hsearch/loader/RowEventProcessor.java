package com.bizosys.hsearch.loader;

import com.bizosys.oneline.ApplicationFault;
import com.bizosys.oneline.SystemFault;

public interface RowEventProcessor {
	public void onHeaderRow(String[] cells) throws ApplicationFault, SystemFault;
	public void onDataRow(String[] cells) throws ApplicationFault, SystemFault;
	public void onEnd() throws ApplicationFault, SystemFault;
}
