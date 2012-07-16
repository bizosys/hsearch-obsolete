package com.bizosys.hsearch.loader;

import java.io.Closeable;
import java.io.IOException;

public interface RowReader extends Closeable {
	String[] readNext() throws IOException;
}
