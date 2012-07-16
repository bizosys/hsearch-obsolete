package com.bizosys.hsearch.util;

public class UnknownAccountException extends Exception {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	public UnknownAccountException() {
		super();
	}

	public UnknownAccountException(String message, Throwable cause) {
		super(message, cause);
	}

	public UnknownAccountException(String message) {
		super(message);
	}

	public UnknownAccountException(Throwable cause) {
		super(cause);
	}

}
