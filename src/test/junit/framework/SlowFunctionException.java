package junit.framework;

public class SlowFunctionException extends Exception {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	public SlowFunctionException() {
		super();
	}

	public SlowFunctionException(String message) {
		super(message);
	}

	public SlowFunctionException(Throwable cause) {
		super(cause);
	}

	public SlowFunctionException(String message, Throwable cause) {
		super(message, cause);
	}

}
