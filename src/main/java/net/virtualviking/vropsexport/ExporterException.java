package net.virtualviking.vropsexport;

public class ExporterException extends Exception {

	public ExporterException(String message, Throwable cause) {
		super(message, cause);
	}

	public ExporterException(String message) {
		super(message);
	}

	public ExporterException(Throwable cause) {
		super(cause);
	}
}
