package group;

/**
 * @author Dmitriy Pavlov
 */
public class ClusterException extends RuntimeException {

	private static final long serialVersionUID = -3717194964607243173L;

	public ClusterException(String message, Exception cause) {
		super(message, cause);
	}

	public ClusterException(String message) {
		super(message);
	}


}
