package group;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Provides information about cluster state
 * 
 * @author Dmitriy Pavlov
 *
 */
public class ClusterHealth {
	
	/**
	 * OK: 				consistent - all buckets are processed by some node, 
	 * 	   				there are no buckets intersection (two+ nodes do not processes the same bucket)
	 * MISSED_BUCKETS: 	inconsistent - there is a bucket which is not processed by any node
	 * INTERSECTED:		inconsistent - there is a bucket which is not processed by two or more nodes
	 * NO_RESPONCE:		inconsistent - there are nodes which doesn't response
	 * 
	 */
	enum State {OK, MISSED_BUCKETS, INTERSECTED, NO_RESPONCE, INCORRECT_STATE }
	
	public State state = State.OK;
	
	public String bucketsCounter = "";
	
	public List<String> unreachableNodes = new ArrayList<>();
	
	/** Address and State of the current node*/
	public String currentStateNode = "";
	/** Address and State of an inconsistent node*/
	public String incorrectStateNode = "";

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append("state:\t\t").append(state).append('\n');
		sb.append("bucketsCounter:     ").append(bucketsCounter).append('\n');
		sb.append("unreachableNodes:   ").append(Arrays.toString(unreachableNodes.toArray())).append('\n');
		sb.append("currentStateNode:   ").append(currentStateNode).append('\n');
		sb.append("incorrectStateNode: ").append(incorrectStateNode).append('\n');
		return sb.toString();
	}

}
