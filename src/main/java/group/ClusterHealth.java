package group;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.jgroups.stack.IpAddress;

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
	enum State {OK, MISSED_BUCKETS, INTERSECTED, NO_RESPONCE, DIFFERENT_STATE }
	
	public State state = State.OK;
	
	public String bucketsCounter = "";
	
	public List<String> unreachableNodes = new ArrayList<>();

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append("state:\t\t\t").append(state).append('\n');
		sb.append("bucketsCounter:\t\t\t").append(bucketsCounter).append('\n');
		sb.append("unreachableNodes:\t\t\t").append(Arrays.toString(unreachableNodes.toArray())).append('\n');
		return sb.toString();
	}

}
