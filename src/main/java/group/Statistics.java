package group;

import group.Cluster.Builder;

import java.io.Serializable;
import java.util.Arrays;

//~ cluster statistics
public class Statistics implements Serializable {
	private static final long serialVersionUID = 5982118797761004551L;

	public volatile int taken = 0;     //number of calls to tale the load 
	public volatile int released = 0;  //number of calls to release the load
	public volatile int refused = 0;   //number of calls to refuse to release the load
	public volatile int calculated = 0;//number of calls to calculate the load
	public volatile int calculatedByTimeout = 0; //number of calls to recalculate the load by timeout
	public final Builder config;
	public final String loadTypes;
	
	public Statistics(Builder config, String loadTypes) {
		this.config = config;
		this.loadTypes = loadTypes;
	}
	
	public Statistics(Builder config) {
		this.config = config;
		this.loadTypes = Arrays.toString(config.loadTypes);
	}

	
    public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append("nodes:\t").append(config.totalNodes).append('\n');
		sb.append("totalBuckets:\t").append(config.totalBuckets).append('\n');
		sb.append("loadTypes:\t").append(loadTypes).append('\n');
		sb.append("calculateCounter:\t").append(calculated).append('\n');
		sb.append("takeCounter:\t\t").append(taken).append('\n');
		sb.append("releaseCounter:\t\t").append(released).append('\n');
		sb.append("refuseCounter:\t\t").append(refused).append('\n');
		sb.append("calculatedByTimeout:\t").append(calculatedByTimeout).append('\n');
		return sb.toString();
    }

	public void add(Statistics st) {
		taken += st.taken;
		released += st.released;
		refused += st.refused;
		calculated += st.calculated;
	}

}
