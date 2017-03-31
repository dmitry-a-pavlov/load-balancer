package group;

import java.io.Serializable;

//~ cluster statistics
public class Statistics implements Serializable {
	private static final long serialVersionUID = 5982118797761004551L;

	public volatile int taken = 0;     //number of calls to tale the load 
	public volatile int released = 0;  //number of calls to release the load
	public volatile int refused = 0;   //number of calls to refuse to release the load
	public volatile int calculated = 0;//number of calls to calculate the load
	public volatile int calculatedByTimeout = 0; //number of calls to recalculate the load by timeout
	
    public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append("calculateConter:\t").append(calculated).append('\n');
		sb.append("takeConter:\t\t").append(taken).append('\n');
		sb.append("releaseConter:\t\t").append(released).append('\n');
		sb.append("refuseConter:\t\t").append(refused).append('\n');
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
