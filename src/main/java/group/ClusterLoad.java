package group;

import static group.ClusterUtils.isEmpty;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.jgroups.Address;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Cluster load distribution state
 *    
 * @author Dmitriy Pavlov
 */
class ClusterLoad implements Cloneable {
	
	protected static final Logger logger = LoggerFactory.getLogger(ClusterLoad.class);

	/** 
	 * map : {Address, "b_1,b_2,...b_n"}
	 * 	      where b_i - bucket number
	 */
	private ConcurrentMap<String, String> load = new ConcurrentHashMap<>();
	private int totalBaketNum = 0;

	public ClusterLoad(int totalBakets) {
	   this.totalBaketNum = totalBakets;
	}

	/**
	 * @param address - node address
	 * @return - array of available buckets
	 */
	public byte[] getLoad(Address address) {
		byte[] result = new byte[totalBaketNum];
		String str = load.get(address.toString());
		if (!isEmpty(str)) {
			String[] buckets = str.split(",");
			for (int i = 0; i < buckets.length; i++) {
				result[Integer.parseInt(buckets[i])] = 1;
			}
		}
		return result;
	}

	public void clear() {
		load.clear();
	}

	public void putLoad(Address address, byte[] buckets) {
		if (buckets == null) {
			load.remove(address.toString());
			return;
		}
		load.put(address.toString(), ClusterUtils.toString(buckets));
	}
	
	public boolean contains(Address address) {
		return load.containsKey(address.toString());
	}

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder(100);
		for (String address : load.keySet()) {
			sb.append("\t\t\t");
			sb.append(address).append("=");
			sb.append(load.get(address));
			sb.append(";\n");
		}
		return sb.toString();
	}

	public void parceState(String clasterLoad) {
		if (isEmpty(clasterLoad)) {
			return;
		}
		String[] loads = clasterLoad.split(";");
		for (int i = 0; i < loads.length; i++) {
			if (isEmpty(loads[i]))
				continue;
			String avmLoad[] = loads[i].split("=");
			if (avmLoad.length != 2)
				continue;
			load.put(avmLoad[0], avmLoad[1]);
		}
	}
	
	public String getState() {
		StringBuilder sb = new StringBuilder(100);
		for (String address : load.keySet()) {
			sb.append(address).append("=");
			sb.append(load.get(address));
			sb.append(";");
		}
		return sb.toString();
	}


	/**
	 * @param current - current load, current buckets distribution
	 * @param currentAddr - node for the load
	 * @param members - claster's nodes list
	 * @return - new load - new array of buckets, does not modify the <code>current</code>
	 * 			 parameter. Return 'null' if nothing were changed
	 */
	public byte[] calcNewLoad(byte[] current, Address currentAddr, List<Address> members) {
		if (current == null) {
			current = getLoad(currentAddr);
		}
		byte[] usedList = new byte[totalBaketNum];
		int usedBucketNum = ClusterUtils.merge(usedList, current);
		final int myBucketNum = usedBucketNum; // current number of buckets
		int nodes = 1;
		int myIndex = 0; // number of current PE in accorders with order by
		int i = 0;
		for (Address adr : members) {
			if (adr.equals(currentAddr)) {// skip ourself
				myIndex = i;
				continue;
			}
			i++;
			nodes++;
			usedBucketNum += ClusterUtils.merge(usedList, getLoad(adr));
		}

		int targetBucketNum = totalBaketNum / nodes;
		// node's number which have to take extra bucket
		int remainder = totalBaketNum % nodes; 
		if (myIndex < remainder) {
			targetBucketNum++;
		}
		if (targetBucketNum == myBucketNum) {
			// current and new allocation haven't changed
			return null;
		}
		byte[] newLoad = new byte[totalBaketNum];
		System.arraycopy(current, 0, newLoad, 0, newLoad.length);

		// get new buckets
		if (targetBucketNum > myBucketNum) {
			if (usedBucketNum == totalBaketNum) {
				return null; // all buckets are taken
			}

			final int delta = targetBucketNum - myBucketNum;
			final int max = delta > totalBaketNum ? totalBaketNum : delta;
			final int cnt = ClusterUtils.allocate(newLoad, usedList, max);
			return cnt == 0 ? null : newLoad;
		}
		// release buckets
		final int delta = myBucketNum - targetBucketNum; // buckets to release
		final int cnt = ClusterUtils.release(newLoad, delta);
		return cnt == 0 ? null : newLoad;
	}

	/**
	 * Delete load of nodes than do not belong the cluster  
	 * @param members - membership list
	 */
	synchronized public void cleanLoad(List<Address> members) {
		ArrayList<String> toDelete = new ArrayList<>();
		main: for (String address: load.keySet()) {
			for (Address addr : members) {
				if(address.equals(addr.toString())) {
					continue main;
				}
			}
			toDelete.add(address);
		}
		for (String address: toDelete) {
			load.remove(address);
			logger.info("Load is removed: {}", address);
		}
	}
	
	public void removeLoad(Address address) {
		load.remove(address.toString());
	}
	
    /*
     * @see java.lang.Object#clone()
     */
    public Object clone() {
    	ClusterLoad obj = null;
        try {
        	obj = (ClusterLoad) super.clone();
        } catch (CloneNotSupportedException e) {
        }
        // deep copy
        obj.load = new ConcurrentHashMap<>(load);
        return obj;
    }

}
