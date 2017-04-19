package group;

import static group.ClusterUtils.isEmpty;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
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
	private final int loadType;

	public ClusterLoad(int totalBakets, int loadType) {
	   this.totalBaketNum = totalBakets;
	   this.loadType = loadType;
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
	
	public boolean hasLoad(Address address) {
		String str = load.get(address.toString());
		return !isEmpty(str); 
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
	
	public void initIfAbsent(Address owner) {
		load.putIfAbsent(owner.toString(), "");
	}

	
	public boolean contains(Address address) {
		return load.containsKey(address.toString());
	}
	
	/**
	 * I find all the nodes which share the given load type
	 * @param loadType - load type
	 * @return members list for the loadType
	 */
	public List<Address> getMembers(int loadType, List<Address> members) {
		List<Address> result = new LinkedList<Address>();
		for (Address mbr: members) {
			if(contains(mbr)) {
				result.add(mbr);
			}
		}
		return result;
	}

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder(100);
		for (String address : load.keySet()) {
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
		Map<Integer, Integer> counter = new HashMap<>();  //extra buckets counter
		int usedBucketNum = ClusterUtils.merge(usedList, current);
		final int myBucketNum = usedBucketNum; // current number of buckets
		int nodes = 1;
		for (Address adr : members) {
			if (adr.equals(currentAddr)) {// skip ourself
				continue;
			}
			nodes++;
			byte[] adrBuckets = getLoad(adr);
			incCounter(counter, ClusterUtils.count(adrBuckets));
			usedBucketNum += ClusterUtils.merge(usedList, adrBuckets);
		}

		int targetBucketNum = totalBaketNum / nodes;
		int remainder = totalBaketNum % nodes; 		// node's number which have to take extra bucket
		if(getCounter(counter, targetBucketNum + 1) < remainder) {			
			targetBucketNum++; // take extra bucket if possible
		}
		if (targetBucketNum == myBucketNum) {
			return null; // current and new allocation haven't changed
		}
		byte[] newLoad = new byte[totalBaketNum];
		System.arraycopy(current, 0, newLoad, 0, newLoad.length);

		// get new buckets
		if (targetBucketNum > myBucketNum) {
			if (usedBucketNum == totalBaketNum) {
				return null; // all buckets are taken
			}
			int delta = targetBucketNum - myBucketNum;
			delta = delta > totalBaketNum ? totalBaketNum : delta; //it is possible when nodes == 1
			final int cnt = ClusterUtils.allocate(newLoad, usedList, delta);
			return cnt == 0 ? null : newLoad;
		}
		// release buckets
		final int delta = myBucketNum - targetBucketNum; // buckets to release
		final int cnt = ClusterUtils.release(newLoad, delta);
		return cnt == 0 ? null : newLoad;
	}
	
	private Integer getCounter(Map<Integer, Integer> counter, int bucketsNum) {
		return counter.get(bucketsNum) == null ? 0 : counter.get(bucketsNum);
	}

	private void incCounter(Map<Integer, Integer> counter, int bucketsNum) {
		Integer x = counter.get(bucketsNum);
		x = (x == null) ? 1 : x + 1;
		counter.put(bucketsNum, x);
	}

	/**
	 * Delete load of nodes than do not belong the cluster  
	 * @param members - membership list
	 */
	public void cleanLoad(List<Address> members) {
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
			logger.info("Load is removed ({}): {}", loadType, address);
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
