package group;

import java.util.Objects;

/**
 * @author Dmitriy Pavlov
 */
public class ClusterUtils {

	private ClusterUtils() {
		throw new AssertionError("Must never be called");
	}
	
	/**
	 * @return number of busy elements
	 */
	public static int markBusy(byte[] newLoad, byte[] busy) {
		if (busy == null) {
			return 0; // nothing to set
		}
		if (newLoad.length != busy.length) {
			throw new IllegalArgumentException("Incorrect length");
		}
		int n = 0; // total number of entries with non zero value
		// arrays assumed to be same length
		for (int i = 0; i < newLoad.length; i++) {
			if (newLoad[i] == 1 && busy[i] != 0) {
				newLoad[i] = 2;
				n++;
			}
		}
		return n;
	}
	

	public static int merge(byte[] list1, byte[] list2) {
		if (list2 == null) {
			return 0; // nothing to merge
		}
		if (list1.length != list2.length) {
			throw new IllegalArgumentException("Incorrect length");
		}
		int n = 0; // total number of entries with non zero value
		// arrays assumed to be same length
		for (int i = 0; i < list1.length; i++) {
			if (list1[i] == 0 && list2[i] != 0) {
				list1[i] = list2[i];
				n++;
			}
		}
		return n;
	}

	/**
	 * Will modify newLoad parameter 
	 * @param newLoad - load to be calculated
	 * @param max - the maximum buckets to be allocated
	 * @return - the number of released buckets
	 */
	public static int allocate(byte[] newLoad, byte[] usedList, int max) {
		int n = 0; // total number of allocated buckets
		for (int i = 0; i < newLoad.length; i++) {
			if (usedList[i] == 0) {
				n++;
				newLoad[i] = 1;
				if (n >= max) // reached max of required allocation
					break;
			}
		}
		return n;
	}

	/**
	 * Will modify newLoad parameter 
	 * @param newLoad - load to be calculated
	 * @param max - the maximum buckets to be released
	 * @return - the number of released buckets
	 */
	public static int release(byte[] newLoad, int max) {
		int n = 0;
		for (int i = 0; i < newLoad.length; i++) {
			if (newLoad[i] == 1) { //skip if > 1
				n++;
				newLoad[i] = 0;
				if (max == n) // reached max of required deallocation
					break;
			}
		}
		return n;
	}

	public static int count(byte[] newList) {
		if (newList == null)
			return 0;
		int n = 0;
		for (int i = 0; i < newList.length; i++) {
			if (newList[i] != 0) {
				n++;
			}
		}
		return n;
	}

	public static boolean isEmpty(String str) {
		return str == null || str.length() == 0;
	}

	public static String toString(byte[] buckets) {
		if (buckets == null) {
			return "";
		}
		StringBuilder sb = new StringBuilder();
		for (int i = 0; i < buckets.length; i++) {
			if (buckets[i] == 0) {
				continue;
			}
			if (sb.length() > 0) {
				sb.append(',');
			}
			sb.append(i);
		}
		return sb.toString();
	}

	/**
	 * @param oldLoad - old load
	 * @param newLoad - new one
	 * @return return released/left buckets.
	 */
	public static byte[] getLeftBuckets(byte[] oldLoad, byte[] newLoad) {
		Objects.requireNonNull(oldLoad);
		Objects.requireNonNull(newLoad);

		byte[] left = new byte[oldLoad.length];
		for (int i = 0; i < oldLoad.length; i++) {
			if (oldLoad[i] != 0 && newLoad[i] == 0) {
				left[i] = 1;
			}
		}
		return left;
	}
	
	/**
	 * @param oldLoad
	 * @param newLoad
	 * @return number of buckets to be left
	 */
	public static int countLeftBuckets(byte[] oldLoad, byte[] newLoad) {
		Objects.requireNonNull(oldLoad);
		Objects.requireNonNull(newLoad);
		int count = 0;
		for (int i = 0; i < oldLoad.length; i++) {
			if (oldLoad[i] != 0 && newLoad[i] == 0) {
				count++;
			}
		}
		return count;
	}
	
	public static byte[] getAddedBuckets(byte[] oldLoad, byte[] newLoad) {
		Objects.requireNonNull(oldLoad);
		Objects.requireNonNull(newLoad);

		byte[] added = new byte[oldLoad.length];
		for (int i = 0; i < oldLoad.length; i++) {
			if (oldLoad[i] == 0 && newLoad[i] != 0) {
				added[i] = 1;
			}
		}
		return added;
	}
	
	public static int getBitsSize(int originalSize) {
		return (originalSize-1)/8+1;
	}
	
	public static byte[] toBits(byte[] load) {
		byte[] result = new byte[getBitsSize(load.length)];
		for (int i = 0; i < load.length; i++) {
			if(load[i] > 0) {
				result[i/8] |= ((byte)1 << (7 - i%8));
			}
		}		
		return result;
	}

	public static byte[] fromBits(byte[] bits, int size) {
		byte[] result = new byte[size];
		for (int i = 0; i < size; i++) {
			if((bits[i/8] & ((byte)1 << (7 - i%8))) != 0) {
				result[i] = 1;
			}
		}		
		return result;
	}

}
