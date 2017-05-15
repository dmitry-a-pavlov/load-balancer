package group;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
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
		return (originalSize-1)/8 + 1;
	}
	
	private static int getIntsSize(int bytesSize) {
		return (bytesSize - 1)/4 + 1;
	}

	
	/**
	 * I turn byte array of 0s or 1s into bits array
	 * @param load - array of 0s OR 1s 
	 * @return - compressed array
	 */
	public static byte[] toBits(byte[] load) {
		byte[] result = new byte[getBitsSize(load.length)];
		for (int i = 0; i < load.length; i++) {
			if(load[i] > 0) {
				result[i/8] |= ((byte)1 << (7 - i%8));
			}
		}		
		return result;
	}

	/**
	 * I turn bits array into byte array of 0s or 1s 
	 * @param bits - array where every bit have to be turned into byte  
	 * @return - uncompressed array of 0s or 1s
	 */
	public static byte[] fromBits(byte[] bits, int size) {
		byte[] result = new byte[size];
		for (int i = 0; i < size; i++) {
			if((bits[i/8] & ((byte)1 << (7 - i%8))) != 0) {
				result[i] = 1;
			}
		}		
		return result;
	}
	
	
	public static byte[] intToByte(int[] array) {
		if(array == null) {
			return null;
		}
		byte[] result = new byte[array.length * 4];
		for (int i = 0; i < array.length; i++) {
			write32bit(array[i], result, i*4);
		}
		return result;
	}
	
	public static int[] byteToInt(byte[] array) {
		if(array == null) {
			return null;
		}
		if(array.length == 0) {
			return new int[] {};
		}
		int[] result = new int[getIntsSize(array.length)];
		for (int i = 0; i < result.length; i++) {
			result[i] = read32bit(array, i*4);
		}
		return result;
	}

	
    /**
     * Writes a 32bit integer at the index.
     */
    public static void write32bit(int value, byte[] code, int index) {
        code[index]     = (byte)((value >>> 24) & 0xff);
        code[index + 1] = (byte)((value >>> 16) & 0xff);
        code[index + 2] = (byte)((value >>>  8) & 0xff);
        code[index + 3] = (byte)((value >>>  0) & 0xff);
    }


    /**
     * Reads a 32bit integer at the index.
     */
    public static int read32bit(byte[] code, int index) {
        return ( code[index]             << 24) | 
        	   ((code[index + 1] & 0xff) << 16) | 
        	   ((code[index + 2] & 0xff) << 8)  | 
        	   ((code[index + 3] & 0xff) << 0);
    }

    //==================================================================
    // Date format
    //==================================================================

    public static final String ISO_NO_MILI_NO_ZONE = "yyyy-MM-dd'T'HH:mm:ss";
    public static final String ISO_NO_MILI = "yyyy-MM-dd'T'HH:mm:ssZZ";
    public static final String ISO = "yyyy-MM-dd'T'HH:mm:ss.SSSZ";  

    protected static final SimpleDateFormat SDF = new SimpleDateFormat(ISO);

    public static Date getFormattedDate(String isoDateString) {
      try {
        return SDF.parse(isoDateString);
      } catch (ParseException e) {
        throw new ClusterException("Wrong date format: " + isoDateString);
      }
    }

    public static String formatDate(Date date) {
      return SDF.format(date);
    }
    
    public static String formatDate(long millis) {
        return SDF.format(new Date(millis));
    }
    
    /**
     * @param nodeLoad - 1,3,...  - comma separated list of buckets
     * @param totalBaketNum - number of buckets for the given load type
     * @return One|Zero array where non-zero element index is buckets number for the nodeLoad [0,0,...,1,.,1,..]
     */
    public static byte[] parse(String nodeLoad, int totalBaketNum) {
      byte[] result = new byte[totalBaketNum];
      if (!isEmpty(nodeLoad)) {
        String[] buckets = nodeLoad.split(",");
        for (int i = 0; i < buckets.length; i++) {
          result[Integer.parseInt(buckets[i])] = 1;
        }
      }
      return result;
    }

    /**
     * @param load - #adr1=1,2;adr2=3,5;...;
     * @return {Node_Address, buckets_distribution}, where buckets_distribution ::= (byte[]){0,0,.,1,...} One|Zero bucket array
     */
    public static Map<String, byte[]> clusterStateToMap(String load, int totalBaketNum) {
      Map<String, byte[]> loadMap = new HashMap<>();
      if(isEmpty(load)) {
        return loadMap;
      }
      
      String[] avmLoads = load.split(";");
      for(String avmLoad: avmLoads) {
        if(avmLoad.trim().length() == 0)
          continue;
        
        String[] str = avmLoad.split("=");
        if(str.length == 0)
          continue;
        
        String address = str[0].trim(); 
        String nodeLoad = str.length == 2? str[1].trim(): null; //AVM may have no load at the moment! Don't skip
        byte[] buckets = ClusterUtils.parse(nodeLoad, totalBaketNum);
        loadMap.put(address, buckets);
      }
      return loadMap;
    }

}
