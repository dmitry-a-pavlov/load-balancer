package group;

import static group.LoadRequest.Type.ACK;
import static group.LoadRequest.Type.COMMIT;
import static group.LoadRequest.Type.RELEASE;
import static group.LoadRequest.Type.TAKE;
import group.ClusterHealth.State;
import group.LoadRequest.Type;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.jgroups.Address;
import org.jgroups.JChannel;
import org.jgroups.Message;
import org.jgroups.ReceiverAdapter;
import org.jgroups.View;
import org.jgroups.blocks.RequestOptions;
import org.jgroups.blocks.ResponseMode;
import org.jgroups.blocks.RpcDispatcher;
import org.jgroups.stack.IpAddress;
import org.jgroups.util.RspList;
import org.jgroups.util.Util;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This is implementation of load balance algorithm over a network connection.
 * It will distribute configurable number of buckets over the nodes of the cluster 
 *   
 * @author Dmitriy Pavlov
 */
public class Cluster extends ReceiverAdapter {

	protected static final Logger logger = LoggerFactory
			.getLogger(Cluster.class);
	
	private final Builder config;
	
	private RpcDispatcher disp;
	private JChannel channel;

	private final List<Address> members = new ArrayList<>();
	private boolean leader = false;
	private Address my_addr = null;
	
	private final Statistics stat = new Statistics(); 

	/**
	 * Shared state.
	 * The major problem is to provide consistency of the shared state 
	 * among all nodes of the cluster 
	 */
	private ClusterLoad theLoads = null; 
	private ClusterLoad newLoads = null; // pending state

	private byte[] buckets = null; 	  // current load
	private byte[] newBuckets = null; // new unconfirmed load

	// wait for a half node or specified number of nodes to be started 
	// before calculate load
	private boolean election = true;
	
	/**
	 * counts numbers of request for release buckets
	 * used to reduce number of recalculation
	 * 
	 * @protectedBy theLoads
	 */
	private volatile int releaseCounter = 0;
	
	private volatile long calculateLoadLastTime = 0; //in nanoseconds
	
	private ScheduledExecutorService serviceSheduler;
	
	public static interface ChangeLoad {
		byte[] changeLoad(byte[] oldLoad, byte[] newLoad);
	}

	// ~constructors ------------------------------------------------
	public static Builder builder() {
		return new Builder();
	}
	
	private Cluster(Builder cfg) {  
		config = cfg;
		buckets = new byte[config.totalBuckets];
		newBuckets = new byte[config.totalBuckets];
		theLoads = new ClusterLoad(config.totalBuckets);
		newLoads = new ClusterLoad(config.totalBuckets);
	}

	public void start() {
		try {
			serviceSheduler = Executors.newScheduledThreadPool(1);
			channel = new JChannel();
			channel.setReceiver(this);
			disp = new RpcDispatcher(channel, this, this, this);
			
			channel.connect("AVMCluster");
			my_addr = channel.getAddress();
			channel.getState(null, 10000);
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(-1);
		}
	}

	public void stop() {
		channel.disconnect();
		serviceSheduler.shutdown();
	}

	@Override
	public void getState(OutputStream output) throws Exception {
		synchronized (theLoads) {
			String clusterLoad = theLoads.getState();
			Util.objectToStream(clusterLoad, new DataOutputStream(output));
		}
	}

	@Override
	public void setState(InputStream input) throws Exception {
		setState(Util.objectFromStream(new DataInputStream(input)));
	}

	@Override
	public void suspect(Address mbr) {
		super.suspect(mbr);
		logger.info("Crashed: {}", mbr.toString());
	}
	
	@Override
	public void viewAccepted(View view) {
		setInternalState(view.getMembers());
		my_addr = channel.getAddress();
		if (election && members.size() < config.nodesToWait ) {
			return; // new load => wait for quorum
		}
		election = false;
		calculateClusterLoad(false);
	}

	/**
	 * The main method to calculate the cluster load
	 * The only leader is eligible to calculate the cluster load!
	 */
	private void calculateClusterLoad(boolean forcibly) {
		theLoads.cleanLoad(members);
		if (!leader) {
			return;
		}
		synchronized (theLoads) {
			stat.calculated++;
			if(!forcibly && releaseCounter > 0) {
				//Wait for a follower to release its buckets  
				logger.debug("Wait acknowledgment for RELESE request");
				return; 
			}
			releaseCounter = 0;
			calculateLoadLastTime = System.nanoTime();
			newLoads = (ClusterLoad) theLoads.clone(); //based on current load 
			logger.info("Calculate cluster load");
			for (Address addr : members) {
				byte[] load = theLoads.getLoad(addr); //current load
				byte[] newLoad = newLoads.calcNewLoad(load, addr, members);
				if (newLoad == null) {
					continue; // no changes
				}
				int leftCount = ClusterUtils.countLeftBuckets(load, newLoad);
				Type command = leftCount > 0? RELEASE: TAKE;
				
				//short cut for the leader node, no need in extra acknowledgment communication
				if (my_addr.equals(addr)) {
					newBuckets = (command == RELEASE)? releaseLeaderLoad(load, newLoad): newLoad;
					newLoads.putLoad(my_addr, newBuckets);
					sendCommit(newBuckets, my_addr, command); 
					continue;
				}
				releaseCounter += (command == RELEASE? 1:0);
				if(releaseCounter > 0 && command == TAKE) {
					continue; //it have to wait before buckets will be released   
				}
				this.newLoads.putLoad(addr, newLoad); //will check acknowledgment
				sendCommand(command, addr, newLoad);
			}
			if(releaseCounter > 0) { 
				scheduleTimeout();
			}
		}
	}

	/** 
	 * Define recalculation by timeout task.
	 * The goal is to reduce the number of calls of calculateClusterLoad().
	 * If leader knows some node must release buckets then it have to recalculate the load after the release will be confirmed.
	 * But if there are more then one nodes to have to release its buckets then the leader should wait all of them
	 * to reduce the number of recalculations (calls of calculateClusterLoad()).
	 * In other case leader will provoke a ping-pong game. 
	 * 
	 * It is likely that some node will fail to release its buckets, then it is necessary to 
	 * provide some reasonable timeout to recalculate load after some of the node have released its buckets
	 * and the cluster has free buckets.
	 */
	private void scheduleTimeout() {
		Runnable dogwatch = new Runnable() {
		  final long lastTime = Cluster.this.calculateLoadLastTime;
		  @Override
		  public void run() {
			if((lastTime == Cluster.this.calculateLoadLastTime) //no recalculation happened since dog watch creation
				&& releaseCounter > 0) { // some release request was not committed timeout 
				calculateClusterLoad(true);
				logger.debug("Load recalculation by timeout");
				stat.calculatedByTimeout++;
			}
		  }

		};
		serviceSheduler.schedule(dogwatch, config.recalLoadTimeout, TimeUnit.MILLISECONDS);
		logger.debug("Scheduling load recalculation by timeout in " + TimeUnit.MILLISECONDS.toSeconds(config.recalLoadTimeout) + " secs.");
	}

	//~ communication ----------------------------------------------------------------------------------
	@Override
	public void receive(Message msg) {
		if (my_addr == null) {
			my_addr = channel.getAddress();
		}
		if (msg.getDest() != null && !msg.getDest().equals(my_addr)) {
			throw new ClusterException("Incorrect address: " + my_addr	+ " expected: " + msg.getDest());
		}
		LoadRequest req = null;
		try {
			req = (LoadRequest) Util.streamableFromByteBuffer(LoadRequest.class, msg.getBuffer());
			switch (req.type) {
			case COMMIT:// message sent from leader to followers & leader itself 
				synchronized (theLoads) {
					if (my_addr.equals(req.owner) && Arrays.equals(req.load, newBuckets)) {
						switch (req.ackType) {
						case RELEASE:
							buckets = newBuckets;
							break;
						case TAKE:
							takeLoad(buckets, newBuckets); //take real load
							buckets = newBuckets;
							break;
						default:
							throw new ClusterException("Illigal command to commit: " + req.ackType);
						}
					}
					theLoads.putLoad(req.owner, req.load); //first - update the common state
					if(leader							   //only the leader is eligible to calculate load
						&& (req.ackType == Type.RELEASE)   //got free buckets => can recalculate state!
						&& !req.owner.equals(my_addr)      //no needs to calculate twice, commit is sent by leader wile recalculation
						&& (--releaseCounter == 0)) 	   //all response received (protected by theLoads) 
					{ 
						calculateClusterLoad(true); 
					}
				}
				break;
			case REFUSE:// message for the leader
				byte[] load = theLoads.getLoad(msg.getSrc()); //current load of the refusing follower!
				byte[] busyBuckets = req.load;
				if (ClusterUtils.markBusy(load, busyBuckets) == 0) {
					throw new ClusterException("Must have busy buckets");
				}
				//recalculate load for the node
				synchronized (theLoads) {
					stat.refused++;
					byte[] newLoad = newLoads.calcNewLoad(load, msg.getSrc(), members);
					if(newLoad == null) {
						this.newLoads.removeLoad(msg.getSrc());
						break;
					}
					
					int leftCount = ClusterUtils.countLeftBuckets(load, newLoad);
					Type command = leftCount > 0? RELEASE: TAKE;
					if(command == RELEASE) {
						this.newLoads.putLoad(msg.getSrc(), newLoad);
						sendCommand(Type.RELEASE, msg.getSrc(), newLoad);
					}
					//TAKE command has to be sent only from calculateClusterLoad() 
				}
				break;
			case TAKE:// for follower from a leader
				byte[] added = ClusterUtils.getAddedBuckets(buckets, req.load);
				logger.info("Load to be taken: [{}]", ClusterUtils.toString(added));
				if(ClusterUtils.count(added) == 0) {
				    break;
				}
				synchronized (theLoads) {
					newBuckets = req.load;
					sendAck(Type.TAKE, msg.getSrc(), newBuckets); // to a leader
				}
				break;
			case RELEASE:// message for follower
				byte[] left = ClusterUtils.getLeftBuckets(buckets, req.load);
				logger.info("Load to be released: [{}]", ClusterUtils.toString(left));
				if(ClusterUtils.count(left) == 0) {
				    break;
				}
				synchronized (theLoads) { //to support 'refuse' process we need to try release buckets here (not in 'commit' stage) 
					synchronized (config.releaseLock) {				
						byte[] busy = releaseLoad(buckets, req.load);
						if (ClusterUtils.count(busy) > 0) {
							// send to a leader - need recalculate the load
							sendCommand(Type.REFUSE, msg.getSrc(), busy);
							return;
						}
					}
					//Here we are sure - there are some free buckets in the cluster
					newBuckets = req.load;
					sendAck(Type.RELEASE, msg.getSrc(), newBuckets); // to a leader
				}
				break;
			case ACK:// message for the leader: change is confirmed (only a leader can check the acknowledgement)
				byte[] newLoad = newLoads.getLoad(msg.getSrc());
				// conversation filter:
				// if newLoads has been changed by a call calculateClusterLoad(...)
				// the request have to be ignored
				if(newLoad != null && Arrays.equals(newLoad, req.load)) {
					sendCommit(newLoad, msg.getSrc(), req.ackType); // message to all: update your state 
				}
				break;
			}

		} catch (Exception e) {
			throw new ClusterException("Cannot receive message: " + req, e);
		}

	}

	//~ Load manipulation ----------------------------------------------------

	/** 
	 * It is possible to release load on leader host without extra communication
	 * @protectedBy theLoads
	 * 
	 * @param oldLoad - current load
	 * @param newLoad - newly calculated load
	 * @return new load, probably recalculated
	 */
	private byte[] releaseLeaderLoad(byte[] oldLoad, byte[] newLoad) {
		if(!leader) {
			return null;
		}
		synchronized (config.releaseLock) {
			byte[] busy = releaseLoad(oldLoad, newLoad);
			if (ClusterUtils.markBusy(oldLoad, busy) > 0) {
				newLoad = newLoads.calcNewLoad(oldLoad, my_addr, members);
				releaseLoad(oldLoad, newLoad);
			}
		}
		byte[] left = ClusterUtils.getLeftBuckets(oldLoad, newLoad);
		logger.info("Leader released the load: {} - left buckets", ClusterUtils.toString(left));
		return newLoad;
	}

	/**
	 * Threads and resources must be prepared here to process load, but
	 * processing have to start only after this.buckets get load
	 * 
	 * @param added - buckets to be taken
	 */
	private void takeLoad(byte[] oldLoad, byte[] newLoad) {
		stat.taken++;
		if(config.takeLoad != null) {
			config.takeLoad.changeLoad(oldLoad, newLoad);
		}
		logger.info("Load is taken: [{}]", ClusterUtils.toString(newLoad));
	}

	
	/**
	 * Verify: is it possible to release real load.
	 * Must release either ALL or nothing, ONLY a leader must recalculate load.
	 * Potentially can get a starvation - when trying to release 1 bucket from two
	 * which can be always busy, but in this case the node must wait and then release resources
	 * when they are free.
	 * 
	 * The Node responsibilities is to wait when resources will be free and can be released. 
	 *  
	 * @param newLoad - the new load
	 * @return return array with busy buckets !ONLY!, can return 'null' what means no busy buckets found.    
	 */
	private byte[] releaseLoad(byte[] oldLoad, byte[] newLoad) {
		stat.released++;
		if(config.releaseLoad == null) {
			return null;
		}
		byte[] busy = null;
		try {
			busy = config.releaseLoad.changeLoad(oldLoad, newLoad);
			logger.info("Load is released: [{}]", ClusterUtils.toString(newLoad));
		} catch (Exception e) {
			logger.error("Fail to release buckets", e);
			return ClusterUtils.getLeftBuckets(oldLoad, newLoad); //refuse to release all buckets
		}
		return busy;
	}

	public void setInternalState(List<Address> mbrs) {
		members.clear();
		for (Address mbr : mbrs)
			addNode(mbr);
		leader = mbrs.size() <= 1
				|| (mbrs.size() > 1 && mbrs.iterator().next().equals(my_addr));
	}

	public void addNode(Address member) {
		Address tmp;
		for (int i = 0; i < members.size(); i++) {
			tmp = members.get(i);
			if (member.equals(tmp))
				return;
		}
		members.add(member);
	}

	public void removeNode(Object member) {
		for (int i = 0; i < members.size(); i++) {
			Object tmp = members.get(i);
			if (member.equals(tmp)) {
				members.remove(members.get(i));
				break;
			}
		}
	}

	public void setState(Object new_state) {
		if (new_state == null)
			return;
		try {
			String clusterLoad = (String) new_state;
			synchronized (theLoads) {
				theLoads.clear();
				theLoads.parceState(clusterLoad);
			}
		} catch (Exception e) {
			logger.error("setState:", e);
		}
	}

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append("Members:\t\t\t").append(members).append('\n');
		sb.append("I am:\t\t\t").append(my_addr);
		if(my_addr instanceof IpAddress) {
			sb.append("\t\t\t").append(((IpAddress)my_addr).toString());
		}
		sb.append('\n');
		sb.append("Leader:\t\t\t").append(leader).append('\n');
		sb.append("Cluster load:\n").append(theLoads.toString())
				.append('\n');
		sb.append("Current load:\t\t\t").append('[')
				.append(ClusterUtils.toString(buckets)).append(']');
		return sb.toString();
	}

	//~ Communication -----------------------------------------------------------------
	
	public void send(Type command, Address dest, LoadRequest request) {
		try {
			byte[] buffer = Util.streamableToByteBuffer(request);
			logger.info(request + " to " + (dest == null? "ALL": dest));
			channel.send(new Message(dest, my_addr, buffer));
		} catch (Exception e) {
			throw new ClusterException("Cannot send message", e);
		}
	}
	
	public void sendCommand(Type command, Address dest, byte[] load) {
		send(command, dest, new LoadRequest(command, load));
	}

	/**
	 * Acknowledgement.
	 * The goal is to be sure a new load is received and common state is the same.
	 * @param ackType - acknowledge type
	 * @param dest - destination address
	 * @param newLoad - new load
	 */
	private void sendAck(Type ackType, Address dest, byte[] newLoad) {
		send(ACK, dest, new LoadRequest(ACK, newLoad, null, ackType));
	}

	/**
	 * Confirmation, will be sent to all nodes
	 * @param load
	 * @param owner - owner of the load
	 * @param ackType - what is committed
	 */
	public void sendCommit(byte[] load, Address owner, Type ackType) {
		send(COMMIT, null, new LoadRequest(COMMIT, load, owner, ackType));
	}
	
    //~ --- STATIC CLASSES ---
    public static class Builder {
    	
    	/** 
    	 * Number of nodes to be wait before starting load calculation
    	 * If it is not specified it will be assigned in accordance to the totalNodes as a QUORUM 
    	 * for a leader election    
    	 */
    	int nodesToWait = -1;

		/** Total number of buckets in the cluster */
    	int totalBuckets = 20;
    	
    	/** Expected number of nodes in the cluster */
    	int totalNodes = 1;
    	
    	/** recalculation timeout in milliseconds */
    	int recalLoadTimeout = 5000; //milliseconds;
    	
    	/** Callback for release load */
    	private ChangeLoad releaseLoad; 
    	
    	/**Callback for take load*/
    	private ChangeLoad takeLoad;
    	
    	/** this lock should synchronize resources before to be released */
    	private Object releaseLock = new Object(); 

    	public Builder releaseLock(Object lock) {
    		releaseLock = lock;
    		return this;
    	}
    	
    	public Builder releaseLoad(ChangeLoad callback) {
    		releaseLoad = callback;
    		return this;
    	}
    	
    	public Builder takeLoad(ChangeLoad callback) {
    		takeLoad = callback;
    		return this;
    	}


    	public Builder totalBuckets(int num) {
    		totalBuckets = num;
    		if(nodesToWait == -1) { 
    		   nodesToWait = totalNodes / 2 + 1;
    		}
    		return this;
    	}
    	
    	public Builder nodesToWait(int num) {
    		nodesToWait = num;
    		return this;
    	}
    	
    	public Builder totalNodes(int num) {
    		totalNodes = num;
    		return this;
    	}
    	
    	public Builder recalLoadTimeout(int milli) {
    		recalLoadTimeout = milli;
    		return this;
    	}
    	
    	
    	public Cluster build() {
    		return new Cluster(this); 
    	}
    	
    }

    public String getState() {
    	return theLoads.getState();
    }
	
    public byte[] getBuckets() {
    	return buckets;
    }
    
    public Statistics getStatistics() {
    	return stat;
    }
    
    /**
     * Take cluster statistics
     * @param timeout - time to wait for response
     * @return
     */
    public Statistics getClusterStatistics(long timeout) {
    	RspList<Statistics> list = null;
    	try {
			list = disp.<Statistics>callRemoteMethods(members, "getStatistics", null, null, new RequestOptions(ResponseMode.GET_ALL, timeout));
		} catch (Exception e) {
			logger.error("Query to cluster statistics fail", e);
			return null;
		}
    	Statistics res = new Statistics();
    	for(Address adr: members) {
    		Statistics st = list.getValue(adr);
    		if(st == null) {
    			logger.error(adr + " didn't return statistics");
    			return null;
    		}
    		res.add(st);
    	}
    	return res;
    }

    

    
    public ClusterHealth checkClusterHealth(long timeout) {
    	ClusterHealth info = new ClusterHealth();
    	List<String> unreachableNodes = new ArrayList<String>();
    	//Check buckets distribution
    	Map<Address, byte[]> map = new HashMap<>(); 
    	for(Address adr: members) {
    		try {
    			byte[] buckets = 
    					disp.<byte[]>callRemoteMethod(adr, "getBuckets", null, null, new RequestOptions(ResponseMode.GET_FIRST, timeout));
    			map.put(adr, buckets);
    		} catch (Exception e) {
    			unreachableNodes.add(adr.toString());
    		}		
    	}
    	if(unreachableNodes.size() > 0) {
    		info.state = State.NO_RESPONCE;
    		info.unreachableNodes = unreachableNodes;
    		return info;
    	}
    	byte[] counter = new byte[config.totalBuckets];
    	for(Address adr: members) {
    		byte[] buckets = map.get(adr);
    		for(int i = 0; i < config.totalBuckets; i++) {
    			counter[i] += buckets[i];
    			if(counter[i]> 1) {
    				info.state = State.INTERSECTED;
    			}
    		}
    	}
    	info.bucketsCounter = Arrays.toString(counter);
    	
    	if(info.state != State.OK) {
    		return info;
    	}

    	for(int i = 0; i < config.totalBuckets; i++) {
			if(counter[0] == 0) {
				info.state = State.MISSED_BUCKETS;
				info.currentStateNode = my_addr + "$" + getState();
				return info;
			}
		}

    	//Check load balancer distributed state 
    	String state = getState();
    	for(Address adr: members) {
    		try {
    			String str = disp.<String>callRemoteMethod(adr, "getState", null, null, new RequestOptions(ResponseMode.GET_FIRST, timeout));
    			if(!state.equals(str)) {
    				info.state = State.INCORRECT_STATE;
    				info.currentStateNode = my_addr + "$" + state; 
    				info.incorrectStateNode = adr + "-" + str;
    				break;
    			}
    		} catch (Exception e) {
    			unreachableNodes.add(adr.toString());
    		}		
    	}
    	if(unreachableNodes.size() > 0) {
    		info.state = State.NO_RESPONCE;
    		info.unreachableNodes = unreachableNodes;
    		return info;
    	}

		return info;
	}

}
