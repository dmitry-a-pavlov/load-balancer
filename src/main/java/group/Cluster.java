package group;

import static group.ClusterUtils.byteToInt;
import static group.ClusterUtils.intToByte;
import static group.LoadRequest.Type.ACK;
import static group.LoadRequest.Type.COMMIT;
import static group.LoadRequest.Type.RELEASE;
import static group.LoadRequest.Type.SET_TYPE;
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
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
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
	
	private final Statistics stat; 

	/**
	 * Shared state {loadType, ClasterLoad}.
	 * The major problem is to provide consistency of the shared state 
	 * among all nodes of the cluster. 
	 */
	private Map<Integer, ClusterLoad> theLoads = new HashMap<>(); 
	private Map<Integer, ClusterLoad> newLoads = new HashMap<>(); // pending state

	private Map<Integer, byte[]> buckets = new HashMap<>();    // current load
	private Map<Integer, byte[]> newBuckets = new HashMap<>(); // new unconfirmed load

	// wait for a half node or specified number of nodes to be started 
	// before calculate load
	private boolean election = true;
	
	/**
	 * counts numbers of request for release buckets
	 * used to reduce number of recalculation
	 * The main idea to wait all buckets before distribute them among active nodes.
	 * 
	 * Most important: DO NOT distribute free bucket before it will be released!
	 * 
	 * @protectedBy theLoads
	 */
	private ConcurrentHashMap<Integer, Integer> releaseCounter = new ConcurrentHashMap<>();
	
	private ConcurrentHashMap<Integer, Long>  calculateLoadLastTime = new ConcurrentHashMap<>(); //in nanoseconds
	
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
		stat = new Statistics(cfg);
		for(int i =0; i < cfg.loadTypes.length; i++) { //my load
			int loadType = cfg.loadTypes[i];
			buckets.put(loadType, new byte[config.totalBuckets]);
			newBuckets.put(loadType, new byte[config.totalBuckets]);
		}
	}

	private void initLoadIfAbsent(Builder cfg) {
		for(int loadType: cfg.loadTypes) {
			initLoadIfAbsent(loadType, my_addr);
		}
	}

	/**
	 * I will initialize the load for <owner>
	 * @param loadType - load type
	 * @param owner - load owner
	 */
	private void initLoadIfAbsent(int loadType, Address owner) {
		if(owner == null)
			return;
		
		ClusterLoad theLoad = theLoads.get(loadType);
		if(theLoad == null) {
			theLoad = new ClusterLoad(config.totalBuckets, loadType);
			theLoads.put(loadType, theLoad);
		}
		theLoad.initIfAbsent(owner);
	}

	public void start() {
		try {
			serviceSheduler = Executors.newScheduledThreadPool(1);
			channel = new JChannel();
			channel.setReceiver(this);
			disp = new RpcDispatcher(channel, this, this, this);
			
			channel.connect(config.clusterName);
			my_addr = channel.getAddress();
			channel.getState(null, 10000);
		} catch (Exception e) {
			stop();
			throw new ClusterException("Cannot get cluster state");
		}
	}

	public void stop() {
		channel.disconnect();
		serviceSheduler.shutdown();
	}

	/**
	 * format: loadType1#adr1=1,2;adr2=3,5;...;\n
	 * 		   loadType2#adr1=5,6;adr2=3,5;...;\n
	 * 		   ....	
	 * 		   loadTypeN#adr1=5,6;adr2=3,5;...;\n
	 */
	@Override
	public void getState(OutputStream output) throws Exception {
		synchronized (theLoads) {
			StringBuilder clusterLoad = new StringBuilder(); 
			for(int loadType: theLoads.keySet()) {
				ClusterLoad theLoad = theLoads.get(loadType);
				String load = theLoad.getState();
				clusterLoad.append(loadType).append('#').append(load).append('\n');
			}
			Util.objectToStream(clusterLoad.toString(), new DataOutputStream(output));
		}
	}

	/**
	 * @see getState
	 */
	@Override
	public void setState(InputStream input) throws Exception {
		Object new_state = null;
		try {
			new_state = Util.objectFromStream(new DataInputStream(input));
		} catch (Exception e) {
			logger.error("setState:", e);
		}
		if (!(new_state instanceof String)) {
			return;
		}

		synchronized (theLoads) {
			theLoads.clear();
			String clusterLoad = (String) new_state;
			String[] typeLoads = clusterLoad.split("\n");
			for(String typeLoad: typeLoads) {
				if(ClusterUtils.isEmpty(typeLoad)) 
					continue;
				String[] str = typeLoad.split("#");
				if(str.length !=2) 
					continue;
				int loadType = Integer.parseInt(str[0]);
				ClusterLoad load = new ClusterLoad(config.totalBuckets, loadType);
				load.parceState(str[1]);
				theLoads.put(loadType, load);
			}
			initLoadIfAbsent(config); //add my load if needed
		}

	}

	@Override
	public void suspect(Address mbr) {
		super.suspect(mbr);
		logger.info("Crashed: {}", mbr.toString());
	}
	
	@Override
	public void viewAccepted(View view) {
		synchronized (theLoads) {
			List<Address> joined = new LinkedList<Address>();
			List<Address> left = new LinkedList<Address>();
			setInternalState(view.getMembers(), joined, left);
			my_addr = channel.getAddress();
			
			if(joined.contains(my_addr)) { //just started
				sendCommand(SET_TYPE, null, 0, intToByte(config.loadTypes)); // to All
			}

			if(leader) {
				//some members have gone
				Set<Integer> types = new HashSet<Integer>();
				for(Address adr: left) {
					types.addAll(getTypes(adr));
				}
				for(Integer type: types) {
					//here is expected to distribute buckets from the failed nodes
					//probably we need to wait that some node will release buckets => 2 parameter is false  
					calculateClusterLoad(type, false);  
				}
				return;
			} 

			//I am not a leader
			if(members.size() > config.nodesToWait) {
				for(Address mbr: members) {
					if(!joined.contains(mbr) && getTypes(mbr).isEmpty()) { 
						logger.warn("Old members with no load found: " + mbr);
						//recoveState(mbr);
					}
				}
			}
		} //synchronized member
	}

	@SuppressWarnings("unused")
	private void recoveState(Address mbr) {
		try {  
			if(my_addr.equals(mbr)) {
				//sendCommand(SET_TYPE, null, 0, intToByte(config.loadTypes));
				channel.getState(null, 10000);
			}
		} catch (Exception e) {
			throw new ClusterException("Cannot get cluster state");
		}
	}
	
	/**
	 * I find all types for the given node.
	 * @param node
	 */
	private Set<Integer> getTypes(Address node) {
		Set<Integer> types = new HashSet<Integer>(); 
		for (Integer type : theLoads.keySet()) {
			ClusterLoad load = theLoads.get(type);
			if (load.hasLoad(node)) {
				types.add(type);
			}
		}
		return types;
	}


	/**
	 * The main method to calculate the cluster load
	 * The only leader is eligible to calculate the cluster load!
	 */
	private void calculateClusterLoad(int loadType, boolean forcibly) {
		if (!leader) {
			return;
		}
		synchronized (theLoads) {
			stat.calculated++;
			if(!forcibly && getCounter(loadType, 0) > 0) {
				//Wait for a follower to release its buckets  
				logger.debug("Wait acknowledgment for RELESE request");
				return; 
			}
			ClusterLoad theLoad = theLoads.get(loadType);
			List<Address> members = theLoad.getMembers(loadType, this.members);
			theLoad.cleanLoad(members); //clean the load only before calculation
			releaseCounter.put(loadType, 0);
			calculateLoadLastTime.put(loadType, System.nanoTime());
			ClusterLoad newLoad = (ClusterLoad) theLoad.clone(); //based on current load
			newLoads.put(loadType, newLoad);
			logger.info("Calculate cluster load for type: {}", loadType);
			if(logger.isTraceEnabled())
				logger.trace("Cluster state:\n{}", newLoad);
			for (Address addr : members) {
				byte[] load = theLoad.getLoad(addr); //current load
				byte[] new_load = newLoad.calcNewLoad(load, addr, members);
				if (new_load == null) {
					continue; // no changes
				}
				int leftCount = ClusterUtils.countLeftBuckets(load, new_load);
				Type command = leftCount > 0? RELEASE: TAKE;
				
				//short cut for the leader node, no need in extra acknowledgment communication
				if (my_addr.equals(addr)) {
					new_load = (command == RELEASE)? releaseLeaderLoad(loadType, members, newLoad, load, new_load): new_load;
					newBuckets.put(loadType, new_load);
					newLoad.putLoad(my_addr, new_load);
					sendCommit(my_addr, loadType, new_load, my_addr, command); //RELEASE and TAKE can be committed 
					continue;
				}
				int counter = releaseCounter.get(loadType);
				if(command == RELEASE) {
					releaseCounter.put(loadType, ++counter);
				}
				if(counter > 0 && command == TAKE) {
					continue; //it have to wait before buckets will be released   
				}
				newLoad.putLoad(addr, new_load); //will check acknowledgment
				sendCommand(command, addr, loadType, new_load);
			}
			if(releaseCounter.get(loadType) > 0) { 
				scheduleTimeout(loadType);
			}
		}
	}

	private Integer getCounter(int loadType, int defVal) {
		Integer counter = releaseCounter.get(loadType);
		return counter != null? counter: defVal;
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
	 * @param loadType - load type
	 */
	private void scheduleTimeout(final int type) {
		Runnable dogwatch = new Runnable() {
		  final long lastTime = Cluster.this.calculateLoadLastTime.get(type);
		  final int loadType = type;
		  @Override
		  public void run() {
			if((lastTime == Cluster.this.calculateLoadLastTime.get(loadType)) //no recalculation happened since dog watch creation
				&& releaseCounter.get(loadType) > 0) { // some release request was not committed timeout 
				calculateClusterLoad(loadType, true);
				logger.debug("Load recalculation by timeout: loadType=" + loadType);
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
			if(logger.isTraceEnabled())
				logger.debug("\n>-----{}\n>-----{}", msg.toString(), req);
			switch (req.type) {
			case COMMIT:// message sent from a leader to followers & leader itself
				if(leader && msg.getDest() == null) { //the leader has its personal commit, it has to ignore public commit
					break; //skip public commit (commit to ALL), do not send it again
				}
				synchronized (theLoads) {
					byte[] new_buckets = newBuckets.get(req.loadType);
					if (my_addr.equals(req.owner) && Arrays.equals(req.load, new_buckets)) //I am an owner of the load
					{ 
						switch (req.ackType) {
						case RELEASE:
							buckets.put(req.loadType, new_buckets);
							break;
						case TAKE:
							byte[] my_buckets = buckets.get(req.loadType);
							takeLoad(req.loadType, my_buckets, new_buckets); //take real load
							buckets.put(req.loadType, new_buckets);
							break;
						default:
							throw new ClusterException("Illigal command to commit: " + req.ackType);
						}
					}
					initLoadIfAbsent(req.loadType, req.owner);
					
					//first - update the common state, 
					ClusterLoad theLoad = theLoads.get(req.loadType);
					if(!leader) {
						theLoad.putLoad(req.owner, req.load);
						break;
					}
					
					//I am the leader
					//only the leader is eligible to calculate load and send public commit 
					
					ClusterLoad newLoad = newLoads.get(req.loadType);
					byte[] new_load = newLoad.getLoad(req.owner);
					// double check for consistency, if it is not recalculated we can update 
					if(newLoad != null && Arrays.equals(new_load, req.load)) {
						theLoad.putLoad(req.owner, req.load);
						
						if( (req.ackType == Type.RELEASE) &&  //got free buckets => can recalculate state!
							!req.owner.equals(my_addr))       //no needs to calculate twice, commit is sent by leader while recalculation
						{
							int counter = releaseCounter.get(req.loadType);
							releaseCounter.put(req.loadType, --counter); 	//protected by 'theLoads'
							if(counter == 0) 								//all response received 
								calculateClusterLoad(req.loadType, true);
							
						}
					
						//it was commit for leader => leader state was updated 
						//then it can spread the load to others
						sendCommit(null, req.loadType, req.load, req.owner, req.ackType); // message to ALL: to update their state
					}						
				}
				break;
			case REFUSE:// message for the leader
				synchronized (theLoads) {
					//recalculate load for the given type
					ClusterLoad theLoad = theLoads.get(req.loadType);
					ClusterLoad newLoad = newLoads.get(req.loadType);
					byte[] load = theLoad.getLoad(msg.getSrc()); //current load of the refusing follower!
					byte[] busyBuckets = req.load;
					if (ClusterUtils.markBusy(load, busyBuckets) == 0) {
						throw new ClusterException("Must have busy buckets");
					}
					stat.refused++;
					byte[] new_load = newLoad.calcNewLoad(load, msg.getSrc(), members);
					if(new_load == null) {
						newLoad.removeLoad(msg.getSrc());
						break;
					}
					
					int leftCount = ClusterUtils.countLeftBuckets(load, new_load);
					Type command = leftCount > 0? RELEASE: TAKE;
					if(command == RELEASE) {
						newLoad.putLoad(msg.getSrc(), new_load);
						sendCommand(Type.RELEASE, msg.getSrc(), req.loadType, new_load);
					}
					//TAKE command has to be sent only from calculateClusterLoad() 
				}
				break;
			case TAKE: // for follower from a leader
				synchronized (theLoads) {
					byte[] my_buckets = buckets.get(req.loadType);
					byte[] added = ClusterUtils.getAddedBuckets(my_buckets, req.load);
					logger.info("Load to be taken for ({}): [{}]", req.loadType, ClusterUtils.toString(added));
					if(ClusterUtils.count(added) == 0) {
						logger.warn("New load is a subset of current load ({}): [{}]", req.loadType, ClusterUtils.toString(req.load));
					    break;
					}
					newBuckets.put(req.loadType, req.load);
					sendAck(Type.TAKE, msg.getSrc(), req.loadType, req.load); // to a leader
				}
				break;
			case RELEASE:// message for follower
				synchronized (theLoads) { //to support 'refuse' process we need to try release buckets here (not in 'commit' stage) 
					byte[] my_buckets = buckets.get(req.loadType);
					byte[] left = ClusterUtils.getLeftBuckets(my_buckets, req.load);
					logger.info("Load to be released for ({}): [{}]", req.loadType, ClusterUtils.toString(left));
					if(ClusterUtils.count(left) == 0) {
						logger.warn("New load has no buckets to release ({}) new load {} current load : [{}]", 
								req.loadType,
								ClusterUtils.toString(req.load),
								ClusterUtils.toString(my_buckets));
					    break;
					}
					synchronized (config.releaseLock) {				
						byte[] busy = releaseLoad(req.loadType, my_buckets, req.load);
						if (ClusterUtils.count(busy) > 0) {
							// send to a leader - need recalculate the load
							sendCommand(Type.REFUSE, msg.getSrc(), req.loadType, busy);
							return;
						}
					}
					//Here we are sure - there are some free buckets in the cluster
					newBuckets.put(req.loadType, req.load);
					sendAck(Type.RELEASE, msg.getSrc(), req.loadType, req.load); // to a leader
				}
				break;
			case ACK:// message for the leader: change is confirmed (only a leader can check the acknowledgement)
				synchronized (theLoads) {
					ClusterLoad newLoad = newLoads.get(req.loadType);
					byte[] new_load = newLoad.getLoad(msg.getSrc());
					// conversation filter:
					// if newLoads has been changed by a call calculateClusterLoad(...)
					// the request have to be ignored
					if(newLoad != null && Arrays.equals(new_load, req.load)) {
						sendCommit(my_addr, req.loadType, new_load, msg.getSrc(), req.ackType); // message to the leader: to update its state 
					}
				}
				break;
			case GET_TYPE: //request for supported load types (sent by leader)
				sendCommand(Type.SET_TYPE, null, 0, intToByte(config.loadTypes)); // to All
				break;
			case SET_TYPE: 			
				synchronized (theLoads) {			
					int[] types = byteToInt(req.load);
					for (int type = 0; type < types.length; type++) {
						initLoadIfAbsent(types[type], msg.getSrc());
					}
					if(leader) {
						if (election && (members.size() < config.nodesToWait)) {
							return; // new load => wait for quorum
						}
						Set<Integer> typeSet = new HashSet<>();
						if(election) {
							typeSet = getLoadTypes(); //have to recalculate all load types at first time
							election = false;
						} else for (int i = 0; i < types.length; i++) {
							typeSet.add(types[i]);	  //only new node's load have to be recalculated 
						}
						
						for (int type:  typeSet) {
							calculateClusterLoad(type, false);
						}
					}
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
	 * @param loadType 
	 * @param members2 
	 * @param theLoad 
	 * @protectedBy theLoads
	 * 
	 * @param old_load - current load
	 * @param newLoad - newly calculated load
	 * @return new load, probably recalculated
	 */
	private byte[] releaseLeaderLoad(int loadType, List<Address> members, ClusterLoad newLoad, byte[] old_load, byte[] new_load) {
		if(!leader) {
			return null;
		}
		synchronized (config.releaseLock) {
			byte[] busy = releaseLoad(loadType, old_load, new_load);
			if (ClusterUtils.markBusy(old_load, busy) > 0) {
				new_load = newLoad.calcNewLoad(old_load, my_addr, members);
				releaseLoad(loadType, old_load, new_load);
			}
		}
		byte[] left = ClusterUtils.getLeftBuckets(old_load, new_load);
		logger.info("Leader released the load for ({}): {} - left buckets", loadType, ClusterUtils.toString(left));
		return new_load;
	}

	/**
	 * Threads and resources must be prepared here to process load, but
	 * processing have to start only after this.buckets get load
	 * 
	 * @param added - buckets to be taken
	 */
	private void takeLoad(int loadType, byte[] oldLoad, byte[] newLoad) {
		stat.taken++;
		if(config.takeLoad != null) {
			config.takeLoad.changeLoad(oldLoad, newLoad);
		}
		logger.info("Load is taken for ({}): [{}]", loadType, ClusterUtils.toString(newLoad));
	}

	
	/**
	 * Verify: is it possible to release real load.
	 * Must release either ALL or nothing, ONLY a leader must recalculate load.
	 * Potentially can get a starvation - when trying to release 1 bucket from two
	 * which can be always busy, but in this case the node must wait and then release resources
	 * when they are free.
	 * 
	 * The Node responsibilities is to wait when resources will be free and can be released. 
	 * @param loadType 
	 *  
	 * @param newLoad - the new load
	 * @return return array with busy buckets !ONLY!, can return 'null' what means no busy buckets found.    
	 */
	private byte[] releaseLoad(int loadType, byte[] oldLoad, byte[] newLoad) {
		stat.released++;
		if(config.releaseLoad == null) {
			return null;
		}
		byte[] busy = null;
		try {
			busy = config.releaseLoad.changeLoad(oldLoad, newLoad);
			logger.info("Load is released for ({}): [{}]", loadType, ClusterUtils.toString(newLoad));
		} catch (Exception e) {
			logger.error("Fail to release buckets", e);
			return ClusterUtils.getLeftBuckets(oldLoad, newLoad); //refuse to release all buckets
		}
		return busy;
	}

	public List<Address> setInternalState(List<Address> newMembers, List<Address> joined, List<Address> left) {
		for (Address mbr: newMembers)
			if(!members.contains(mbr))
				joined.add(mbr);
		
		for (Address mbr: members)
			if(!newMembers.contains(mbr))
				left.add(mbr);
		
		
		members.clear();
		for (Address mbr : newMembers)
			addNode(mbr);
		leader = newMembers.size() <= 1
				|| (newMembers.size() > 1 && newMembers.iterator().next().equals(my_addr));
		return joined; 
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
		sb.append("Cluster load:\n").append(theLoads.toString());
		sb.append('\n');
		sb.append("Current load:\n").append('[');
		for (Integer type: buckets.keySet()) {
			byte[] my_buckets = buckets.get(type);
			sb.append(type).append('=');
			sb.append(ClusterUtils.toString(my_buckets));
			sb.append('\n');
		}
		sb.append(']');
		return sb.toString();
	}

	//~ Communication -----------------------------------------------------------------
	
	public void send(Address dest, LoadRequest request) {
		try {
			byte[] buffer = Util.streamableToByteBuffer(request);
			logger.debug("{} to {} from {}", request, (dest == null? "ALL": dest), my_addr);
			channel.send(new Message(dest, my_addr, buffer));
		} catch (Exception e) {
			throw new ClusterException("Cannot send message", e);
		}
	}
	
	public void sendCommand(Type command, Address dest, int loadType, byte[] load) {
		send(dest, new LoadRequest(command, loadType, load));
	}

	/**
	 * Acknowledgement.
	 * The goal is to be sure a new load is received and common state is the same.
	 * @param ackType - acknowledge type
	 * @param dest - destination address
	 * @param newLoad - new load
	 */
	private void sendAck(Type ackType, Address dest, int loadType, byte[] newLoad) {
		send(dest, new LoadRequest(ACK, loadType, newLoad, null, ackType));
	}

	/**
	 * Confirmation, will be sent to <dest> nodes
	 * @param loadType - type of the load
	 * @param load - pay load for <owner> 
	 * @param owner - owner of the load
	 * @param ackType - what is committed
	 */
	public void sendCommit(Address dest, int loadType, byte[] load, Address owner, Type ackType) {
		send(dest, new LoadRequest(COMMIT, loadType, load, owner, ackType));
	}
	
    //~ --- STATIC CLASSES ---
    public static class Builder {
    	
    	/**
    	 * Each number in the array denotes some type of load.
    	 */
    	int[] loadTypes = new int []{-1};

		/** 
    	 * Number of nodes to be wait before starting load calculation
    	 * If it is -1 it will be assigned in accordance to the totalNodes as a QUORUM
    	 * for a leader election (half of the nodes in the system)
    	 * 
    	 * default is -1 - wait for QUORUM
    	 */
    	int nodesToWait = -1;

		/** Total number of buckets in the cluster */
    	int totalBuckets = 20;
    	
    	/** Expected number of nodes in the cluster */
    	int totalNodes = 1;
    	
    	/** recalculation timeout in milliseconds */
    	int recalLoadTimeout = 5000; //milliseconds;
    	
    	/** Callback for release load */
    	ChangeLoad releaseLoad; 
    	
    	/**Callback for take load*/
    	ChangeLoad takeLoad;
    	
    	/** this lock should synchronize resources before to be released */
    	Object releaseLock = new Object();
    	
    	/** Name of the cluster */
    	String clusterName;

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
    		return this;
    	}
    	
    	/**
    	 * I set how many nodes must be waited before start load distribution
    	 * @param num - -1 means set QUORUM
    	 * @return
    	 */
    	public Builder nodesToWait(int num) {
    		nodesToWait = num;
    		if(nodesToWait == -1) { 
     		   nodesToWait = totalNodes / 2 + 1;
     		}
    		return this;
    	}
    	
    	public Builder totalNodes(int num) {
    		totalNodes = num;
    		if(nodesToWait == -1) { 
    		   //goal is to reduce the number of load recalculation 
     		   nodesToWait = totalNodes / 2 + 1;
     		}
    		return this;
    	}
    	
    	public Builder recalLoadTimeout(int milli) {
    		recalLoadTimeout = milli;
    		return this;
    	}
    	
    	public Builder loadTypes(int[] types) {
    		loadTypes = types;
    		return this;
    	}
    	
        public Builder clusterName(String name) {
            clusterName = name;
            return this;
        }
    	
    	public Cluster build() {
    		if(ClusterUtils.isEmpty(clusterName)) {
    			throw new ClusterException("Cluster name must be specified");
    		}
    		return new Cluster(this); 
    	}
    	
    }
    
    //~ Cluster state validation --------------------------------------------------------------
    //  the methods are remotely called  
    public String getState(Integer loadType) {
    	ClusterLoad theLoad = theLoads.get(loadType);
    	StringBuilder sb = new StringBuilder(100);
    	for(Address address: members) {
			sb.append(address).append("=");
			sb.append(ClusterUtils.toString(theLoad.getLoad(address)));
			sb.append(";");
    	}
		return sb.toString();
    }
	
    public byte[] getBuckets(Integer loadType) {
    	return buckets.get(loadType);
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
			list = disp.<Statistics>callRemoteMethods(members, "getStatistics", null, null, 
													  new RequestOptions(ResponseMode.GET_ALL, timeout));
		} catch (Exception e) {
			logger.error("Query to cluster statistics fail", e);
			return null;
		}
    	Statistics res = new Statistics(config, getLoadTypes().toString());
    	for(Address adr: members) {
    		Statistics st = list.getValue(adr);
    		if(st == null) {
    			logger.warn(adr + " didn't return statistics");
    			continue;
    		}
    		res.add(st);
    	}
    	return res;
    }

    public Statistics getClusterStatistics_first(long timeout) {
    	Statistics res = new Statistics(config, getLoadTypes().toString());
    	for(Address adr: members) {
	    	try {
	    		Statistics stat = 
    					disp.<Statistics>callRemoteMethod(adr, "getStatistics", 
    							null, null, 
    							new RequestOptions(ResponseMode.GET_FIRST, timeout));
	    		res.add(stat);
			} catch (Exception e) {
				logger.error("Query to cluster statistics fail {}, {}", adr, e.getMessage());
				return null;
			}
    	}
    	return res;
    }

    
    public ClusterHealth checkClusterHealth(Integer loadType, long timeout) {
    	synchronized (theLoads) {
	    	ClusterHealth info = new ClusterHealth(loadType);
	    	List<String> unreachableNodes = new ArrayList<String>();
	    	
	    	if(members.size() != config.totalNodes) {
	    		info.message = "Expected nodes: " + config.totalNodes + "Started: " + members.size(); 
	    	}
	    	
	    	//Check buckets distribution
	    	Map<Address, byte[]> map = new HashMap<>(); 
	    	for(Address adr: members) {
	    		try {
	    			byte[] buckets = 
	    					disp.<byte[]>callRemoteMethod(adr, "getBuckets", 
	    							new Object[]{loadType}, new Class[] {Integer.class}, 
	    							new RequestOptions(ResponseMode.GET_FIRST, timeout));
	    			map.put(adr, buckets);
	    		} catch (Exception e) {
	    			unreachableNodes.add(adr.toString());
	    		}		
	    	}
	    	if(unreachableNodes.size() > 0) {
	    		info.state = State.NO_RESPONSE;
	    		info.unreachableNodes = unreachableNodes;
	    		return info;
	    	}
	    	byte[] counter = new byte[config.totalBuckets];
	    	for(Address adr: members) {
	    		byte[] buckets = map.get(adr);
	    		if(ClusterUtils.count(buckets) == 0) {
	    			//double check
	    			try {
						buckets = 
								disp.<byte[]>callRemoteMethod(adr, "getBuckets", 
										new Object[]{loadType}, new Class[] {Integer.class}, 
										new RequestOptions(ResponseMode.GET_FIRST, timeout));
						map.put(adr, buckets);
					} catch (Exception e) {
						unreachableNodes.add(adr.toString());
					}
	    			if(ClusterUtils.count(buckets) == 0) {	
	    				info.state = State.NO_BUCKETS;
	    				info.incorrectStateNode = adr.toString();
	    				return info;
	    			}
	    		}
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
				if(counter[i] == 0) {
					info.state = State.MISSED_BUCKETS;
					info.currentStateNode = my_addr + "$" + getState(loadType);
					return info;
				}
			}
	
	    	//Check load balancer distributed state 
	    	String state = getState(loadType);
	    	for(Address adr: members) {
	    		try {
	    			String str = disp.<String>callRemoteMethod(adr, "getState", 
	    					new Object[]{loadType}, new Class[] {Integer.class}, 
	    					new RequestOptions(ResponseMode.GET_FIRST, timeout));
	    			if(!state.equals(str)) {
	    				info.state = State.INCORRECT_STATE;
	    				info.currentStateNode = my_addr + "$" + state; 
	    				info.incorrectStateNode = adr + "$" + str;
	    				break;
	    			}
	    		} catch (Exception e) {
	    			unreachableNodes.add(adr.toString());
	    		}		
	    	}
	    	if(unreachableNodes.size() > 0) {
	    		info.state = State.NO_RESPONSE;
	    		info.unreachableNodes = unreachableNodes;
	    		return info;
	    	}
	    	return info;
    	}	
	}

	public Set<Integer> getLoadTypes() {
		return theLoads.keySet();
	}

	public String getClusterName() {
		return channel.getClusterName();
	}

	public Builder getConfig() {
		return config;
	}

}
