package system;

import group.Cluster;
import group.ClusterHealth;
import ninja.lifecycle.Dispose;
import ninja.lifecycle.Start;
import ninja.utils.NinjaProperties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.inject.Inject;
import com.google.inject.Singleton;

@Singleton
public class SysCluster {

	protected static final Logger logger = LoggerFactory
			.getLogger(SysCluster.class);

	private NinjaProperties properties;
	
	private final Cluster cluster; 




	// ~constructors ------------------------------------------------
	
	@Inject
	public SysCluster(NinjaProperties ninjaProperties) { //TODO get rid of NinjaProperties 
		properties = ninjaProperties;
		
		int TOTAL_BUCKETS = properties.getIntegerWithDefault("cluster.total_buckets", 20);
		int TOTAL_NODES = properties.getIntegerWithDefault("cluster.total_nodes", 1);
		
		cluster = Cluster.builder()
				.totalBuckets(TOTAL_BUCKETS)
				.totalNodes(TOTAL_NODES).loadTypes(new int[]{-1, 155})
				.build();
	}

	@Start(order = 1)
	public void startCluster() {
		cluster.start();
	}

	@Dispose(order = 100)
	public void stopCluster() {
		cluster.stop();
	}
	
	@Override
	public String toString() {
		return cluster.toString();
	}

	public ClusterHealth checkClusterHealth(Integer type) {
		return cluster.checkClusterHealth(type, 5000);
	}

}
