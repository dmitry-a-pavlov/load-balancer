package group;

import static org.junit.Assert.assertTrue;
import group.ClusterHealth.State;

import java.util.Arrays;
import java.util.Collection;
import java.util.Set;

import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.MethodSorters;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


@FixMethodOrder(MethodSorters.NAME_ASCENDING)
@RunWith(Parameterized.class)
public class SequentialStartTest {
	protected static final Logger logger = LoggerFactory
			.getLogger(SequentialStartTest.class);


	@Parameters
	public static Collection<Object[]> data() {
		return Arrays.asList(
				new Object[][] {
						{20, 1},
						{35, 3}, 
						{20, 8},
				});
	}

	
	private static final int TIME_OUT = 10000;
	private int totalBuckets;
	private int totalNodes;
    
	public SequentialStartTest(int numBuckets, int numNodes) {
		this.totalBuckets = numBuckets; 
		this.totalNodes = numNodes;
	}
    
    
    
	@Test
	public void test() throws Exception {
		Cluster[] clusters = new Cluster[totalNodes];  
		for(int i = 0; i < totalNodes; i++) {
			clusters[i] = Cluster.builder()
					.totalBuckets(totalBuckets)
					.totalNodes(totalNodes)
					.loadTypes(new int[] {-1, 155})
					.build();
			clusters[i].start();
		}
		Thread.sleep(10000);
		
		Set<Integer> loadTypes = clusters[0].getLoadTypes();
		for(Integer type: loadTypes) {
			ClusterHealth health = clusters[0].checkClusterHealth(type, TIME_OUT);
			logger.info("Cluster health:\n" + health);
			logger.info("Cluster state ({}):\n{}", type, clusters[0].getState(type));
			assertTrue("Inconsistent state:\n" + health, health.state == State.OK);
		}
		Statistics statistic = clusters[0].getClusterStatistics(TIME_OUT);
	
		for(int i = totalNodes - 1; i >= 0; i--) {
			clusters[i].stop();
		}
		logger.info("Cluster statistics:\n" + statistic);
	}


}
