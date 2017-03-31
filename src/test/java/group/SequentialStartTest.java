package group;

import static org.junit.Assert.assertTrue;
import group.ClusterHealth.State;

import java.util.Arrays;
import java.util.Collection;











import org.junit.runner.RunWith;
import org.junit.runners.MethodSorters;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.junit.FixMethodOrder;
import org.junit.Test;
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
						//{20, 1},
						//{35, 3}, 
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
			clusters[i] = Cluster.builder().totalBuckets(totalBuckets).totalNodes(totalNodes).build();
			clusters[i].start();
		}
		Thread.sleep(3000);

		ClusterHealth health = clusters[0].checkClusterHealth(TIME_OUT);
		logger.info("Cluster health:\n" + health);
		assertTrue("Inconsistent state:\n" + health, health.state == State.OK);
		String state = clusters[0].getState();
		Statistics statistic = clusters[0].getClusterStatistics(TIME_OUT);
	
		for(int i = totalNodes - 1; i >= 0; i--) {
			clusters[i].stop();
		}
		logger.info(state);
		logger.info("Cluster statistics:\n" + statistic);
	}


}
