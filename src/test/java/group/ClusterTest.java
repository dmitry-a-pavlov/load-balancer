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


@FixMethodOrder(MethodSorters.NAME_ASCENDING)
@RunWith(Parameterized.class)
public class ClusterTest {

	@Parameters
	public static Collection<Object[]> data() {
		return Arrays.asList(
				new Object[][] {
						//{20, 1},
						//{20, 3}, 
						{20, 8},
				});
	}

	
	private static final int TIME_OUT = 10000;
	private int totalBuckets;
	private int totalNodes;
    
	public ClusterTest(int numBuckets, int numNodes) {
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
		assertTrue("Inconsistent state: " + health, health.state == State.OK);
		
		for(int i = totalNodes - 1; i >= 0; i--) {
			clusters[i].stop();
		}
	}


}
