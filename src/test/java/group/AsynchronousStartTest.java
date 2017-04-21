package group;

import static org.junit.Assert.assertTrue;
import group.ClusterHealth.State;

import java.util.Arrays;
import java.util.Collection;
import java.util.Set;
import java.util.concurrent.CountDownLatch;

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
public class AsynchronousStartTest {
	protected static final Logger logger = LoggerFactory
			.getLogger(AsynchronousStartTest.class);


	@Parameters
	public static Collection<Object[]> data() {
		return Arrays.asList(
				new Object[][] {
						{20, 1, -1},
						{35, 3, -1}, 
						{20, 8, 1},
						{20, 8, -1},
						{20, 8, 8},
						//{60, 30, 1},
						//{120, 60, -1},
				});
	}

	
	private static final int TIME_OUT = 10000;
	private int totalBuckets;
	private int totalNodes;
	private int nodesToWait;
	
	private CountDownLatch startGate = null;
	private CountDownLatch endGate = null;
	private CountDownLatch readyGate = null;

    
	public AsynchronousStartTest(int numBuckets, int numNodes, int nodesToWait) {
		this.totalBuckets = numBuckets; 
		this.totalNodes = numNodes;
		this.nodesToWait = nodesToWait;
	}
    
    
    
	@Test
	public void test() throws Exception {
		this.startGate = new CountDownLatch(1);
		this.readyGate = new CountDownLatch(totalNodes);
		this.endGate = new CountDownLatch(totalNodes);
		
		NodeThread[] threads = new NodeThread[totalNodes];
		for(int i = 0; i < totalNodes; i++) {
			threads[i] = new NodeThread(i + 1);
			threads[i].start();
		}
		this.readyGate.await(); //wait for all threads
		long clusterStart = System.nanoTime();
		this.startGate.countDown();
		this.endGate.await();
		long elapsedStartTime = System.nanoTime() - clusterStart;
		Thread.sleep(10000);
		Set<Integer> loadTypes = threads[0].cluster.getLoadTypes();
		Statistics statistic = threads[0].cluster.getClusterStatistics(TIME_OUT);
		for(Integer type: loadTypes) {
			ClusterHealth health = threads[0].cluster.checkClusterHealth(type, TIME_OUT);
			logger.info("Cluster health:\n" + health);
			logger.info("Cluster state ({}):\n{}", type, threads[0].cluster.getState(type));
			assertTrue("Inconsistent state:\n" + health, health.state == State.OK);
		}
		logger.info("------------------------------------------------------------");
		for(int i = totalNodes - 1; i >= 0; i--) {
			threads[i].cluster.stop();
		}
		
		long time = 0;
		for(int i = totalNodes - 1; i >= 0; i--) {
			time += threads[i].elapsed;
		}
		time = time/totalNodes;
		logger.info("Avereage node start time:\t" + time/1000000000);
		logger.info("Cluster start time:\t" + elapsedStartTime/1000000000);
		logger.info("Cluster statistics:\n" + statistic);
		
	}


	
	
	final private class NodeThread extends Thread {

		Cluster cluster;
		long elapsed = 0;

		NodeThread(int no) {
			setName("NodeThread" + no);
			cluster = Cluster.builder()
					.totalBuckets(AsynchronousStartTest.this.totalBuckets)
					.totalNodes(AsynchronousStartTest.this.totalNodes)
					.nodesToWait(AsynchronousStartTest.this.nodesToWait)
					.loadTypes(new int[] {-1, 155})
					.build();
		}

		@Override
		public void run() {
			ready();
			await();
			try {
				final long start = System.nanoTime();
				cluster.start();
				elapsed = (System.nanoTime() - start);
			} finally {
				finish();
			}
		}
		
		private void ready() {
			if (readyGate == null)
				return;
			readyGate.countDown();
		}

		private void finish() {
			if (endGate == null)
				return;
			endGate.countDown();
		}

		private void await() {
			if (startGate == null)
				return;
			try {
				startGate.await();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}

}
