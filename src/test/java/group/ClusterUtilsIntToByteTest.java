package group;

import static org.junit.Assert.*;
import group.ClusterUtils;

import java.util.Arrays;
import java.util.Collection;

import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.MethodSorters;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
@RunWith(Parameterized.class)
public class ClusterUtilsIntToByteTest {

	@Parameters
	public static Collection<Object[]> data() {
		return Arrays.asList(
				new Object[][] { 
						{null},
						{new int[]{}}, 
						{new int[]{-1}},
						{new int[]{Integer.MAX_VALUE, Integer.MIN_VALUE}},
						{new int[]{1245,-1,3321,41,251,15,1,1453654}},						
						{new int[]{1,2,3,4,5,6,7,8,9,10,11,12,13,-12131314}},
				});
	}
	
	int[] array; 
	
	public ClusterUtilsIntToByteTest(int[] array) {
		this.array = array;
	}

	
	@Test
	public void test() {
		byte[] toBytes = ClusterUtils.intToByte(array);
		int[] fromBytes = ClusterUtils.byteToInt(toBytes);
		assertArrayEquals(fromBytes, array);
	}

}
