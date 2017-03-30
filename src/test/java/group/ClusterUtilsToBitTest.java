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
public class ClusterUtilsToBitTest {

	@Parameters
	public static Collection<Object[]> data() {
		return Arrays.asList(
				new Object[][] { 
						{new byte[]{}, new byte[]{0}}, 
						{new byte[]{1,1,1,1,1,1,1,1}, new byte[]{ (byte) 0b11111111  }},						
						{new byte[]{1,1,0,1,1}, new byte[]{ (byte) 0b11011000  }},
						{new byte[]{1,1,1,1,1,1,1,1,1}, new byte[]{ (byte) 0b11111111, (byte) 0b10000000}},
						{new byte[]{0,0,0,0,0,0,1,0, 1,1,1,1,1,1,1,1 }, new byte[]{ (byte) 0b00000010, (byte) 0b11111111}},
						{new byte[]{1,1,0,0,0,0,1,0, 1,1,0,1,1,1 }, 
						 new byte[]{ (byte) 0b11000010, (byte) 0b11011100}}
				});
	}
	
	byte[] load; 
	byte[] bits;
	
	public ClusterUtilsToBitTest(byte[] load, byte[] bits) {
		this.load = load;
		this.bits = bits;
	}

	
	@Test
	public void test() {
		byte[] toBits = ClusterUtils.toBits(load);
		assertArrayEquals(toBits, bits);
		byte[] fromBits = ClusterUtils.fromBits(bits, load.length);
		assertArrayEquals(load, fromBits);
	}

}
