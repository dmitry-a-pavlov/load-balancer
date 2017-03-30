package group;

import static group.ClusterUtils.fromBits;
import static group.ClusterUtils.getBitsSize;
import static group.ClusterUtils.toBits;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Objects;

import org.jgroups.Address;
import org.jgroups.Global;
import org.jgroups.util.Streamable;
import org.jgroups.util.Util;

/**
 * Defines communication protocol for load balance algorithm
 * 
 * @author Dmitriy Pavlov
 */
public class LoadRequest implements Streamable {

	//Lexicon
	enum Type {
		COMMIT, TAKE, RELEASE, ACK, REFUSE;

		static Type get(int ordinal) throws InstantiationException {
			switch (ordinal) {
			case 0:
				return Type.COMMIT;
			case 1:
				return Type.TAKE;
			case 2:
				return Type.RELEASE;
			case 3:
				return Type.ACK;
			case 4:
				return Type.REFUSE;
			default:
				throw new InstantiationException("ordinal " + ordinal
						+ " cannot be mapped to enum");
			}
		}
	}

	Type type;		//message type
	Type ackType;   //what was confirmed if type == (ACK || COMMIT) 
	
	byte[] load; 	//buckets distribution
	Address owner;  //owner of the load

	public LoadRequest() {
	}

	//for commit/acknowledgment
	public LoadRequest(Type type, byte[] load, Address owner, Type ackType) {
		Objects.requireNonNull(type);
		this.type = type;
		this.load = load;
		this.owner = owner;
		this.ackType = ackType;
	}
	
	//for command
	public LoadRequest(Type type, byte[] load) {
		this(type, load, null, null);
	}

	@Override
	public void writeTo(DataOutput out) throws Exception {
		out.writeByte(this.type.ordinal());
		switch (type) {
		case TAKE:
		case RELEASE:	
		case REFUSE:
			out.writeInt(this.load.length);
			out.write(toBits(this.load));
			break;
		case COMMIT:
			out.writeInt(this.load.length);
			out.write(toBits(this.load));
			Util.writeAddress(owner, out);
			out.writeByte(this.ackType.ordinal());
			break;
		case ACK:
			out.writeByte(this.ackType.ordinal());
			out.writeInt(this.load.length);
			out.write(toBits(this.load));
			break;
		}
	}

	@Override
	public void readFrom(DataInput in) throws Exception {
		this.type = Type.get(in.readByte());
		switch (type) {
		case TAKE:
		case RELEASE:	
		case REFUSE:
			this.load = readLoad(in);
			break;
		case COMMIT:
			this.load = readLoad(in);
			owner = Util.readAddress(in);
			this.ackType = Type.get(in.readByte());
			break;
		case ACK:
			this.ackType = Type.get(in.readByte());
			this.load = readLoad(in);
			break;
		}
	}

	private byte[] readLoad(DataInput in) throws IOException {
		int length = in.readInt();
		byte[] bits = new byte[getBitsSize(length)];
		in.readFully(bits);
		return fromBits(bits, length);
	}

	final static int SIZE = Global.BYTE_SIZE + Global.INT_SIZE * 3;

	public byte[] toBuffer() {
		ByteBuffer buf = ByteBuffer.allocate(SIZE);
		buf.putInt(type.ordinal());
		buf.put(load);
		return buf.array();
	}

	public void fromBuffer(ByteBuffer buf) throws InstantiationException {
		type = Type.get(buf.getInt());
		buf.get(load);
	}

	@Override
    public String toString() {
    	StringBuilder sb =  new StringBuilder();
    	sb.append(type);
    	if(ackType != null) {
    		sb.append(" (ack: " + ackType + ")");
    	}
    	if(load != null) {
    		sb.append('[').append(ClusterUtils.toString(load)).append(']');
    	}
    	if(owner != null) {
    		sb.append(" owner: " + owner);
    	}
    	return sb.toString();
    }
}
