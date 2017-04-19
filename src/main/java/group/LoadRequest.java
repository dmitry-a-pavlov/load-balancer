package group;

import static group.ClusterUtils.byteToInt;
import static group.ClusterUtils.fromBits;
import static group.ClusterUtils.getBitsSize;
import static group.ClusterUtils.toBits;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
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
		COMMIT, TAKE, RELEASE, ACK, REFUSE, GET_TYPE, SET_TYPE;

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
			case 5:
				return Type.GET_TYPE;
			case 6:
				return Type.SET_TYPE;
			default:
				throw new InstantiationException("ordinal " + ordinal
						+ " cannot be mapped to enum");
			}
		}
	}

	Type type;		//message type
	Type ackType;   //what was confirmed if type == (ACK || COMMIT) 

	int     loadType;	//We can have multiple load type, each of them is balanced separately
	byte[]  load; 	//buckets distribution
	Address owner;  //owner of the load

	public LoadRequest() {
	}

	//for commit/acknowledgment
	public LoadRequest(Type type, int loadType, byte[] load, Address owner, Type ackType) {
		Objects.requireNonNull(type);
		this.type = type;
		this.loadType = loadType;
		this.load = load;
		this.owner = owner;
		this.ackType = ackType;
	}
	
	//for command
	public LoadRequest(Type type, int loadType, byte[] load) {
		this(type, loadType, load, null, null);
	}
	
	//for GET_TYPE
	public LoadRequest(Type type) {
		this(type, 0, null, null, null);
	}

	@Override
	public void writeTo(DataOutput out) throws Exception {
		out.writeByte(this.type.ordinal());
		out.writeInt(this.loadType);
		switch (type) {
		case TAKE:
		case RELEASE:	
		case REFUSE:
			writeLoad(out);
			break;
		case COMMIT:
			writeLoad(out);
			Util.writeAddress(owner, out);
			out.writeByte(this.ackType.ordinal());
			break;
		case ACK:
			out.writeByte(this.ackType.ordinal());
			writeLoad(out);
			break;
		case GET_TYPE:
			break;
		case SET_TYPE:
			writeLoad(out, false);
			break;
		}
	}

	@Override
	public void readFrom(DataInput in) throws Exception {
		this.type = Type.get(in.readByte());
		this.loadType = in.readInt();
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
		case GET_TYPE:
			break;
		case SET_TYPE:			
			this.load = readLoad(in, false);
			break;
		}
	}

	private void writeLoad(DataOutput out) throws IOException {
		writeLoad(out, true);
	}
	
	private void writeLoad(DataOutput out, boolean compress) throws IOException {
		out.writeInt(this.load.length);
		out.write(compress? toBits(this.load): this.load);
	}

	private byte[] readLoad(DataInput in) throws IOException {
		return readLoad(in, true);
	}
	
	private byte[] readLoad(DataInput in, boolean compress) throws IOException {
		int length = in.readInt();
		if(compress) {
			byte[] bits = new byte[getBitsSize(length)];
			in.readFully(bits);
			return fromBits(bits, length);
		} else {
			byte[] bytes = new byte[length];
			in.readFully(bytes);
			return bytes;
		}
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
    	if(loadType != 0) {
    		sb.append('(').append(loadType).append(')');
    	}
    	if(ackType != null) {
    		sb.append(" (ack: " + ackType + ")");
    	}
    	if(load != null && type == Type.SET_TYPE) {
    		sb.append(Arrays.toString(byteToInt(load)));
    	} else if(load != null) {
    		sb.append('[').append(ClusterUtils.toString(load)).append(']');
    	}
    	if(owner != null) {
    		sb.append(" owner: " + owner);
    	}
    	return sb.toString();
    }
}
