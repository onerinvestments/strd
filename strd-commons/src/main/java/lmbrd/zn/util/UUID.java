/*
 * uuid.java
 * 
 * Created 07.02.2003
 *
 * eaio: uuid - an implementation of the uuid specification
 * Copyright (c) 2003-2008 Johann Burkard (jb@eaio.com) http://eaio.com.
 * 
 * Permission is hereby granted, free of charge, to any person obtaining a
 * copy of this software and associated documentation files (the "Software"),
 * to deal in the Software without restriction, including without limitation
 * the rights to use, copy, modify, merge, publish, distribute, sublicense,
 * and/or sell copies of the Software, and to permit persons to whom the
 * Software is furnished to do so, subject to the following conditions:
 * 
 * The above copyright notice and this permission notice shall be included
 * in all copies or substantial portions of the Software.
 * 
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS
 * OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN
 * NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM,
 * DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR
 * OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE
 * USE OR OTHER DEALINGS IN THE SOFTWARE.
 * 
 */
package lmbrd.zn.util;

import com.eaio.util.lang.Hex;
import com.eaio.uuid.UUIDGen;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;

/**
 * Creates UUIDs according to the DCE Universal Token Identifier specification.
 * <p/>
 * UUIDs of this class are suitable as primary keys for EJB Entity Beans. You
 * will need to add the two following methods to your CMP Entity Beans:
 * <pre>
 * public abstract long getTime();
 * public abstract void setTime(long time);
 *
 * public abstract long getClockSeqAndNode();
 * public abstract void setClockSeqAndNode(long clockSeqAndNode);
 * </pre>
 * <p/>
 * Don't forget to set com.eaio.uuid.UUID as the primary key in your
 * descriptor and to initialize the primary key in your bean by calling
 * <pre>
 * public UUID ejbCreate(...) throws CreateException {
 *  setTime(UUIDGen.newTime());
 *  setClockAndNode(UUIDGen.getClockSeqAndNode());
 *  return null;
 * }
 * </pre>
 * <p/>
 * For usage with the Sun Application Server, you must comments out the
 * <code>serialVersionUID</code> field and recompile the sources.
 *
 * @author <a href="mailto:jb@eaio.de">Johann Burkard</a>
 * @version $Id$
 * @see <a href="http://www.opengroup.org/onlinepubs/9629399/apdxa.htm">
 *      http://www.opengroup.org/onlinepubs/9629399/apdxa.htm
 *      </a>
 * @see <a href="http://www.uddi.org/pubs/draft-leach-uuids-guids-01.txt">
 *      http://www.uddi.org/pubs/draft-leach-uuids-guids-01.txt
 *      </a>
 */

public class UUID implements Comparable<UUID>, Serializable, Cloneable {

	/**
	 * If you are working with the Sun One Application Server and you are using
	 * this class as primary keys for EJBs, you must comments this field out.
	 * <p/>
	 * This applies to version 7.
	 */
	static final long serialVersionUID = 7435962790062944603L;

	/**
	 * The time field of the UUID.
	 *
	 * @serial
	 */

	public long time;

	/**
	 * The clock sequence and node field of the UUID.
	 *
	 * @serial
	 */

	public long clockSeqAndNode;

	/**
	 * Constructor for UUID. Constructs a new, unique UUID.
	 *
	 * @see UUIDGen#newTime()
	 * @see UUIDGen#getClockSeqAndNode()
	 */
	public UUID() {
		this( UUIDGen.newTime(), UUIDGen.getClockSeqAndNode() );
	}

	/**
	 * Constructor for UUID. Constructs a UUID from two <code>long</code> values.
	 *
	 * @param time			the upper 64 bits
	 * @param clockSeqAndNode the lower 64 bits
	 */
	public UUID(long time, long clockSeqAndNode) {
		this.time = time;
		this.clockSeqAndNode = clockSeqAndNode;
	}

	/**
	 * Copy constructor for UUID. Values of the given UUID are copied.
	 *
	 * @param u the UUID, may not be <code>null</code>
	 */
	public UUID(UUID u) {
		this( u.time, u.clockSeqAndNode );
	}

	/**
	 * Parses a textual representation of a UUID.
	 * <p/>
	 * No validation is performed. If the {@link CharSequence} is shorter than 36 characters,
	 * {@link ArrayIndexOutOfBoundsException}s will be thrown.
	 *
	 * @param s the {@link CharSequence}, may not be <code>null</code>
	 */
	public UUID(CharSequence s) {
		this( Hex.parseLong( s.subSequence( 0, 18 ) ), Hex.parseLong( s.subSequence(
				19, 36 ) ) );
	}

	/**
	 * Compares this UUID to another Object. Throws a {@link ClassCastException} if
	 * the other Object is not an instance of the UUID class. Returns a value
	 * smaller than zero if the other UUID is "larger" than this UUID and a value
	 * larger than zero if the other UUID is "smaller" than this UUID.
	 *
	 * @param t the other UUID, may not be <code>null</code>
	 * @return a value &lt; 0, 0 or a value &gt; 0
	 * @throws ClassCastException
	 * @see java.lang.Comparable#compareTo(java.lang.Object)
	 */
	public int compareTo(UUID t) {
		if ( this == t ) {
			return 0;
		}
		if ( time > t.time ) {
			return 1;
		}
		if ( time < t.time ) {
			return -1;
		}
		if ( clockSeqAndNode > t.clockSeqAndNode ) {
			return 1;
		}
		if ( clockSeqAndNode < t.clockSeqAndNode ) {
			return -1;
		}
		return 0;
	}

	/**
	 * Tweaked Serialization routine.
	 *
	 * @param out the ObjectOutputStream
	 * @throws IOException
	 */
	private void writeObject(ObjectOutputStream out) throws IOException {
		out.writeLong( time );
		out.writeLong( clockSeqAndNode );
	}

	/**
	 * Tweaked Serialization routine.
	 *
	 * @param in the ObjectInputStream
	 * @throws IOException
	 */
	private void readObject(ObjectInputStream in) throws IOException {
		time = in.readLong();
		clockSeqAndNode = in.readLong();
	}

	/**
	 * Returns this UUID as a String.
	 *
	 * @return a String, never <code>null</code>
	 * @see java.lang.Object#toString()
	 * @see #toAppendable(Appendable)
	 */
	@Override
	public final String toString() {
		return toAppendable( null ).toString();
	}

	/**
	 * Appends a String representation of this to the given {@link StringBuffer} or
	 * creates a new one if none is given.
	 *
	 * @param in the StringBuffer to append to, may be <code>null</code>
	 * @return a StringBuffer, never <code>null</code>
	 * @see #toAppendable(Appendable)
	 */
	public StringBuffer toStringBuffer(StringBuffer in) {
		StringBuffer out = in;
		if ( out == null ) {
			out = new StringBuffer( 36 );
		} else {
			out.ensureCapacity( out.length() + 36 );
		}
		return (StringBuffer) toAppendable( out );
	}

	/**
	 * Appends a String representation of this object to the given {@link Appendable} object.
	 * <p/>
	 * For reasons I'll probably never understand, Sun has decided to have a number of I/O classes implement
	 * Appendable which forced them to destroy an otherwise nice and simple interface with {@link IOException}s.
	 * <p/>
	 * I decided to ignore any possible IOExceptions in this method.
	 *
	 * @param a the Appendable object, may be <code>null</code>
	 * @return an Appendable object, defaults to a {@link StringBuilder} if <code>a</code> is <code>null</code>
	 */
	public Appendable toAppendable(Appendable a) {
		Appendable out = a;
		if ( out == null ) {
			out = new StringBuilder( 36 );
		}
		try {
			Hex.append( out, (int) (time >> 32) ).append( '-' );
			Hex.append( out, (short) (time >> 16) ).append( '-' );
			Hex.append( out, (short) time ).append( '-' );
			Hex.append( out, (short) (clockSeqAndNode >> 48) ).append( '-' );
			Hex.append( out, clockSeqAndNode, 12 );
		} catch( IOException ex ) {
			// What were they thinking?
		}
		return out;
	}

	/**
	 * Returns a hash code of this UUID. The hash code is calculated by XOR'ing the
	 * upper 32 bits of the time and clockSeqAndNode fields and the lower 32 bits of
	 * the time and clockSeqAndNode fields.
	 *
	 * @return an <code>int</code> representing the hash code
	 * @see java.lang.Object#hashCode()
	 */
	@Override
	public int hashCode() {
		return (int) ((time >> 32) ^ time ^ (clockSeqAndNode >> 32) ^ clockSeqAndNode);
	}

	/**
	 * Clones this UUID.
	 *
	 * @return a new UUID with identical values, never <code>null</code>
	 */
	@Override
	public Object clone() {
		try {
			return super.clone();
		} catch( CloneNotSupportedException ex ) {
			// One of Sun's most epic fails.
			return null;
		}
	}

	/**
	 * Returns the time field of the UUID (upper 64 bits).
	 *
	 * @return the time field
	 */
	public final long getTime() {
		return time;
	}

	/**
	 * Returns the clock and node field of the UUID (lower 64 bits).
	 *
	 * @return the clockSeqAndNode field
	 */
	public final long getClockSeqAndNode() {
		return clockSeqAndNode;
	}

	/**
	 * Compares two Objects for equality.
	 *
	 * @param obj the Object to compare this UUID with, may be <code>null</code>
	 * @return <code>true</code> if the other Object is equal to this UUID,
	 *         <code>false</code> if not
	 * @see java.lang.Object#equals(Object)
	 */
	@Override
	public boolean equals(Object obj) {
		return obj instanceof UUID && compareTo( (UUID) obj ) == 0;
	}

	/**
	 * Returns the nil UUID (a UUID whose values are both set to zero).
	 * <p/>
	 * Starting with version 2.0, this method does return a new UUID instance every
	 * time it is called. Earlier versions returned one instance. This has now been
	 * changed because this UUID has public, non-final instance fields. Returning a
	 * new instance is therefore more safe.
	 *
	 * @return a nil UUID, never <code>null</code>
	 */
	public static UUID nilUUID() {
		return new UUID( 0, 0 );
	}

	public byte[] getBytes() {
		byte[] bytes = new byte[ 16 ];

		bytes[ 0 ] = (byte) ((time >> 56) & 0xFF);
		bytes[ 1 ] = (byte) ((time >> 48) & 0xFF);
		bytes[ 2 ] = (byte) ((time >> 40) & 0xFF);
		bytes[ 3 ] = (byte) ((time >> 32) & 0xFF);
		bytes[ 4 ] = (byte) ((time >> 24) & 0xFF);
		bytes[ 5 ] = (byte) ((time >> 16) & 0xFF);
		bytes[ 6 ] = (byte) ((time >> 8) & 0xFF);
		bytes[ 7 ] = (byte) ((time) & 0xFF);

		bytes[ 8 ] = (byte) ((clockSeqAndNode >> 56) & 0xFF);
		bytes[ 9 ] = (byte) ((clockSeqAndNode >> 48) & 0xFF);
		bytes[ 10 ] = (byte) ((clockSeqAndNode >> 40) & 0xFF);
		bytes[ 11 ] = (byte) ((clockSeqAndNode >> 32) & 0xFF);
		bytes[ 12 ] = (byte) ((clockSeqAndNode >> 24) & 0xFF);
		bytes[ 13 ] = (byte) ((clockSeqAndNode >> 16) & 0xFF);
		bytes[ 14 ] = (byte) ((clockSeqAndNode >> 8) & 0xFF);
		bytes[ 15 ] = (byte) ((clockSeqAndNode) & 0xFF);

		return bytes;
	}

	public static UUID fromBytes(byte bytes[]) {
		long time = (((long) bytes[ 0 ] << 56) +
				((long) (bytes[ 1 ] & 255) << 48) +
				((long) (bytes[ 2 ] & 255) << 40) +
				((long) (bytes[ 3 ] & 255) << 32) +
				((long) (bytes[ 4 ] & 255) << 24) +
				((bytes[ 5 ] & 255) << 16) +
				((bytes[ 6 ] & 255) << 8) +
				((bytes[ 7 ] & 255) << 0));

		long clockSeqAndNode = (((long) bytes[ 8 ] << 56) +
				((long) (bytes[ 9 ] & 255) << 48) +
				((long) (bytes[ 10 ] & 255) << 40) +
				((long) (bytes[ 11 ] & 255) << 32) +
				((long) (bytes[ 12 ] & 255) << 24) +
				((bytes[ 13 ] & 255) << 16) +
				((bytes[ 14 ] & 255) << 8) +
				((bytes[ 15 ] & 255) << 0));

		return new UUID( time, clockSeqAndNode );
	}
}
