/*
 * Javolution - Java(TM) Solution for Real-Time and Embedded Systems
 * Copyright (C) 2008 - Javolution (http://javolution.org/)
 * All rights reserved.
 * 
 * Permission to use, copy, modify, and distribute this software is
 * freely granted, provided that this notice is preserved.
 *
 * 20-sep-2012:
 * copied to com.precog.util.BitSet to allow some small additions to the API.
 */
package com.precog.util;

import javolution.util.*;

import java.lang.UnsupportedOperationException;
import java.util.Set;
import javax.realtime.MemoryArea;
import javolution.context.ObjectFactory;
import javolution.lang.MathLib;
import javolution.lang.Reusable;

/**
 * <p> This class represents either a table of bits or a set of non-negative 
 *     numbers.</p>
 * 
 * <p> This class is integrated with the collection framework (as 
 *     a set of {@link Index indices} and obeys the collection semantic
 *     for methods such as {@link #size} (cardinality) or {@link #equals}
 *     (same set of indices).</p>
 * 
 * @author  <a href="mailto:jean-marie@dautelle.com">Jean-Marie Dautelle</a>
 * @version 5.3, February 24, 2008
 */
public class BitSet extends FastCollection <Index>  implements Set <Index> , Reusable {

    /**
     * Holds the set factory.
     */
    private static final ObjectFactory FACTORY = new ObjectFactory() {

        public Object create() {
            return new BitSet();
        }
    };
    /**
     * Holds the bits (64 bits per long).
     */
    private long[] bits;

    public long[] getBits() {
        return bits;
    }

    public void setBits(long[] arr) {
        bits = arr;
        _length = arr.length;
    }

    /**
     * Holds the length in words (long) of this bit set.
     * Any word at or above the current length should be ignored (assumed
     * to be zero). 
     */
    private int _length;

    public int getBitsLength() {
        return _length;
    }

    /**
     * Creates a bit set of small initial capacity. All bits are initially 
     * {@code false}.
     */
    public BitSet() {
        this(64);
    }

    /**
     * Creates a bit set of specified initial capacity (in bits). 
     * All bits are initially {@code false}.  This
     * constructor reserves enough space to represent the integers
     * from {@code 0} to {@code bitSize-1}.
     * 
     * @param bitSize the initial capacity in bits.
     */
    public BitSet(int bitSize) {
        _length = ((bitSize - 1) >> 6) + 1;
        bits = new long[_length];
    }

    /**
     * Creates a bitset given the bits array and length.
     *
     * Used internally by copy.
     */
    private BitSet(long[] _bits, int _len) {
        bits = _bits;
        _length = _len;
    }

    /**
     * Returns a new, preallocated or {@link #recycle recycled} set instance
     * (on the stack when executing in a {@link javolution.context.StackContext
     * StackContext}).
     *
     * @return a new, preallocated or recycled set instance.
     */
    public static BitSet newInstance() {
        BitSet bitSet = (BitSet) FACTORY.object();
        bitSet._length = 0;
        return bitSet;
    }

    /**
     * Recycles a set {@link #newInstance() instance} immediately
     * (on the stack when executing in a {@link javolution.context.StackContext
     * StackContext}). 
     */
    public static void recycle(BitSet instance) {
        FACTORY.recycle(instance);
    }

    /**
     * Adds the specified index to this set. This method is equivalent 
     * to <code>set(index.intValue())</code>.
     * 
     * @param index the integer value to be appended to this set.
     * @return {@code true} if this set did not contains the specified
     *         index; {@code false} otherwise.
     */
    public boolean add( Index index) {
        int bitIndex = ((Index) index).intValue();
        if (this.get(bitIndex)) return false; // Already there.
        set(bitIndex);
        return true;
    }
    
    /**
     * Performs the logical AND operation on this bit set and the
     * given bit set. This means it builds the intersection
     * of the two sets. The result is stored into this bit set.
     *
     * @param that the second bit set.
     */
    public void and(BitSet that) {
        final int n = MathLib.min(this._length, that._length);
        for (int i = 0; i < n; ++i) {
            this.bits[i] &= that.bits[i];
        }
        this._length = n;
    }

    /**
     * Performs the logical AND operation on this bit set and the
     * complement of the given bit set.  This means it
     * selects every element in the first set, that isn't in the
     * second set. The result is stored into this bit set.
     *
     * @param that the second bit set
     */
    public void andNot(BitSet that) {
        int i = Math.min(this._length, that._length);
        while (--i >= 0) {
            this.bits[i] &= ~that.bits[i];
        }
    }

    /**
     * Returns the number of bits set to {@code true} (or the size of this 
     * set).
     *
     * @return the number of bits being set.
     */
    public int cardinality() {
        int sum = 0;
        for (int i = 0; i < _length; i++) {
            sum += MathLib.bitCount(bits[i]);
        }
        return sum;
    }

    /**
     * Sets all bits in the set to {@code false} (empty the set).
     */
    public void clear() {
        _length = 0;
    }

    /**
     * Removes the specified integer value from this set. That is
     * the corresponding bit is cleared.
     *
     * @param bitIndex a non-negative integer.
     * @throws IndexOutOfBoundsException if {@code index < 0}
     */
    public void clear(int bitIndex) {
        int longIndex = bitIndex >> 6;
        if (longIndex >= _length) 
            return;
        bits[longIndex] &= ~(1L << bitIndex);
    }

    /**
     * Sets the bits from the specified {@code fromIndex} (inclusive) to the
     * specified {@code toIndex} (exclusive) to {@code false}.
     *
     * @param  fromIndex index of the first bit to be cleared.
     * @param  toIndex index after the last bit to be cleared.
     * @throws IndexOutOfBoundsException if 
     *          {@code (fromIndex < 0) | (toIndex < fromIndex)}
     */
    public void clear(int fromIndex, int toIndex) {
        if ((fromIndex < 0) || (toIndex < fromIndex)) 
            throw new IndexOutOfBoundsException();
        int i = fromIndex >>> 6;
        if (i >= _length) 
            return; // Ensures that i < _length
        int j = toIndex >>> 6;
        if (i == j) {
            bits[i] &= ((1L << fromIndex) - 1) | (-1L << toIndex);
            return;
        }
        bits[i] &= (1L << fromIndex) - 1;
        if (j < _length) {
            bits[j] &= -1L << toIndex;
        }
        for (int k = i + 1; (k < j) && (k < _length); k++) {
            bits[k] = 0;
        }
    }

    public BitSet copy() {
        long[] _bits = new long[_length];
        System.arraycopy(bits, 0, _bits, 0, _length);
        return new BitSet(_bits, _length);
    }

    /**
     * Sets the bit at the index to the opposite value.
     *
     * @param bitIndex the index of the bit.
     * @throws IndexOutOfBoundsException if {@code bitIndex < 0}
     */
    public void flip(int bitIndex) {
        int i = bitIndex >> 6;
        setLength(i + 1);
        bits[i] ^= 1L << bitIndex;
    }

    /**
     * Sets a range of bits to the opposite value.
     *
     * @param fromIndex the low index (inclusive).
     * @param toIndex the high index (exclusive).
     * @throws IndexOutOfBoundsException if 
     *          {@code (fromIndex < 0) | (toIndex < fromIndex)}
     */
    public void flip(int fromIndex, int toIndex) {
        if ((fromIndex < 0) || (toIndex < fromIndex)) 
            throw new IndexOutOfBoundsException();
        int i = fromIndex >>> 6;
        int j = toIndex >>> 6;
        setLength(j + 1);
        if (i == j) {
            bits[i] ^= (-1L << fromIndex) & ((1L << toIndex) - 1);
            return;
        }
        bits[i] ^= -1L << fromIndex;
        bits[j] ^= (1L << toIndex) - 1;
        for (int k = i + 1; k < j; k++) {
            bits[k] ^= -1;
        }
    }

    /**
     * Returns {@code true}> if the specified integer is in 
     * this bit set; {@code false } otherwise.
     *
     * @param bitIndex a non-negative integer.
     * @return the value of the bit at the specified index.
     * @throws IndexOutOfBoundsException if {@code bitIndex < 0}
     */
    public boolean get(int bitIndex) {
        int i = bitIndex >> 6;
        return (i >= _length) ? false : (bits[i] & (1L << bitIndex)) != 0;
    }

    // added for scala compatibility
    public boolean apply(int bitIndex) {
        int i = bitIndex >> 6;
        return (i >= _length) ? false : (bits[i] & (1L << bitIndex)) != 0;
    }
    public boolean contains(int bitIndex) {
        int i = bitIndex >> 6;
        return (i >= _length) ? false : (bits[i] & (1L << bitIndex)) != 0;
    }

    /**
     * Returns a new bit set composed of a range of bits from this one.
     *
     * @param fromIndex the low index (inclusive).
     * @param toIndex the high index (exclusive).
     * @return a context allocated bit set instance.
     * @throws IndexOutOfBoundsException if 
     *          {@code (fromIndex < 0) | (toIndex < fromIndex)}
     */
    public BitSet get(int fromIndex, int toIndex) {
        if (fromIndex < 0 || fromIndex > toIndex) 
            throw new IndexOutOfBoundsException();
        BitSet bitSet = BitSet.newInstance();
        int length = MathLib.min(_length, (toIndex >>> 6) + 1);
        bitSet.setLength(length);
        System.arraycopy(bits, 0, bitSet.bits, 0, length);
        bitSet.clear(0, fromIndex);
        bitSet.clear(toIndex, length << 6);
        return bitSet;
    }

    /**
     * Returns {@code true} if this bit set shares at least one
     * common bit with the specified bit set.
     *
     * @param that the bit set to check for intersection
     * @return {@code true} if the sets intersect; {@code false} otherwise.
     */
    public boolean intersects(BitSet that) {
        int i = MathLib.min(this._length, that._length);
        while (--i >= 0) {
            if ((bits[i] & that.bits[i]) != 0) {
                return true;
            }
        }
        return false;
    }

    // provided by super class
    //public boolean isEmpty() {
    //    return nextSetBit(0) == -1;
    //}
    
    /**
     * Returns the logical number of bits actually used by this bit
     * set.  It returns the index of the highest set bit plus one.
     * 
     * <p> Note: This method does not return the number of set bits
     *           which is returned by {@link #size} </p>
     *
     * @return the index of the highest set bit plus one.
     */
    public int length() {
        for (int i = _length; --i >= 0;) {
            long l = bits[i];
            if (l != 0) {
                return i << 6 + 64 - MathLib.numberOfTrailingZeros(l);
            }
        }
        return 0;
    }

    /**
     * Returns the index of the next {@code false} bit, from the specified bit
     * (inclusive).
     *
     * @param fromIndex the start location.
     * @return the first {@code false} bit.
     * @throws IndexOutOfBoundsException if {@code fromIndex < 0} 
     */
    public int nextClearBit(int fromIndex) {
        int offset = fromIndex >> 6;
        long mask = 1L << fromIndex;
        while (offset < _length) {
            long h = bits[offset];
            do {
                if ((h & mask) == 0) {
                    return fromIndex;
                }
                mask <<= 1;
                fromIndex++;
            } while (mask != 0);
            mask = 1;
            offset++;
        }
        return fromIndex;
    }

    /**
     * Returns the index of the next {@code true} bit, from the specified bit
     * (inclusive). If there is none, {@code -1} is returned. 
     * The following code will iterates through the bit set:[code]
     *    for (int i=nextSetBit(0); i >= 0; i = nextSetBit(i)) {
     *         ...
     *    }[/code]
     *
     * @param fromIndex the start location.
     * @return the first {@code false} bit.
     * @throws IndexOutOfBoundsException if {@code fromIndex < 0} 
     */
    public int nextSetBit(int fromIndex) {
        int offset = fromIndex >> 6;
        long mask = 1L << fromIndex;
        while (offset < _length) {
            long h = bits[offset];
            do {
                if ((h & mask) != 0) 
                    return fromIndex;
                mask <<= 1;
                fromIndex++;
            } while (mask != 0);
            mask = 1;
            offset++;
        }
        return -1;
    }

    public boolean nonEmpty() {
        return nextSetBit(0) != -1;
    }

    /**
     * Performs the logical OR operation on this bit set and the one specified.
     * In other words, builds the union of the two sets.  
     * The result is stored into this bit set.
     *
     * @param that the second bit set.
     */
    public void or(BitSet that) {
        if (that._length > this._length) {
            setLength(that._length);
        }
        for (int i = that._length; --i >= 0;) {
            bits[i] |= that.bits[i];
        }
    }

    /**
     * Adds the specified integer to this set (corresponding bit is set to 
     * {@code true}.
     *
     * @param bitIndex a non-negative integer.
     * @throws IndexOutOfBoundsException if {@code bitIndex < 0}
     */
    public void set(int bitIndex) {
        int i = bitIndex >> 6;
        if (i >= _length) {
            setLength(i + 1);
        }
        bits[i] |= 1L << bitIndex;
    }

    /**
     * Sets the bit at the given index to the specified value.
     *
     * @param bitIndex the position to set.
     * @param value the value to set it to.
     * @throws IndexOutOfBoundsException if {@code bitIndex < 0}
     */
    public void set(int bitIndex, boolean value) {
        if (value) {
            set(bitIndex);
        } else {
            clear(bitIndex);
        }
    }

    // added for scala compatibility
    public void update(int bitIndex, boolean value) {
        if (value) {
            set(bitIndex);
        } else {
            clear(bitIndex);
        }
    }

    /**
     * Sets the bits from the specified {@code fromIndex} (inclusive) to the
     * specified {@code toIndex} (exclusive) to {@code true}.
     *
     * @param  fromIndex index of the first bit to be set.
     * @param  toIndex index after the last bit to be set.
     * @throws IndexOutOfBoundsException if 
     *          {@code (fromIndex < 0) | (toIndex < fromIndex)}
     */
    public void set(int fromIndex, int toIndex) {
        if ((fromIndex < 0) || (toIndex < fromIndex)) 
            throw new IndexOutOfBoundsException();
        int i = fromIndex >>> 6;
        int j = toIndex >>> 6;
        setLength(j + 1);
        if (i == j) {
            bits[i] |= (-1L << fromIndex) & ((1L << toIndex) - 1);
            return;
        }
        bits[i] |= -1L << fromIndex;
        bits[j] |= (1L << toIndex) - 1;
        for (int k = i + 1; k < j; k++) {
            bits[k] = -1;
        }
    }

    /**
     * Sets the bits between from (inclusive) and to (exclusive) to the
     * specified value.
     *
     * @param fromIndex the start range (inclusive).
     * @param toIndex the end range (exclusive).
     * @param value the value to set it to.
     * @throws IndexOutOfBoundsException if {@code bitIndex < 0}
     */
    public void set(int fromIndex, int toIndex, boolean value) {
        if (value) {
            set(fromIndex, toIndex);
        } else {
            clear(fromIndex, toIndex);
        }
    }

    /**
     * Returns the cardinality of this bit set (number of bits set).
     * 
     * <P>Note: Unlike {@code java.util.BitSet} this method does not 
     *          returns an approximation of the number of bits of space 
     *          actually in use. This method is compliant with 
     *          java.util.Collection meaning for size().</p>
     *
     * @return the cardinality of this bit set.
     */
    public int size() {
        return cardinality();
    }

    /**
     * Performs the logical XOR operation on this bit set and the one specified.
     * In other words, builds the symmetric remainder of the two sets 
     * (the elements that are in one set, but not in the other).  
     * The result is stored into this bit set.
     *
     * @param that the second bit set.
     */
    public void xor(BitSet that) {
        if (that._length > this._length) {
            setLength(that._length);
        }
        for (int i = that._length; --i >= 0;) {
            bits[i] ^= that.bits[i];
        }
    }

    // Optimization.
    public boolean equals(Object obj) {
        if (!(obj instanceof BitSet)) 
            return super.equals(obj);
        BitSet that = (BitSet) obj;
        int n = MathLib.min(this._length, that._length);
        for (int i = 0; i < n; ++i) {
            if (bits[i] != that.bits[i]) 
                return false;
        }
        for (int i = n; i < this._length; i++) {
            if (this.bits[i] != 0) 
                return false;
        }
        for (int i = n; i < that._length; i++) {
            if (that.bits[i] != 0) 
                return false;
        }
        return true;
    }
    
    // Optimization.
    public int hashCode() {
        int h = 0;
         for (int i=nextSetBit(0); i >= 0; i = nextSetBit(i)) {
             h += i;
        }
        return h;
    }

    // Implements Reusable.
    public void reset() {
        _length = 0;
    }
    
    // Implements abstract methods.
    
    // Records holds the ordering position of the bit sets.
    // (e.g. first bit set has a position of zero).
    
    public Record head() {
        return Index.valueOf(-1);
    }

    public Record tail() {
        return Index.valueOf(cardinality());
    }

    public  Index  valueOf(Record record) {
        int i = ((Index)record).intValue();
        int count= 0;
        for (int j = 0; j < _length;) {
            long l = bits[j++];
            count += MathLib.bitCount(l);
            if (count > i) { // Found word for record.
                int bitIndex = j << 6;
                while (count != i) {
                    int shiftRight = MathLib.numberOfLeadingZeros(l) + 1;
                    l <<= shiftRight;
                    bitIndex -= shiftRight;
                    count--;
                }
                return Index.valueOf(bitIndex);
            }
        }
        return null;
    }

    public void delete(Record record) {
        Index bitIndex = (Index) valueOf(record);
        if (bitIndex != null)
        throw new UnsupportedOperationException("Not supported yet.");
    }

    /**
     * Sets the new length of the table (all new bits are <code>false</code>).
     *
     * @param newLength the new length of the table.
     */
    private final void setLength(final int newLength) {
        if (bits.length < newLength) { // Resizes array.
            MemoryArea.getMemoryArea(this).executeInArea(new Runnable() {

                public void run() {
                    int arrayLength = bits.length;
                    while (arrayLength < newLength) {
                        arrayLength <<= 1;
                    }
                    long[] tmp = new long[arrayLength];
                    System.arraycopy(bits, 0, tmp, 0, _length);
                    bits = tmp;
                }
            });
        }
        for (int i = _length; i < newLength; i++) {
            bits[i] = 0;
        }
        _length = newLength;
    }

    private static final long serialVersionUID = 1L;
}
