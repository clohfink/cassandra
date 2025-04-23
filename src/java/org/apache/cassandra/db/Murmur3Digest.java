package org.apache.cassandra.db;

import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import sun.misc.Unsafe;
import org.apache.cassandra.db.context.CounterContext;
import org.apache.cassandra.db.marshal.ValueAccessor;
import sun.nio.ch.DirectBuffer;

/**
 * A streaming, zero-allocation Murmur3 digest implementation.
 * This implementation follows the Murmur3_x64_128 algorithm.
 *
 * Not thread-safe.
 */
public class Murmur3Digest extends Digest
{
    // Constants for Murmur3_x64_128
    private static final long C1 = 0x87c37b91114253d5L;
    private static final long C2 = 0x4cf5ad432745937fL;

    // Internal state variables for the hash.
    private long h1;
    private long h2;
    private int totalLen;
    private final int seed;

    // Tail buffer for storing partial (less than 16-byte) blocks.
    private final byte[] tailBuffer = new byte[16];
    private int tailLen = 0;

    // Thread-local temporary buffer for non-array-backed ByteBuffer updates.
    private static final ThreadLocal<byte[]> tmpBuffer = ThreadLocal.withInitial(() -> new byte[4096]);

    private static final Unsafe UNSAFE;
    private static final long BYTE_ARRAY_BASE_OFFSET;

    static
    {
        try
        {
            Field f = Unsafe.class.getDeclaredField("theUnsafe");
            f.setAccessible(true);
            UNSAFE = (Unsafe) f.get(null);
            BYTE_ARRAY_BASE_OFFSET = UNSAFE.arrayBaseOffset(byte[].class);
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }
    }

    /**
     * Creates a Murmur3Digest with the default seed (0).
     */
    public Murmur3Digest()
    {
        this(0);
    }

    /**
     * Creates a Murmur3Digest with the given seed.
     *
     * @param seed the seed value for the hash.
     */
    public Murmur3Digest(int seed)
    {
        super(null);
        this.seed = seed;
        this.h1 = seed;
        this.h2 = seed;
        this.totalLen = 0;
    }

    /**
     * Updates the hash with the given byte array.
     */
    @Override
    public Digest update(byte[] buf, int off, int len)
    {
        if (buf == null)
            throw new IllegalArgumentException("Buffer is null");
        if (off < 0 || len < 0 || off + len > buf.length)
            throw new IllegalArgumentException(String.format(
            "Invalid buffer update: buf.length=%d, off=%d, len=%d, off+len=%d",
            buf.length, off, len, off + len));

        // If there are pending tail bytes from a previous update, try to complete a block.
        if (tailLen > 0)
        {
            int needed = 16 - tailLen;
            if (len < needed)
            {
                // Not enough to fill the block; just copy into the tail.
                System.arraycopy(buf, off, tailBuffer, tailLen, len);
                tailLen += len;
                totalLen += len;
                return this;
            }
            else
            {
                // Complete the block using the tail buffer.
                System.arraycopy(buf, off, tailBuffer, tailLen, needed);
                processBlock(tailBuffer, 0);
                totalLen += needed;
                off += needed;
                len -= needed;
                tailLen = 0;
            }
        }

        // Process full 16-byte blocks directly from buf.
        int blocks = len / 16;
        for (int i = 0; i < blocks; i++)
        {
            processBlock(buf, off + i * 16);
        }
        int processed = blocks * 16;
        totalLen += processed;

        // Copy any remaining bytes into the tail buffer.
        int remaining = len - processed;
        if (remaining > 0)
        {
            System.arraycopy(buf, off + processed, tailBuffer, 0, remaining);
            tailLen = remaining;
            totalLen += remaining;
        }
        return this;
    }

    /**
     * Processes a single 16-byte block starting at the given offset in the byte array.
     */
    private void processBlock(byte[] block, int offset)
    {
        // Read two 8-byte little-endian longs from the block using Unsafe.
        long k1 = UNSAFE.getLong(block, BYTE_ARRAY_BASE_OFFSET + offset);
        long k2 = UNSAFE.getLong(block, BYTE_ARRAY_BASE_OFFSET + offset + 8);

        // Mix k1.
        k1 *= C1;
        k1 = Long.rotateLeft(k1, 31);
        k1 *= C2;
        h1 ^= k1;

        h1 = Long.rotateLeft(h1, 27);
        h1 += h2;
        h1 = h1 * 5 + 0x52dce729;

        // Mix k2.
        k2 *= C2;
        k2 = Long.rotateLeft(k2, 33);
        k2 *= C1;
        h2 ^= k2;

        h2 = Long.rotateLeft(h2, 31);
        h2 += h1;
        h2 = h2 * 5 + 0x38495ab5;
    }

    /**
     * Updates the hash with the content of the given ByteBuffer.
     * For array-backed buffers, the update is delegated directly; for non-array-backed ones,
     * uses a thread-local temporary buffer.
     */
    @Override
    public Digest update(ByteBuffer input)
    {
        int pos = input.position();
        int rem = input.remaining();
        update(input, pos, rem);
        return this;
    }

    /**
     * Updates the hash with data from the given ByteBuffer starting at the specified position
     * for a given length. This method does not change the buffer's position.
     */
    @Override
    public Digest update(ByteBuffer input, int pos, int len)
    {
        if (len <= 0)
            return this;
        if (input.hasArray())
        {
            update(input.array(), input.arrayOffset() + pos, len);
        }
        else
        {
            byte[] buf = tmpBuffer.get();

            if (input.isDirect())
            {
                // For direct ByteBuffers, use Unsafe.copyMemory for efficient block transfer.
                long sourceAddress = ((DirectBuffer) input).address() + pos;
                int remaining = len;
                while (remaining > 0)
                {
                    int chunk = Math.min(remaining, buf.length);
                    UNSAFE.copyMemory(null, sourceAddress, buf, BYTE_ARRAY_BASE_OFFSET, chunk);
                    update(buf, 0, chunk);
                    sourceAddress += chunk;
                    remaining -= chunk;
                }
            }
            else
            {
                // Fallback for non-array, non-direct ByteBuffers:
                // Read one byte at a time (in chunks) using ByteBuffer.get(int index).
                int remaining = len;
                int offset = pos;
                while (remaining > 0)
                {
                    int chunk = Math.min(remaining, buf.length);
                    for (int i = 0; i < chunk; i++)
                    {
                        buf[i] = input.get(offset + i);
                    }
                    update(buf, 0, chunk);
                    offset += chunk;
                    remaining -= chunk;
                }
            }
        }
        return this;
    }

    /**
     * Updates the hash using a counter context, delegating to the provided ValueAccessor.
     */
    @Override
    public <V> Digest updateWithCounterContext(V context, ValueAccessor<V> accessor)
    {
        if (accessor.isEmpty(context))
            return this;
        int pos = CounterContext.headerLength(context, accessor);
        int size = accessor.size(context);
        int len = size - pos;
        accessor.digest(context, pos, len, this);
        return this;
    }

    /**
     * Updates the hash with a single byte value.
     */
    @Override
    public Digest updateWithByte(int val)
    {
        byte[] t = tmpBuffer.get();
        t[0] = (byte) (val & 0xFF);
        update(t, 0, 1);
        return this;
    }

    /**
     * Updates the hash with a 32‑bit integer value.
     */
    @Override
    public Digest updateWithInt(int val)
    {
        byte[] t = tmpBuffer.get();
        UNSAFE.putInt(t, BYTE_ARRAY_BASE_OFFSET, val); // little-endian conversion
        update(t, 0, 4);
        return this;
    }

    /**
     * Updates the hash with a 64‑bit long value.
     */
    @Override
    public Digest updateWithLong(long val)
    {
        byte[] t = tmpBuffer.get();
        UNSAFE.putLong(t, BYTE_ARRAY_BASE_OFFSET, val); // little-endian conversion
        update(t, 0, 8);
        return this;
    }

    /**
     * Updates the hash with a boolean value.
     * (Mapping: true -> 1, false -> 0; adjust as needed to maintain legacy behavior.)
     */
    @Override
    public Digest updateWithBoolean(boolean val)
    {
        return updateWithByte(val ? 1 : 0);
    }

    /**
     * Completes the digest computation, processing any tail bytes, performing finalization,
     * and returns the 128‑bit hash as a 16‑byte big‑endian array.
     */
    @Override
    public byte[] digest()
    {
        long k1 = 0;
        long k2 = 0;

        // Process remaining tail bytes.
        switch (tailLen)
        {
            case 15: k2 ^= ((long) tailBuffer[14] & 0xffL) << 48;
            case 14: k2 ^= ((long) tailBuffer[13] & 0xffL) << 40;
            case 13: k2 ^= ((long) tailBuffer[12] & 0xffL) << 32;
            case 12: k2 ^= ((long) tailBuffer[11] & 0xffL) << 24;
            case 11: k2 ^= ((long) tailBuffer[10] & 0xffL) << 16;
            case 10: k2 ^= ((long) tailBuffer[9] & 0xffL) << 8;
            case 9:  k2 ^= ((long) tailBuffer[8] & 0xffL);
                k2 *= C2;
                k2 = Long.rotateLeft(k2, 33);
                k2 *= C1;
                h2 ^= k2;
            case 8:  k1 ^= ((long) tailBuffer[7] & 0xffL) << 56;
            case 7:  k1 ^= ((long) tailBuffer[6] & 0xffL) << 48;
            case 6:  k1 ^= ((long) tailBuffer[5] & 0xffL) << 40;
            case 5:  k1 ^= ((long) tailBuffer[4] & 0xffL) << 32;
            case 4:  k1 ^= ((long) tailBuffer[3] & 0xffL) << 24;
            case 3:  k1 ^= ((long) tailBuffer[2] & 0xffL) << 16;
            case 2:  k1 ^= ((long) tailBuffer[1] & 0xffL) << 8;
            case 1:  k1 ^= ((long) tailBuffer[0] & 0xffL);
                k1 *= C1;
                k1 = Long.rotateLeft(k1, 31);
                k1 *= C2;
                h1 ^= k1;
            default: break;
        }

        // Finalization: incorporate total length and mix the state.
        h1 ^= totalLen;
        h2 ^= totalLen;

        h1 += h2;
        h2 += h1;

        h1 = fmix64(h1);
        h2 = fmix64(h2);

        h1 += h2;
        h2 += h1;

        // Produce the final hash (16 bytes).
        byte[] result = new byte[16];
        // Note: The final result is returned as big-endian.
        UNSAFE.putLong(result, BYTE_ARRAY_BASE_OFFSET, h1);
        UNSAFE.putLong(result, BYTE_ARRAY_BASE_OFFSET + 8, h2);
        return result;
    }

    /**
     * The fmix function from the Murmur3 algorithm finalization step.
     */
    private static long fmix64(long k)
    {
        k ^= k >>> 33;
        k *= 0xff51afd7ed558ccdL;
        k ^= k >>> 33;
        k *= 0xc4ceb9fe1a85ec53L;
        k ^= k >>> 33;
        return k;
    }

    /**
     * Returns the total number of bytes processed.
     */
    @Override
    public long inputBytes()
    {
        return totalLen;
    }
}
