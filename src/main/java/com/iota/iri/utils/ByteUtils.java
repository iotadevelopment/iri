package com.iota.iri.utils;

import java.util.BitSet;

public class ByteUtils {
    // converts a byte array to a BitSet starting at the offset
    public static BitSet convertByteArrayToBitSet(byte[] byteArray, int startOffset, int sizeOfBitSet) {
        if((byteArray.length - startOffset) * 8 < sizeOfBitSet) {
            throw new IllegalArgumentException("the byte[] is too small to create a BitSet of length " + sizeOfBitSet);
        }

        BitSet result = new BitSet(sizeOfBitSet);

        int bitMask = 128;
        for(int i = 0; i < sizeOfBitSet; i++) {
            // insert the bits in reverse order
            result.set(i, (byteArray[i / 8 + startOffset] & bitMask) != 0);

            bitMask = bitMask / 2;

            if(bitMask == 0) {
                bitMask = 128;
            }
        }

        return result;
    }

    public static BitSet convertByteArrayToBitSet(byte[] byteArray, int startOffset) {
        return convertByteArrayToBitSet(byteArray, startOffset, (byteArray.length - startOffset) * 8);
    }

    public static BitSet convertByteArrayToBitSet(byte[] byteArray) {
        return convertByteArrayToBitSet(byteArray, 0);
    }

    public static byte[] convertBitSetToByteArray(BitSet bitSet) {
        int lengthOfBitSet = bitSet.length();
        int lengthOfArray = (int) Math.ceil(lengthOfBitSet / 8.0);

        byte[] result = new byte[lengthOfArray];

        for(int i = 0; i < lengthOfBitSet; i++) {
            // for every new index -> start with a 1 so the shifting keeps track of the position we are on
            if(i % 8 == 0) {
                result[i / 8] = 1;
            }

            // shift the existing bits to the left to make space for the bit that gets written now
            result[i / 8] <<= 1;

            // write the current bit
            result[i / 8] ^= bitSet.get(i) ? 1 : 0;

            // if we are at the last bit -> shift the missing bytes
            if(i == (lengthOfBitSet - 1)) {
                result[i / 8] <<= (8 - (i % 8) - 1);
            }
        }

        return result;
    }
}
