package edu.caltech.test.nanodb.util;


import org.testng.annotations.Test;

import edu.caltech.nanodb.util.ArrayUtil;


/**
 */
@Test
public class TestArrayUtil {
    static byte[] DATA_A = { 1, 2, 3 };
    static byte[] DATA_B = { 1, 3, 4 };
    static byte[] DATA_C = { 1, 2, 4 };
    static byte[] DATA_D = { 1, 2, 3 };
    static byte[] DATA_E = { 0, 1, 3 };
    static byte[] DATA_F = { 0, 2, 3 };
    static byte[] DATA_G = { 0, 1, 2 };

    static byte[] DATA_H = { 1, 2, 3, 4 };

    public void testSizeOfIdenticalRange() {
        assert ArrayUtil.sizeOfIdenticalRange(DATA_A, DATA_B, 0) == 1;
        assert ArrayUtil.sizeOfIdenticalRange(DATA_A, DATA_B, 1) == 0;
        assert ArrayUtil.sizeOfIdenticalRange(DATA_A, DATA_B, 2) == 0;

        assert ArrayUtil.sizeOfIdenticalRange(DATA_A, DATA_C, 0) == 2;
        assert ArrayUtil.sizeOfIdenticalRange(DATA_A, DATA_C, 1) == 1;
        assert ArrayUtil.sizeOfIdenticalRange(DATA_A, DATA_C, 2) == 0;

        assert ArrayUtil.sizeOfIdenticalRange(DATA_A, DATA_D, 0) == 3;
        assert ArrayUtil.sizeOfIdenticalRange(DATA_A, DATA_D, 1) == 2;
        assert ArrayUtil.sizeOfIdenticalRange(DATA_A, DATA_D, 2) == 1;

        assert ArrayUtil.sizeOfIdenticalRange(DATA_A, DATA_E, 0) == 0;
        assert ArrayUtil.sizeOfIdenticalRange(DATA_A, DATA_E, 1) == 0;
        assert ArrayUtil.sizeOfIdenticalRange(DATA_A, DATA_E, 2) == 1;

        assert ArrayUtil.sizeOfIdenticalRange(DATA_A, DATA_F, 0) == 0;
        assert ArrayUtil.sizeOfIdenticalRange(DATA_A, DATA_F, 1) == 2;
        assert ArrayUtil.sizeOfIdenticalRange(DATA_A, DATA_F, 2) == 1;

        assert ArrayUtil.sizeOfIdenticalRange(DATA_A, DATA_G, 0) == 0;
        assert ArrayUtil.sizeOfIdenticalRange(DATA_A, DATA_G, 1) == 0;
        assert ArrayUtil.sizeOfIdenticalRange(DATA_A, DATA_G, 2) == 0;
    }
    
    
    public void testSizeOfIdenticalRangeAbuse() {
        try {
            ArrayUtil.sizeOfIdenticalRange(DATA_A, null, 0);
            assert false;
        }
        catch (IllegalArgumentException e) {
            // success
        }

        try {
            ArrayUtil.sizeOfIdenticalRange(null, DATA_A, 0);
            assert false;
        }
        catch (IllegalArgumentException e) {
            // success
        }

        try {
            ArrayUtil.sizeOfIdenticalRange(DATA_A, DATA_H, 0);
            assert false;
        }
        catch (IllegalArgumentException e) {
            // success
        }

        try {
            ArrayUtil.sizeOfIdenticalRange(DATA_H, DATA_A, 0);
            assert false;
        }
        catch (IllegalArgumentException e) {
            // success
        }
    }


    public void testSizeOfDifferentRange() {
        assert ArrayUtil.sizeOfDifferentRange(DATA_A, DATA_B, 0) == 0;
        assert ArrayUtil.sizeOfDifferentRange(DATA_A, DATA_B, 1) == 2;
        assert ArrayUtil.sizeOfDifferentRange(DATA_A, DATA_B, 2) == 1;

        assert ArrayUtil.sizeOfDifferentRange(DATA_A, DATA_C, 0) == 0;
        assert ArrayUtil.sizeOfDifferentRange(DATA_A, DATA_C, 1) == 0;
        assert ArrayUtil.sizeOfDifferentRange(DATA_A, DATA_C, 2) == 1;

        assert ArrayUtil.sizeOfDifferentRange(DATA_A, DATA_D, 0) == 0;
        assert ArrayUtil.sizeOfDifferentRange(DATA_A, DATA_D, 1) == 0;
        assert ArrayUtil.sizeOfDifferentRange(DATA_A, DATA_D, 2) == 0;

        assert ArrayUtil.sizeOfDifferentRange(DATA_A, DATA_E, 0) == 2;
        assert ArrayUtil.sizeOfDifferentRange(DATA_A, DATA_E, 1) == 1;
        assert ArrayUtil.sizeOfDifferentRange(DATA_A, DATA_E, 2) == 0;

        assert ArrayUtil.sizeOfDifferentRange(DATA_A, DATA_F, 0) == 1;
        assert ArrayUtil.sizeOfDifferentRange(DATA_A, DATA_F, 1) == 0;
        assert ArrayUtil.sizeOfDifferentRange(DATA_A, DATA_F, 2) == 0;

        assert ArrayUtil.sizeOfDifferentRange(DATA_A, DATA_G, 0) == 3;
        assert ArrayUtil.sizeOfDifferentRange(DATA_A, DATA_G, 1) == 2;
        assert ArrayUtil.sizeOfDifferentRange(DATA_A, DATA_G, 2) == 1;
    }


    public void testSizeOfDifferentRangeAbuse() {
        try {
            ArrayUtil.sizeOfDifferentRange(DATA_A, null, 0);
            assert false;
        }
        catch (IllegalArgumentException e) {
            // success
        }

        try {
            ArrayUtil.sizeOfDifferentRange(null, DATA_A, 0);
            assert false;
        }
        catch (IllegalArgumentException e) {
            // success
        }

        try {
            ArrayUtil.sizeOfDifferentRange(DATA_A, DATA_H, 0);
            assert false;
        }
        catch (IllegalArgumentException e) {
            // success
        }

        try {
            ArrayUtil.sizeOfDifferentRange(DATA_H, DATA_A, 0);
            assert false;
        }
        catch (IllegalArgumentException e) {
            // success
        }
    }
}
