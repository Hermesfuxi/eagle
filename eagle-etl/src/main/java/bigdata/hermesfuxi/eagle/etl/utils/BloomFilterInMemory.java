package bigdata.hermesfuxi.eagle.etl.utils;

import java.util.BitSet;

public class BloomFilterInMemory {
    protected BitSet bloom;

    public BloomFilterInMemory(int size) {
        bloom = new BitSet(size);
    }

    public synchronized boolean addElement(int element) {
        boolean added = false;
        if (!getBit(element)) {
            added = true;
            setBit(element, true);
        }
        return added;
    }

    public synchronized void clear() {
        bloom.clear();
    }

    public synchronized boolean contains(int element) {
        if (!getBit(element)) {
            return false;
        }
        return true;
    }

    protected boolean getBit(int index) {
        return bloom.get(index);
    }

    protected void setBit(int index, boolean to) {
        bloom.set(index, to);
    }

    public synchronized boolean isEmpty() {
        return bloom.isEmpty();
    }
}
