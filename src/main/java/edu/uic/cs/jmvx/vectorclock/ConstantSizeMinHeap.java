package edu.uic.cs.jmvx.vectorclock;

/**
 * Can't load a regular priority queue while constructing the vector clock
 * So here's a simple version of one
 * Code is modified from PriorityQueue which uses Object[] and a Comparator
 * That's been subbed out for just *positive* integers
 * Empty cells are given the sentinel value of -1
 */
public class ConstantSizeMinHeap {
    protected int[] queue;

    protected int size = 0;

    public ConstantSizeMinHeap(int maxSize) {
        queue = new int[maxSize];
        //init empty slots. Technically, we don't need to do this, but a sentinel is nice
        for(int i = 0; i < maxSize; i++){
            queue[i] = -1;
        }
        size = 0;
    }

    public ConstantSizeMinHeap(int from, int to) {
        size = to - from;
        queue = new int[size];
        for(int i = 0; i < size; i++){
            queue[i] = from + i;
        }
        //sort the queue!
        heapify();
    }

    public boolean offer(int e) {
        int i = size;
        if (i >= queue.length)
            throw new RuntimeException("Queue is out of space");
        size = i + 1;
        if (i == 0)
            queue[0] = e;
        else
            siftUp(i, e);
        return true;
    }

    public int poll() {
        if (size == 0)
            throw new RuntimeException("Queue is empty");
        int s = --size;
        int result = queue[0];
        int x = queue[s];
        queue[s] = -1;
        if (s != 0)
            siftDown(0, x);
        return result;
    }

    private int indexOf(int o) {
        for (int i = 0; i < size; i++)
            if (o == queue[i])
                return i;
        return -1;
    }

    public boolean remove(int o) {
        int i = indexOf(o);
        if (i == -1)
            return false;
        else {
            removeAt(i);
            return true;
        }
    }

    private int removeAt(int i) {
        // assert i >= 0 && i < size;
        int s = --size;
        if (s == i) // removed last element
            queue[i] = -1;
        else {
            int moved = queue[s];
            queue[s] = -1;
            siftDown(i, moved);
            if (queue[i] == moved) {
                siftUp(i, moved);
                if (queue[i] != moved)
                    return moved;
            }
        }
        return -1;
    }

    private void siftUp(int k, int x) {
        while (k > 0) {
            int parent = (k - 1) >>> 1;
            int e = queue[parent];
            if (x >= e)
                break;
            queue[k] = e;
            k = parent;
        }
        queue[k] = x;
    }

    private void siftDown(int k, int x) {
        int half = size >>> 1;
        while (k < half) {
            int child = (k << 1) + 1;
            int c = queue[child];
            int right = child + 1;
            if (right < size && c > queue[right])
                c = queue[child = right];
            if (x <= c)
                break;
            queue[k] = c;
            k = child;
        }
        queue[k] = x;
    }

    private void heapify() {
        for (int i = (size >>> 1) - 1; i >= 0; i--)
            siftDown(i, queue[i]);
    }

    public boolean isEmpty(){
        return size == 0;
    }
}
