package simpledb;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

/**
 * Each instance of HeapPage stores data for one page of HeapFiles and implements the Page interface that is used by
 * BufferPool.
 * 
 * @see HeapFile
 * @see BufferPool
 * 
 */
public class HeapPage implements Page {

    final HeapPageId pid;
    final TupleDesc td;
    final byte header[];
    final Tuple tuples[];
    final int numSlots;
    private final Map<Tuple, Integer> indexLookup;
    private final List<Integer> emptySlots;

    byte[] oldData;
    private final Byte oldDataLock = new Byte((byte) 0);

    /**
     * Create a HeapPage from a set of bytes of data read from disk. The format of a HeapPage is a set of header bytes
     * indicating the slots of the page that are in use, some number of tuple slots. Specifically, the number of tuples
     * is equal to:
     * <p>
     * floor((BufferPool.PAGE_SIZE*8) / (tuple size * 8 + 1))
     * <p>
     * where tuple size is the size of tuples in this database table, which can be determined via
     * {@link Catalog#getTupleDesc}. The number of 8-bit header words is equal to:
     * <p>
     * ceiling(no. tuple slots / 8)
     * <p>
     * 
     * @param id
     *            The id of the HeapPage
     * @param data
     *            The data for this HeapPage.
     * @throws IOException
     *             thrown if there is an IO error.
     * 
     * @see Database#getCatalog
     * @see Catalog#getTupleDesc
     * @see BufferPool#PAGE_SIZE
     */
    public HeapPage(HeapPageId id, byte[] data) throws IOException {
        pid = id;
        td = Database.getCatalog().getTupleDesc(id.getTableId());
        numSlots = getNumTuples();
        DataInputStream dis = new DataInputStream(new ByteArrayInputStream(data));
        indexLookup = new HashMap<Tuple, Integer>(numSlots, 1f);
        emptySlots = new ArrayList<Integer>();

        // allocate and read the header slots of this page
        header = new byte[getHeaderSize()];
        for (int i = 0; i < header.length; i++) {
            header[i] = dis.readByte();

            for (int j = 0; j < 8; j++) {
                // If a slot is empty, add it to the empty list.
                int val = i * 8 + j;
                if (!isSlotUsed(val)) {
                    emptySlots.add(val);
                }
            }
        }

        tuples = new Tuple[numSlots];
        try {
            // allocate and read the actual records of this page
            for (int i = 0; i < tuples.length; i++) {
                Tuple tup = readNextTuple(dis, i);
                tuples[i] = tup;

                if (tup != null) {
                    indexLookup.put(tup, i);
                }
            }
        } catch (NoSuchElementException e) {
            e.printStackTrace();
        }
        dis.close();

        setBeforeImage();
    }

    /**
     * Retrieve the number of tuples on this page.
     * 
     * @return the number of tuples on this page
     */
    private int getNumTuples() {
        return BufferPool.PAGE_SIZE * 8 / (td.getSize() * 8 + 1);
    }

    /**
     * Computes the number of bytes in the header of a page in a HeapFile with each tuple occupying tupleSize bytes
     * 
     * @return the number of bytes in the header of a page in a HeapFile with each tuple occupying tupleSize bytes
     */
    private int getHeaderSize() {
        return (int) Math.round(Math.ceil((double) numSlots / 8));
    }

    /**
     * Return a view of this page before it was modified -- used by recovery
     */
    @Override
    public HeapPage getBeforeImage() {
        try {
            byte[] oldDataRef = null;
            synchronized (oldDataLock) {
                oldDataRef = oldData;
            }
            return new HeapPage(pid, oldDataRef);
        } catch (IOException e) {
            e.printStackTrace();
            // should never happen -- we parsed it OK before!
            System.exit(1);
        }
        return null;
    }

    /**
     * @see simpledb.Page#setBeforeImage()
     */
    @Override
    public void setBeforeImage() {
        synchronized (oldDataLock) {
            oldData = getPageData().clone();
        }
    }

    /**
     * @return the PageId associated with this page.
     */
    @Override
    public HeapPageId getId() {
        return pid;
    }

    /**
     * Suck up tuples from the source file.
     * 
     * @param dis
     *            The data to read from.
     * @param slotId
     *            The id of the slot to set.
     * @return The requested Tuple.
     * @throws NoSuchElementException
     *             Thrown if there isn't another tuple.
     */
    private Tuple readNextTuple(DataInputStream dis, int slotId) {
        // if associated bit is not set, read forward to the next tuple, and
        // return null.
        if (!isSlotUsed(slotId)) {
            for (int i = 0; i < td.getSize(); i++) {
                try {
                    dis.readByte();
                } catch (IOException e) {
                    throw new NoSuchElementException("error reading empty tuple");
                }
            }
            return null;
        }

        // read fields in the tuple
        Tuple t = new Tuple(td);
        RecordId rid = new RecordId(pid, slotId);
        t.setRecordId(rid);
        try {
            for (int j = 0; j < td.numFields(); j++) {
                Field f = td.getFieldType(j).parse(dis);
                t.setField(j, f);
            }
        } catch (java.text.ParseException e) {
            e.printStackTrace();
            throw new NoSuchElementException("parsing error!");
        }

        return t;
    }

    /**
     * Generates a byte array representing the contents of this page. Used to serialize this page to disk.
     * <p>
     * The invariant here is that it should be possible to pass the byte array generated by getPageData to the HeapPage
     * constructor and have it produce an identical HeapPage object.
     * 
     * @see #HeapPage
     * @return A byte array correspond to the bytes of this page.
     */
    @Override
    public byte[] getPageData() {
        int len = BufferPool.PAGE_SIZE;
        ByteArrayOutputStream baos = new ByteArrayOutputStream(len);
        DataOutputStream dos = new DataOutputStream(baos);

        // create the header of the page
        for (byte element : header) {
            try {
                dos.writeByte(element);
            } catch (IOException e) {
                // this really shouldn't happen
                e.printStackTrace();
            }
        }

        // create the tuples
        for (int i = 0; i < tuples.length; i++) {

            // empty slot
            if (!isSlotUsed(i)) {
                for (int j = 0; j < td.getSize(); j++) {
                    try {
                        dos.writeByte(0);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }

                }
                continue;
            }

            // non-empty slot
            for (int j = 0; j < td.numFields(); j++) {
                Field f = tuples[i].getField(j);
                try {
                    f.serialize(dos);

                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

        // padding
        int zerolen = BufferPool.PAGE_SIZE - (header.length + td.getSize() * tuples.length); // - numSlots *
                                                                                             // td.getSize();
        byte[] zeroes = new byte[zerolen];
        try {
            dos.write(zeroes, 0, zerolen);
        } catch (IOException e) {
            e.printStackTrace();
        }

        try {
            dos.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }

        return baos.toByteArray();
    }

    /**
     * Static method to generate a byte array corresponding to an empty HeapPage. Used to add new, empty pages to the
     * file. Passing the results of this method to the HeapPage constructor will create a HeapPage with no valid tuples
     * in it.
     * 
     * @return The returned ByteArray.
     */
    public static byte[] createEmptyPageData() {
        int len = BufferPool.PAGE_SIZE;
        return new byte[len]; // all 0
    }

    /**
     * Delete the specified tuple from the page; the tuple should be updated to reflect that it is no longer stored on
     * any page.
     * 
     * @param t
     *            The tuple to delete
     * 
     * @throws DbException
     *             if this tuple is not on this page, or tuple slot is already empty.
     */
    public void deleteTuple(Tuple t) throws DbException {
        Integer i = indexLookup.remove(t);

        if (i == null) {
            throw new DbException("The tuple does not exist.");
        } else if (!isSlotUsed(i)) {
            throw new DbException("The tuple is already removed.");
        }

        markSlotUsed(i, false);
        t.setRecordId(null);
        tuples[i] = null;
    }

    /**
     * Adds the specified tuple to the page; the tuple should be updated to reflect that it is now stored on this page.
     * 
     * @param t
     *            The tuple to add.
     * 
     * @throws DbException
     *             if the page is full (no empty slots) or tupledesc is mismatch.
     */
    public void insertTuple(Tuple t) throws DbException {
        try {
            t.resetTupleDesc(td);
        } catch (IllegalArgumentException e) {
            throw new DbException("The tuple does not match the table schema.");
        }

        if (emptySlots.size() == 0) {
            throw new DbException("There is no empty slot.");
        }

        int slotId = emptySlots.remove(emptySlots.size() - 1); // Get next slot.
        RecordId rid = new RecordId(pid, slotId); // Update tuples rid.
        t.setRecordId(rid);
        tuples[slotId] = t;
        markSlotUsed(slotId, true);
    }

    /**
     * Marks this page as dirty/not dirty and record that transaction that did the dirtying
     */
    @Override
    public void markDirty(boolean dirty, TransactionId tid) {
        // some code goes here
        // not necessary for lab1
    }

    /**
     * Returns the tid of the transaction that last dirtied this page, or null if the page is not dirty
     */
    @Override
    public TransactionId isDirty() {
        // some code goes here
        // Not necessary for lab1
        return null;
    }

    /**
     * Returns the number of empty slots on this page.
     * 
     * @return The numbe of empty slots.
     */
    public int getNumEmptySlots() {
        return emptySlots.size();
    }

    /**
     * Returns true if associated slot on this page is filled.
     * 
     * @param i
     *            The index to check usage.
     * 
     * @return True if the slot is used, false otherwise.
     */
    public boolean isSlotUsed(int i) {
        int index = i / 8;
        int bit = i % 8;
        return (header[index] >> bit & 0x1) == 0x1;
    }

    /**
     * Abstraction to fill or clear a slot on this page.
     * 
     * @param i
     *            The index to change.
     * @param value
     *            The value to set the slot usage to.
     */
    private void markSlotUsed(int i, boolean value) {
        int index = i / 8;
        int bit = i % 8;
        byte cur = header[index];
        int mask = 0x1;
        mask <<= bit;

        if (value) {
            // Make sure I set the right index to a 1.
            cur |= (byte) mask;
        } else {
            // Make sure I set the right index to a 0 using a reverse mask.
            mask = ~mask;
            cur &= (byte) mask;
        }
        header[i] = cur;
    }

    /**
     * @return an iterator over all tuples on this page (calling remove on this iterator throws an
     *         UnsupportedOperationException) (note that this iterator shouldn't return tuples in empty slots!)
     */
    @Override
    public Iterator<Tuple> iterator() {
        return new HeapPageIterator();
    }

    /**
     * This iterator is greedy. It will find the next value immediately after returning the previous. This is possibly
     * less efficient if they don't do a full traversal, but it follows standard design patterns in that it hasNext() is
     * not a mutator.
     * 
     * @author Conor
     * 
     */
    private class HeapPageIterator implements Iterator<Tuple> {
        private int next;

        public HeapPageIterator() {
            next = -1;
            findNext();
        }

        /**
         * Finds the next used index.
         */
        private void findNext() {
            do {
                next++;
            } while (hasNext() && !isSlotUsed(next));
        }

        /**
         * @see java.util.Iterator#hasNext()
         */
        @Override
        public boolean hasNext() {
            return next < numSlots;
        }

        /**
         * @see java.util.Iterator#next()
         */
        @Override
        public Tuple next() {
            Tuple tmp = tuples[next];
            findNext();
            return tmp;
        }

        /**
         * @see java.util.Iterator#remove()
         */
        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }
    }
}
