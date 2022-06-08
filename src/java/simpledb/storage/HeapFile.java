package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Permissions;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 * 
 * @see HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {

    private final File file;

    private final TupleDesc tupleDesc;

    class HeapFileIterator implements DbFileIterator {

        private final BufferPool bufferPool;

        private final TransactionId transactionId;

        private int currentPageNumber;

        private Iterator<Tuple> tupleIterator;

        public HeapFileIterator(TransactionId transactionId) {
            bufferPool = Database.getBufferPool();
            this.transactionId = transactionId;
            currentPageNumber = numPages();
            tupleIterator = null;
        }

        @Override
        public void open() throws DbException, TransactionAbortedException {
            currentPageNumber = -1;
            tupleIterator = null;
        }

        @Override
        public boolean hasNext() throws DbException, TransactionAbortedException {

            while (tupleIterator == null || !tupleIterator.hasNext()) {
                ++currentPageNumber;
                if (currentPageNumber < numPages()) {
                    Page page = bufferPool.getPage(
                            transactionId,
                            new HeapPageId(getId(), currentPageNumber),
                            Permissions.READ_ONLY
                    );

                    if (page == null) {
                        tupleIterator = null;
                        return false;
                    } else {
                        HeapPage heapPage = (HeapPage) page;
                        tupleIterator = heapPage.iterator();
                    }
                } else {
                    return false;
                }
            }

            return true;
        }

        @Override
        public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            return tupleIterator.next();
        }

        @Override
        public void rewind() throws DbException, TransactionAbortedException {
            currentPageNumber = -1;
            tupleIterator = null;
        }

        @Override
        public void close() {
            currentPageNumber = numPages();
            tupleIterator = null;
        }
    }

    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param file
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File file, TupleDesc tupleDesc) {
        // some code goes here
        this.file = file;
        this.tupleDesc = tupleDesc;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // some code goes here
        return file;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere to ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     * 
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        // some code goes here
        return file.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return tupleDesc;
    }

    // see DbFile.java for javadocs
    @Override
    public Page readPage(PageId pageId) {
        // some code goes here
        HeapPageId heapPageId = new HeapPageId(pageId.getTableId(), pageId.getPageNumber());
        int offset = BufferPool.getPageSize() * heapPageId.getPageNumber();
        byte[] data = new byte[BufferPool.getPageSize()];
        try {
            RandomAccessFile randomAccessFile = new RandomAccessFile(file, "r");
            randomAccessFile.seek(offset);
            randomAccessFile.readFully(data);
            return new HeapPage(heapPageId, data);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for lab1
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // some code goes here
        return (int) (file.length() / BufferPool.getPageSize());
    }

    // see DbFile.java for javadocs
    public List<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId transactionId) {
        // some code goes here
        return new HeapFileIterator(transactionId);
    }

}

