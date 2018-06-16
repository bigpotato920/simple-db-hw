package simpledb;

import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 * 
 * @see simpledb.HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {

    private File f = null;
    private TupleDesc td = null;

    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td) {
        // some code goes here
        this.f = f;
        this.td = td;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // some code goes here
        return f;
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
    @Override
    public int getId() {
        // some code goes here
        return f.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    @Override
    public TupleDesc getTupleDesc() {
        // some code goes here
        return td;
    }

    // see DbFile.java for javadocs
    @Override
    public Page readPage(PageId pid) {
        // some code goes here
        long pageOffset = pid.getPageNumber() * BufferPool.getPageSize();
        byte []data = new byte[BufferPool.getPageSize()];

        try {
            RandomAccessFile randomAccessFile = new RandomAccessFile(this.f, "r");
            randomAccessFile.seek(pageOffset);
            randomAccessFile.read(data);

            randomAccessFile.close();
            return new HeapPage((HeapPageId) pid, data);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    // see DbFile.java for javadocs
    @Override
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for lab1
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // some code goes here
        return (int)(this.f.length() * 1.0 / BufferPool.getPageSize());
    }

    // see DbFile.java for javadocs
    @Override
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    @Override
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    @Override
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here
        return new HeapFileIterator(this, tid);
    }

    private class HeapFileIterator implements DbFileIterator {
        private HeapFile heapFile = null;
        private TransactionId transactionId = null;
        private int curPageIndex = 0;
        private int pageCount = 0;
        private HeapPage curPage = null;
        private Iterator<Tuple> curPageIter = null;

        HeapFileIterator(HeapFile heapFile, TransactionId tid) {
            this.heapFile = heapFile;
            this.transactionId = tid;
        }

        @Override
        public void open() throws DbException, TransactionAbortedException {
            pageCount = heapFile.numPages();
            curPage = getHeapPage(curPageIndex, transactionId);
            curPageIter = curPage.iterator();
        }

        @Override
        public boolean hasNext() throws DbException, TransactionAbortedException {
            if (curPageIter == null) {
                return false;
            }

            if (curPageIter.hasNext()) {
                return true;
            }

            curPageIndex++;
            while (curPageIndex < pageCount) {
                curPage = getHeapPage(curPageIndex, transactionId);
                curPageIter = curPage.iterator();
                if (curPageIter.hasNext()) {
                    return true;
                }
            }

            return false;
        }

        @Override
        public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
            if (curPageIter == null) {
                throw new NoSuchElementException();
            }
            return curPageIter.next();
        }

        @Override
        public void rewind() throws DbException, TransactionAbortedException {
            close();
            open();
        }

        @Override
        public void close() {
            curPage = null;
            curPageIter = null;
            curPageIndex = 0;
        }

        private HeapPage getHeapPage(int pageIndex, TransactionId transactionId) throws TransactionAbortedException, DbException {
            HeapPageId heapPageId = new HeapPageId(heapFile.getId(), pageIndex);
            return  (HeapPage) Database.getBufferPool().getPage(transactionId, heapPageId, Permissions.READ_ONLY);
        }
    }
}

