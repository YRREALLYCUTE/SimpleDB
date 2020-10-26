package simpledb;

import javax.xml.crypto.Data;
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


    private File file;

    private TupleDesc td;
    /**
     * Constructs a heap file backed by the specified file.
     *
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td) {
        // some code goes here
        this.file = f;
        this.td = td;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     *
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // some code goes here
        return this.file;
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
        return this.td;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        // some code goes here
        // 通过pid计算偏移量，然后读取一个页
        try {
            RandomAccessFile randomAccessFile = new RandomAccessFile(file, "r");

            // 计算偏移量
            int pgno = pid.getPageNumber();
            int pageSize = Database.getBufferPool().getPageSize();

            int offset = pgno * pageSize;
            // 读取一个pagesize的内容
            byte[] buffer = new byte[pageSize];

            randomAccessFile.seek(offset);
            randomAccessFile.read(buffer);
            HeapPage heapPage = new HeapPage(new HeapPageId(pid.getTableId(), pgno), buffer);
            return heapPage;
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
        long fileLen = file.length();
        return (int) Math.floor((double)fileLen/Database.getBufferPool().getPageSize());
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
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
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here
        return new DbFileIterator() {
            int pages = numPages();
            int readingPage;
            PageId readingPid;
            Page page;
            Iterator<Tuple> it;
            @Override
            public void open() throws DbException, TransactionAbortedException {
                readingPage=0;
                readingPid = new HeapPageId(getId(), readingPage);
                page = Database.getBufferPool().getPage(tid, readingPid, Permissions.READ_ONLY);
                it = ((HeapPage)page).iterator();
            }

            @Override
            public boolean hasNext() throws DbException, TransactionAbortedException {
                if(it == null)
                    return false;
                if(readingPage < pages - 1) {
                    boolean hasNextTupleInPage = it.hasNext();
                    if(!hasNextTupleInPage){
                        Database.getBufferPool().releasePage(tid, readingPid);
                        readingPage++;
                        readingPid = new HeapPageId(getId(), readingPage);
                        page = Database.getBufferPool().getPage(tid, readingPid, Permissions.READ_ONLY);
                        it = ((HeapPage) page).iterator();
                    }
                    return true;
                }
                if(readingPage == pages-1) {
                   return it.hasNext();
                }
                return false;
            }

            @Override
            public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
                if(it == null)
                    throw new NoSuchElementException("iterator is not open");
                return it.next();
            }

            @Override
            public void rewind() throws DbException, TransactionAbortedException {
                readingPage=0;
                readingPid = new HeapPageId(getId(), readingPage);
                page = Database.getBufferPool().getPage(tid, readingPid, Permissions.READ_ONLY);
                it = ((HeapPage)page).iterator();
            }

            @Override
            public void close() {
                readingPage = pages+1;
                it = null;
                Database.getBufferPool().releasePage(tid, readingPid);
            }
        };
    }

}

