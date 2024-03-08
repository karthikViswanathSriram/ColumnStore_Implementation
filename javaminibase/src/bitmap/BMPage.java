package bitmap;

import java.io.*;
import global.*;
import diskmgr.*;

/**
 * Class heap file page.
 * The design assumes that records are kept compacted when
 * deletions are performed.
 */

public class BMPage extends Page implements GlobalConst {

    public static final int DPFIXED = 2 * 2 + 3 * 4;
    public static final int MAX_BITS = (MAX_SPACE - DPFIXED) * 8;

    // Removed TYPE, SLOT_CNT, USED_PTR from page metadata as they are used in heap
    // pages only.
    public static final int BIT_COUNTER = 0;
    public static final int FREE_SPACE = 2;
    public static final int PREV_PAGE = 4;
    public static final int NEXT_PAGE = 8;
    public static final int CUR_PAGE = 12;

    /*
     * Warning:
     * These items must all pack tight, (no padding) for
     * the current implementation to work properly.
     * Be careful when modifying this class.
     */

    /**
     * number of bytes used
     */
    private short bitCounter;

    /**
     * number of bytes free in data[]
     */
    private short freeSpace;

    /**
     * backward pointer to data page
     */
    private PageId prevPage = new PageId();

    /**
     * forward pointer to data page
     */
    private PageId nextPage = new PageId();

    /**
     * page number of this page
     */
    protected PageId curPage = new PageId();

    /**
     * Default constructor
     */

    public BMPage() {

    }

    /**
     * Constructor of class BMPage
     * open a BMPage and make this BMPage point to the given page
     * 
     * @param page the given page in Page type
     */

    public BMPage(Page page) {
        data = page.getpage();
    }

    /**
     * Constructor of class HFPage
     * open a existed hfpage
     * 
     * @param apage a page in buffer pool
     */

    public void openBMpage(Page apage) {
        data = apage.getpage();
    }

    /**
     * Initialize a new page
     * 
     * @param pageNo the page number of a new page to be initialized
     * @param apage  the Page to be initialized
     * @see Page
     * @exception IOException I/O errors
     */

    public void init(PageId pageNo, Page apage)
            throws IOException {
        data = apage.getpage();

        bitCounter = (short) 0;
        Convert.setShortValue(bitCounter, BIT_COUNTER, data);

        freeSpace = (short) MAX_BITS; // amount of space available
        Convert.setShortValue(freeSpace, FREE_SPACE, data);

        curPage.pid = pageNo.pid;
        Convert.setIntValue(curPage.pid, CUR_PAGE, data);

        nextPage.pid = prevPage.pid = INVALID_PAGE;
        Convert.setIntValue(prevPage.pid, PREV_PAGE, data);
        Convert.setIntValue(nextPage.pid, NEXT_PAGE, data);

        // Setting empty bytes to 0
        for (Integer i = DPFIXED; i < MAX_SPACE; i++) {
            Convert.setByteValue((byte) 0, i, data);
        }

    }

    /**
     * @return byte array
     */

    public byte[] getBMpageArray() {
        return data;
    }

    /**
     * Dump contents of a page
     * 
     * @exception IOException I/O errors
     */
    public void dumpPage()
            throws IOException {
        int i;

        bitCounter = Convert.getShortValue(BIT_COUNTER, data);
        freeSpace = Convert.getShortValue(FREE_SPACE, data);
        curPage.pid = Convert.getIntValue(CUR_PAGE, data);
        nextPage.pid = Convert.getIntValue(NEXT_PAGE, data);

        System.out.println("dumpPage");
        System.out.println("curPage= " + curPage.pid);
        System.out.println("nextPage= " + nextPage.pid);
        System.out.println("freeSpace= " + freeSpace);
        System.out.println("byteCnt= " + bitCounter);

        for (i = DPFIXED; i < DPFIXED + bitCounter; i++) {
            Byte val = Convert.getByteValue(i, data);
            System.out.print("Position: " + i + "Value: " + val);

        }

    }

    /**
     * @return PageId of previous page
     * @exception IOException I/O errors
     */
    public PageId getPrevPage()
            throws IOException {
        prevPage.pid = Convert.getIntValue(PREV_PAGE, data);
        return prevPage;
    }

    /**
     * sets value of prevPage to pageNo
     * 
     * @param pageNo page number for previous page
     * @exception IOException I/O errors
     */
    public void setPrevPage(PageId pageNo)
            throws IOException {
        prevPage.pid = pageNo.pid;
        Convert.setIntValue(prevPage.pid, PREV_PAGE, data);
    }

    /**
     * @return page number of next page
     * @exception IOException I/O errors
     */
    public PageId getNextPage()
            throws IOException {
        nextPage.pid = Convert.getIntValue(NEXT_PAGE, data);
        return nextPage;
    }

    /**
     * sets value of nextPage to pageNo
     * 
     * @param pageNo page number for next page
     * @exception IOException I/O errors
     */
    public void setNextPage(PageId pageNo)
            throws IOException {
        nextPage.pid = pageNo.pid;
        Convert.setIntValue(nextPage.pid, NEXT_PAGE, data);
    }

    /**
     * @return page number of current page
     * @exception IOException I/O errors
     */
    public PageId getCurPage()
            throws IOException {
        curPage.pid = Convert.getIntValue(CUR_PAGE, data);
        return curPage;
    }

    /**
     * sets value of curPage to pageNo
     * 
     * @param pageNo page number for current page
     * @exception IOException I/O errors
     */
    public void setCurPage(PageId pageNo)
            throws IOException {
        curPage.pid = pageNo.pid;
        Convert.setIntValue(curPage.pid, CUR_PAGE, data);
    }

    /**
     * @return bitCounter used in this page
     * @exception IOException I/O errors
     */
    public short getbitCounter()
            throws IOException {
        bitCounter = Convert.getShortValue(BIT_COUNTER, data);
        return bitCounter;
    }

    /**
     * sets value of bitCounter to byteCtr
     * 
     * @param byteCtr number of bytes used in this page
     * @exception IOException I/O errors
     */
    public void setbitCounter(short byteCtr)
            throws IOException {
        bitCounter = byteCtr;
        Convert.setShortValue(bitCounter, BIT_COUNTER, data);
    }

    /**
     * returns the amount of available space on the page.
     * 
     * @return the amount of available space on the page
     * @exception IOException I/O errors
     */
    public int available_space()
            throws IOException {
        freeSpace = Convert.getShortValue(FREE_SPACE, data);
        return freeSpace;
    }

    /**
     * Determining if the page is empty
     * 
     * @return true if the BMPage is has no data in it, false otherwise
     * @exception IOException I/O errors
     */
    public boolean empty()
            throws IOException {

        Short byteCtr = getbitCounter();
        if (byteCtr == 0) {
            return true;
        }
        return false;
    }

    public void updateCounter(int value) throws IOException {
        bitCounter = Convert.getShortValue(BIT_COUNTER, data);
        bitCounter += value;
        Convert.setShortValue(bitCounter, BIT_COUNTER, data);
        freeSpace = Convert.getShortValue(FREE_SPACE, data);
        freeSpace -= value;
        Convert.setShortValue(freeSpace, FREE_SPACE, data);
    }

    /**
     * Compacts the data in a BMPage
     * 
     * @exception IOException I/O errors
     */
    public void writeBMPageArray(Byte[] byteArray) throws IOException {

        for (int i = 0; i < byteArray.length; i++) {
            Convert.setByteValue(byteArray[i], i, data);
        }

    }

}
