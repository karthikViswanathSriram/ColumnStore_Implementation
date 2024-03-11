package bitmap;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;

import btree.PinPageException;
import btree.UnpinPageException;
import diskmgr.Page;
import global.GlobalConst;
import global.PageId;
import global.SystemDefs;

public class BM implements GlobalConst {

    public BM() {
    }

    public void printBitMap(BitMapHeaderPage header) throws Exception {
        if (header == null) {
            throw new Exception("Header is null");
        }
        List<PageId> pagesPinned = new ArrayList<>();

        System.out.println("Header Page ID: " + header.getPageId().pid);
        System.out.println("Columnar File Name: " + header.getColumnarFileName());
        System.out.println("Column Number: " + header.getColumnNumber());
        System.out.println("Attribute Type: " + header.getAttrType());
        System.out.println("Value: " + header.getValue());

        PageId firstBMPageId = header.get_rootId();
        if (firstBMPageId.pid == INVALID_PAGE) {
            System.out.println("No pages in the bitmap");
            return;
        }

        Page page = pinPage(firstBMPageId);
        pagesPinned.add(firstBMPageId);
        BMPage bmPage = new BMPage(page);

        int position = 0;
        while (true) {
            int bitCount = bmPage.getCounter();
            BitSet currentBitSet = BitSet.valueOf(bmPage.getBMpageArray());
            for (int i = 0; i < bitCount; i++) {
                System.out.println("Position: " + position + "   Value: " + (currentBitSet.get(i) ? 1 : 0));
                position++;
            }
            if (bmPage.getNextPage().pid == INVALID_PAGE) {
                break;
            } else {

                page = pinPage(bmPage.getNextPage());
                pagesPinned.add(bmPage.getNextPage());
                bmPage.openBMpage(page);
            }
        }
        for (PageId pageId : pagesPinned) {
            unpinPage(pageId);
        }

    }

    /***
     * Unpin the given page
     * 
     * @param pageno
     * @throws UnpinPageException
     */
    private static void unpinPage(PageId pageno) throws UnpinPageException {
        try {
            SystemDefs.JavabaseBM.unpinPage(pageno, false /* = not DIRTY */);
        } catch (Exception e) {
            e.printStackTrace();
            throw new UnpinPageException(e, "");
        }
    }

    /***
     * Pin the page passed as input
     * 
     * @param pageno
     * @return
     * @throws PinPageException
     */
    private static Page pinPage(PageId pageno) throws PinPageException {
        try {
            Page page = new Page();
            SystemDefs.JavabaseBM.pinPage(pageno, page, false/* Rdisk */);
            return page;
        } catch (Exception e) {
            e.printStackTrace();
            throw new PinPageException(e, "");
        }
    }

}
