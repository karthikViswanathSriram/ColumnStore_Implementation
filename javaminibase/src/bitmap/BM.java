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

    public static void traverseBitMap(BitMapFile bitMapFile) throws Exception {
        BitMapHeaderPage header = bitMapFile.getHeaderPage();
        if (header == null) {
            System.out.println("Header is empty");
            return;
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

        while (true) {
            System.out.println("Page ID: " + bmPage.getCurPage().pid);
            System.out.println("Prev Page ID: " + bmPage.getPrevPage().pid);
            System.out.println("Next Page ID: " + bmPage.getNextPage().pid);
            System.out.println("Counter: " + bmPage.getCounter());

            if (bmPage.getNextPage().pid == INVALID_PAGE) {
                break;
            } else {
                page = pinPage(bmPage.getNextPage());
                pagesPinned.add(bmPage.getNextPage());
                bmPage = new BMPage(page);
            }
        }
        for (PageId pageId : pagesPinned) {
            unpinPage(pageId);
        }
    }

    public static void printBitMap(BitMapHeaderPage header) throws Exception {
        if (header == null) {
            System.out.println("Header is empty");
            return;
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
                bmPage = new BMPage(page);
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
