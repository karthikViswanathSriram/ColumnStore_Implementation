/*Changes have to be made to this file*/

package iterator;

import columnar.Columnarfile;
import global.AttrType;
import heap.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class ColumnarScanUtils {
    /*
    * Gets the attribute string sizes from the coulumar file
    * and required for the seting the tuple header for the projection
    * */
    public static short[] getTargetColumnStringSizes(Columnarfile columnarfile, short[] targetedCols) {
        short  str_attr_size = 27;
        short[] str_sizes = new short[targetedCols.length];
        Arrays.fill(str_sizes, str_attr_size);
        return str_sizes;
        // return columnarfile.getStrSize(targetedCols);
    }

    /*
    * Gets the attribute types of the target columns for the columnar file
    * Is used while setting the Tuple header for the projection
    *
    * */
    public static AttrType[] getTargetColumnAttributeTypes(Columnarfile columnarfile, short[] targetedCols) {
        AttrType[] attributes = columnarfile.get_AttrTypes();
        AttrType[] targetAttrTypes = new AttrType[targetedCols.length];
        for (int i = 0; i < targetAttrTypes.length; i++) {
            targetAttrTypes[i] = attributes[targetedCols[i]];
        }
        return targetAttrTypes;
    }

    // open the targeted column heap files and store those reference for scanning
    public static Heapfile[] getTargetHeapFiles(Columnarfile columnarfile,  short[] targetedCols) throws HFException, HFBufMgrException, HFDiskMgrException, IOException {
        Heapfile[] targetHeapFiles = new Heapfile[targetedCols.length];
        for (int i = 0; i < targetedCols.length; i++) {
            targetHeapFiles[i] = columnarfile.getColumn(targetedCols[i]);
        }
        return targetHeapFiles;
    }

    public static Tuple getProjectionTuple(Columnarfile columnarfile, FldSpec[] fldSpecs, short[] targetedCols) throws Exception {

        AttrType[] types = new AttrType[fldSpecs.length];
        List<Short> sizes = new ArrayList<>();
        for (int i = 0; i < fldSpecs.length; i++) {
            switch (fldSpecs[i].relation.key) {
                case RelSpec.outer:      // Field of outer (t1)
                    types[i] = columnarfile.getAttrtypeforcolumn(targetedCols[fldSpecs[i].offset-1]);
                    if(types[i].attrType == AttrType.attrString)
                        sizes.add(columnarfile.get_attr_size(targetedCols[fldSpecs[i].offset-1]));
                    break;
                default:
                    throw new WrongPermat("something is wrong in perm_mat");

            }
        }

        short[] strSizes = new short[sizes.size()];
        for (int i = 0; i < strSizes.length; i++) {
            strSizes[i] = sizes.get(i);
        }

        Tuple jTuple = new Tuple();
        jTuple.setHdr((short)fldSpecs.length, types, strSizes);
        jTuple = new Tuple(jTuple.size());
        jTuple.setHdr((short)fldSpecs.length, types, strSizes);
        return jTuple;
    }

    public static Tuple getProjectionTuple(Columnarfile outer, Columnarfile inner, FldSpec[] fldSpecs, short[] innerCols, short[] outerCols) throws Exception {

        AttrType[] types = new AttrType[fldSpecs.length];
        List<Short> sizes = new ArrayList<>();
        for (int i = 0; i < fldSpecs.length; i++) {
            switch (fldSpecs[i].relation.key) {
                case RelSpec.outer:      // Field of outer (t1)
                    types[i] = outer.getAttrtypeforcolumn(fldSpecs[i].offset-1);
                    if(types[i].attrType == AttrType.attrString)
                        sizes.add(outer.get_attr_size(outerCols[fldSpecs[i].offset-1]));
                    break;
                case RelSpec.innerRel:      // Field of outer (t1)
                    types[i] = inner.getAttrtypeforcolumn(fldSpecs[i].offset-1);
                    if(types[i].attrType == AttrType.attrString)
                        sizes.add(inner.get_attr_size(innerCols[fldSpecs[i].offset-1]));
                    break;
                default:
                    throw new WrongPermat("something is wrong in perm_mat");

            }
        }

        short[] strSizes = new short[sizes.size()];
        for (int i = 0; i < strSizes.length; i++) {
            strSizes[i] = sizes.get(i);
        }

        Tuple jTuple = new Tuple();
        jTuple.setHdr((short)fldSpecs.length, types, strSizes);
        jTuple = new Tuple(jTuple.size());
        jTuple.setHdr((short)fldSpecs.length, types, strSizes);
        return jTuple;
    }
}