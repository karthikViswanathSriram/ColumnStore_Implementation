package interfaces;

import bufmgr.*;
import columnar.Columnarfile;
import diskmgr.PCounter;
import global.AttrType;
import global.SystemDefs;
import heap.Tuple;

import java.io.*;
import static global.GlobalConst.NUMBUF;

public class BatchInsert {
	private static final int NUM_PAGES = 10000;

	public static void main(String[] args) throws PageUnpinnedException, BufMgrException, IOException, PageNotFoundException, PagePinnedException, HashOperationException {
		/*
			Query Schema: DATAFILE COLUMNDB COLUMNARFILE NUMCOLUMNS 
		*/
		String dataFile = args[0];
		String columnDB = args[1];
		String columnarFile = args[2];
		Integer numColumns = Integer.parseInt(args[3]);
		String dbPath = HelperFunctions.dbPath(columnDB);
		int createDB = 0;

		
		int numPages = NUM_PAGES;

		File f = new File(dbPath);
		
		// Check if file exists 
		// if it doesn't don't create a new db
		if(f.exists() && !f.isDirectory()) 
		{ 
			System.out.println("File already exists");
			numPages = 0;	
		}
		else
		{
			System.out.println("File didn't exist hence creating");
		}

		if(args.length == 5)
		{
			if(Integer.parseInt(args[4]) == 1)
			{
				numPages = 0;
				System.out.println("Using existing DB");
			}
			else
			{
				numPages = NUM_PAGES;
				System.out.println("Creating new DB");
			}
		}
		
		
		SystemDefs sysDef = new SystemDefs(dbPath, numPages, NUMBUF, null);

		runBatchInsert(dataFile, columnarFile, numColumns);
		SystemDefs.JavabaseBM.flushAllPages();
		SystemDefs.JavabaseDB.closeDB();

		// Add the function to display the read and write count
		System.out.println("Read Count: " + PCounter.rcounter);
		System.out.println("Write Count: " + PCounter.wcounter);
	}

	private static void runBatchInsert(String dataFile, String columnarFile, int numColumns) throws IOException {

		FileInputStream filestream = null;
		BufferedReader br = null;
		try 
		{
			filestream = new FileInputStream(dataFile);
			br = new BufferedReader(new InputStreamReader(filestream));

			// Parameters for creating a columnar file
			AttrType[] types = new AttrType[numColumns];
			String[] names  = new String[numColumns];

			// First Line having column names and attribute types
			String attrType = br.readLine();
			String rows[] = attrType.split("\t");
			int i = 0;
			int numStrings = 0;
			for (String row: rows) 
			{
				String cols[] = row.split(":");
				names[i] = cols[0];
				if (cols[1].contains("char"))
				{
					types[i] = new AttrType(AttrType.attrString);
					// char(25) 
					// index 5 till end
					// sizes[i] = Short.parseShort(cols[1].substring(5, cols[1].length()-1));
					numStrings++;
				}
				else
				{
					types[i] = new AttrType(AttrType.attrInteger);
					// sizes[i] = 4;
				}
				i++;
			}

			short[] sizes = new short[numStrings];
			i = 0;
			for (String row: rows)
			{
				String cols[] = row.split(":");
				if (cols[1].contains("char"))
				{
					sizes[i] = Short.parseShort(cols[1].substring(5, cols[1].length()-1)); 
					i++;
				}
			}

			Columnarfile cf = new Columnarfile(columnarFile, numColumns, types, sizes, names);

			int tupleCount = 0;
			// Second row (line)
			String line = br.readLine();
			while (line != null)   
			{
				String colValues[] = line.split("\t");

				Tuple t = new Tuple();
				t.setHdr((short)numColumns, types, sizes);
				int size = t.size();
				t = new Tuple(size);
				t.setHdr((short)numColumns, types, sizes);

				int j = 0;
				for (String colVal: colValues) 
				{
					switch(types[j].attrType)
					{
						case 0:
							t.setStrFld(j+1, colVal);
							break;
						case 1:
							t.setIntFld(j+1, Integer.parseInt(colVal));
							break;
						default:
							break;
					}
					j++;
				}

				// Inset tuple to the columnar file
				byte[] tupleByteArray = t.getTupleByteArray();
				cf.insertTuple(tupleByteArray);
				tupleCount++;

				// Read next line
				line = br.readLine();
			}
			cf.close();
			br.close();
			System.out.println(tupleCount +" tuples inserted to the columnar file "+ columnarFile + "\n");

		} 
		catch (Exception e) 
		{
			e.printStackTrace();
		} 
		finally 
		{
			filestream.close();
			br.close();
		}
	}
}