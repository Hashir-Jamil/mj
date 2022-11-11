
        package iterator;

        import heap.*;
        import global.*;
        import diskmgr.*;
        import bufmgr.*;
        import index.*;
        import java.io.*;
        import java.lang.reflect.Array;

        import btree.*;
        import chainexception.*;
        import diskmgr.*;
        import iterator.*;

/*==========================================================================*/
/**
 * Sort-merge join.
 * Call the two relations being joined R (outer) and S (inner).  This is an
 * implementation of the naive sort-merge join algorithm.  The external
 * sorting utility is used to generate runs.  Then the iterator interface
 * is called to fetch successive tuples for the final merge with joining.
 */

public class SortMerge extends Iterator implements GlobalConst {
    AttrType[] in1;
    int len_in1;
    short[] s1_sizes;
    AttrType[] in2;
    int len_in2;
    short[] s2_sizes;

    int join_col_in1;
    int sortFld1Len;
    int join_col_in2;
    int sortFld2Len;

    int amt_of_mem;
    Iterator am1;
    Iterator am2;

    boolean in1_sorted;
    boolean in2_sorted;
    TupleOrder order;
    CondExpr[] outFilter;
    FldSpec[] proj_list;
    int n_out_flds;
    private Iterator outerStream,innerStream;

    /**
     * constructor,initialization
     *
     * @param in1          Array containing field types of R
     * @param len_in1      # of columns in R
     * @param s1_sizes     Shows the length of the string fields in R.
     * @param in2          Array containing field types of S
     * @param len_in2      # of columns in S
     * @param s2_sizes     Shows the length of the string fields in S
     * @param sortFld1Len  The length of sorted field in R
     * @param sortFld2Len  The length of sorted field in S
     * @param join_col_in1 The col of R to be joined with S
     * @param join_col_in2 The col of S to be joined with R
     * @param amt_of_mem   Amount of memory available, in pages
     * @param am1          Access method for left input to join
     * @param am2          Access method for right input to join
     * @param in1_sorted   Is am1 sorted?
     * @param in2_sorted   Is am2 sorted?
     * @param order        The order of the tuple: assending or desecnding?
     * @param outFilter    Ptr to the output filter
     * @param proj_list    Shows what input fields go where in the output tuple
     * @param n_out_flds   Number of outer relation fileds
     * @throws JoinNewFailed             Allocate failed
     * @throws JoinLowMemory             Memory not enough
     * @throws SortException             Exception from sorting
     * @throws TupleUtilsException       Exception from using tuple utils
     * @throws JoinsException            Exception reading stream
     * @throws IndexException            Exception...
     * @throws InvalidTupleSizeException Exception...
     * @throws InvalidTypeException      Exception...
     * @throws PageNotReadException      Exception...
     * @throws PredEvalException         Exception...
     * @throws LowMemException           Exception...
     * @throws UnknowAttrType            Exception...
     * @throws UnknownKeyTypeException   Exception...
     * @throws IOException               Some I/O fault
     * @throws Exception                 Generic
     */

    public SortMerge(
            AttrType[] in1,
            int len_in1,
            short[] s1_sizes,
            AttrType[] in2,
            int len_in2,
            short[] s2_sizes,

            int join_col_in1,
            int sortFld1Len,
            int join_col_in2,
            int sortFld2Len,

            int amt_of_mem,
            Iterator am1,
            Iterator am2,

            boolean in1_sorted,
            boolean in2_sorted,
            TupleOrder order,

            CondExpr[] outFilter,
            FldSpec[] proj_list,
            int n_out_flds
    )
            throws JoinNewFailed,
            JoinLowMemory,
            SortException,
            TupleUtilsException,
            JoinsException,
            IndexException,
            InvalidTupleSizeException,
            InvalidTypeException,
            PageNotReadException,
            PredEvalException,
            LowMemException,
            UnknowAttrType,
            UnknownKeyTypeException,
            IOException,
            Exception {
        this.in1 = in1;
        this.len_in1 = len_in1;
        this.s1_sizes = s1_sizes;
        this.in2 = in2;
        this.len_in2 = len_in2;
        this.s2_sizes = s2_sizes;
        this.join_col_in1 = join_col_in1;
        this.sortFld1Len = sortFld1Len;
        this.join_col_in2 = join_col_in2;
        this.sortFld2Len = sortFld2Len;
        this.amt_of_mem = amt_of_mem;
        this.am1 = am1;
        this.am2 = am2;
        this.in1_sorted = in1_sorted;
        this.in2_sorted = in2_sorted;
        this.order = order;
        this.outFilter = outFilter;
        this.proj_list = proj_list;
        this.n_out_flds = n_out_flds;

        //check for sorting --> 1 corresponds to outer and 2 corresponds to inner from here on out
        //if sorted just reference the private streams to the public iterators
        if (in1_sorted) {
            outerStream = am1;
        }
        if (in2_sorted) {
            innerStream = am2;
        }

        //if not sorted then make new Sort objects for the streams.
        if (!in1_sorted) {
            outerStream = new Sort(in1, (short) (in1.length), s1_sizes, am1, join_col_in1, order,
                sortFld1Len, amt_of_mem);
        }
        if (!in2_sorted) {
            innerStream = new Sort(in2, (short) in2.length, s2_sizes, am2, join_col_in2, order,
                sortFld2Len, amt_of_mem);
        }

        //prepare the tuples for the join output, inner and outer inputs
        Tuple joinTuple = new Tuple();
        AttrType[] joinTypes = new AttrType[n_out_flds];
        short[] joinTupleStrSize = null;

        joinTupleStrSize = TupleUtils.setup_op_tuple(
            joinTuple,
            joinTypes,
            in1,
            len_in1,
            in2,
            len_in2,
            s1_sizes,
            s2_sizes,
            proj_list,
            n_out_flds
        );

        Tuple tupleOuter = new Tuple();
        tupleOuter.setHdr((short) in1.length,in1,s1_sizes);

        Tuple tupleInner = new Tuple();
        tupleInner.setHdr((short) in2.length,in2,s2_sizes);

        System.out.println("\n Non Sorted File \n");
//        Tuple presorted = am1.get_next();
//        do{
////          System.out.println(presorted.returnTupleByteArray().length);
//            System.out.println(new String(presorted.returnTupleByteArray()));
//            presorted.tupleCopy(new Tuple());
//            System.out.println("\n");
//        }
//        while (readTuple(presorted, am1));


        //create sorters and tuples
//        Sort sorter1 = new Sort(in1, (short) (in1.length), s1_sizes, am1, join_col_in1, order,
//                sortFld1Len, amt_of_mem);
//        Tuple tuple1 = sorter1.get_next();
//
//        System.out.println("\n Sorted File \n");
//        while(tuple1 != null){
//
//            System.out.println(new String(tuple1.returnTupleByteArray()));
//            tuple1.tupleCopy(new Tuple());
//            System.out.println("\n");
//            tuple1 = sorter1.get_next();
//        }
//        sorter1.close();
        System.out.println("finished sort");
//
//        Sort sorter2 = new Sort(in2, (short) in2.length, s2_sizes, am2, join_col_in2, order,
//                sortFld2Len, amt_of_mem);
//        Tuple tuple2 = sorter2.get_next();
//        sorter2.close();


    } // End of SortMerge constructor

    /*--------------------------------------------------------------------------*/

    /**
     * Reads a tuple from a stream in a less painful way.
     */
    private boolean readTuple(
            Tuple tuple,
            Iterator tupleStream
    )
            throws JoinsException,
            IndexException,
            UnknowAttrType,
            TupleUtilsException,
            InvalidTupleSizeException,
            InvalidTypeException,
            PageNotReadException,
            PredEvalException,
            SortException,
            LowMemException,
            UnknownKeyTypeException,
            IOException,
            Exception {
        Tuple temp;
        temp = tupleStream.get_next();
        if (temp != null) {
            tuple.tupleCopy(temp);
            return true;
        } else {
            return false;
        }
    } // End of readTuple

    /*--------------------------------------------------------------------------*/

    /**
     * Return the next joined tuple.
     *
     * @return the joined tuple is returned
     * @throws IOException               I/O errors
     * @throws JoinsException            some join exception
     * @throws IndexException            exception from super class
     * @throws InvalidTupleSizeException invalid tuple size
     * @throws InvalidTypeException      tuple type not valid
     * @throws PageNotReadException      exception from lower layer
     * @throws TupleUtilsException       exception from using tuple utilities
     * @throws PredEvalException         exception from PredEval class
     * @throws SortException             sort exception
     * @throws LowMemException           memory error
     * @throws UnknowAttrType            attribute type unknown
     * @throws UnknownKeyTypeException   key type unknown
     * @throws Exception                 other exceptions
     */

    public Tuple get_next()
            throws IOException,
            JoinsException,
            IndexException,
            InvalidTupleSizeException,
            InvalidTypeException,
            PageNotReadException,
            TupleUtilsException,
            PredEvalException,
            SortException,
            LowMemException,
            UnknowAttrType,
            UnknownKeyTypeException,
            Exception {
        Tuple r;
        Tuple s;

        return null;
    } // End of get_next

    /*--------------------------------------------------------------------------*/

    /**
     * implement the abstract method close() from super class Iterator
     * to finish cleaning up
     *
     * @throws IOException    I/O error from lower layers
     * @throws JoinsException join error from lower layers
     * @throws IndexException index access error
     */

    public void close()
            throws JoinsException,
            IOException, IndexException, SortException {
        if (!this.closeFlag) {
            try {
                this.am1.close();
                this.am2.close();
                closeFlag = true;
            } catch (JoinsException je) {
                throw new JoinsException(je, "Sort.java: error in closing iterator.");
            } catch (IOException ioe) {
                throw new IOException("Sort.java: error in closing iterator.");
            } catch (IndexException iE) {
                throw new IndexException();
            } catch (SortException e) {
                throw new SortException("Sort.java: error in closing iterator.");
            }
        }
    } // End of close
}