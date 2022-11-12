
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
    private byte[][] matchSpace = new byte[1][MINIBASE_PAGESIZE];
    private Heapfile innerHeap;
    private IoBuf matchBuff;
    private Tuple tupleOuter, tupleInner, joinTuple, currentOutterTuple;
    private boolean getNextOut = true;
    private boolean getNextIn = true;

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

        //check if enough memory blocks available
        if(amt_of_mem < 2){
            throw new JoinLowMemory("memory too low");
        }
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
        joinTuple = new Tuple();
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

        tupleOuter = new Tuple();
        tupleOuter.setHdr((short) in1.length,in1,s1_sizes);

        currentOutterTuple = new Tuple();
        currentOutterTuple.setHdr((short) in1.length,in1,s1_sizes);


        this.tupleInner = new Tuple();
        this.tupleInner.setHdr((short) in2.length,in2,s2_sizes);

        //create IO Buffer for inner table
        matchBuff = new IoBuf();
        innerHeap = null;
        try {
            innerHeap = new Heapfile(null);
        }
        catch(Exception e) {
            throw new SortException (e, "Creating heapfile for IoBuf use failed.");
        }
        matchBuff.init(matchSpace, 1, tupleInner.size(),innerHeap);

        //check if these objects were successfully made
        if(matchBuff == null || tupleInner == null|| tupleOuter == null || joinTuple == null){
            throw new JoinNewFailed("Buffer pool not successfully allocated");
        }

        //load the buffer with inner tuples
        tupleInner = innerStream.get_next();
        while(tupleInner != null){
            matchBuff.Put(tupleInner);
            tupleInner = innerStream.get_next();
        }

        //reset for further use
        this.tupleInner = new Tuple();
        this.tupleInner.setHdr((short) in2.length,in2,s2_sizes);

        if(tupleInner == null){
            throw new JoinNewFailed("Buffer pool not successfully allocated");
        }
    } // End of SortMerge constructor

    /*--------------------------------------------------------------------------*/

    /**
     * Reads a tuple from a stream in a less painful way.
     * Did not use this!!!
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

        //take next tuple in outer array
        if(getNextOut){
            currentOutterTuple = outerStream.get_next();
            getNextOut = false;
        }

        //if no more tuples left in outer array
        if (currentOutterTuple == null) {
            return null;
        }

        //prepare inner tuples again for use
        this.tupleInner = new Tuple();
        this.tupleInner.setHdr((short) in2.length,in2,s2_sizes);
        tupleInner = matchBuff.Get(tupleInner);

        //compare equality of selected outer tuples with all inner tuples
        while( tupleInner!= null){
            int x = TupleUtils.CompareTupleWithTuple(in1[join_col_in1-1],currentOutterTuple,join_col_in1,tupleInner,join_col_in2);
            if(x == 0) { //if equality of tuples then join
                Projection.Join(currentOutterTuple,
                        in1,
                        tupleInner,
                        in2,
                        joinTuple,    // The Joined-Tuple
                        proj_list,      // The projection list
                        n_out_flds  // # fields in joined-tuple
                );
                return joinTuple;
            }
            //get next inner tuple for next round of loop
            tupleInner = matchBuff.Get(tupleInner);
        }
        //prepare buffer for a reread for next outer tuple and reset flag for the first if statement to be triggered
        matchBuff.reread();
        getNextOut = true;

        //recursion...ᕦ(òᴥó)ᕥ...(̿▀̿‿ ̿▀̿ ̿)
        return get_next();
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
                //close all iterators
                this.am1.close();
                this.am2.close();
                this.outerStream.close();
                this.innerStream.close();
                closeFlag = true;
            } catch (JoinsException je) {
                throw new JoinsException(je, "Sort.java: error in closing iterator.");
            } catch (IOException ioe) {
                throw new IOException("Sort.java: error in closing iterator.");
            } catch (IndexException iE) {
                throw new IndexException();
            } catch (SortException e) {
                throw new SortException("Sort.java: error in closing iterator.");
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    } // End of close
}