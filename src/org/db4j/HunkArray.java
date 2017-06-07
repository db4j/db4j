// copyright 2017 nqzero - see License.txt for terms

package org.db4j;

import java.io.Serializable;
import kilim.Pausable;
import org.db4j.Db4j.Transaction;
import org.srlutils.Simple.Rounder;

public abstract class HunkArray<TT,CC extends Command.RwPrimitive<TT,CC>,ZZ extends HunkArray>
        extends Hunkable<HunkArray> implements Serializable {
    static final long serialVersionUID = -9057551081001858374L;

    
    /** block size, entry size (fixme -- needs to be calc'd based on type), entries per hunk */
    protected int bs, es = 8, eph;
    transient protected Db4j db4j;
    protected String name;
    transient protected long hunksBase;
    transient protected Vars loc;
    /** maximum number of slots, hunks per page, bytes per hunk */
    transient protected int maxSlots, hpp, bph;
    /** meta hunk size, ie the number of superblocks in a metahunk */
    protected int mhs;
    transient protected boolean dbg = false;
    transient protected int bytesPerHunk;

    protected String name() { return name; }
    
    protected String info() {
        if (true) return getClass().getName();
        // fixme -- this is all pre-kilim and isn't meaningful anymore
        //   should be stripped down to what can be found without pauses
        //   and another method introduce that is Pausable that captures everything else
        int nhunks = 0; // fixme -- find using a task
        String txt = String.format( "HunkArray -- base:%8d, nhunks:%8d\n", hunksBase, nhunks );
        MetaHunk meta = new MetaHunk().setKhunk( nhunks-1 );
        meta.setKhunk( nhunks-1 + meta.size-1 );
        int level = meta.level;
        int nh = meta.sbIndex();
        int ns = Rounder.divup( nh, hpp );
        meta.setKhunk( 0 );
        for (int ii = 0; ii < ns; ii++) {
            int hunkSlot = 0;
            txt += String.format( "\thunkSlot %2d -- %8d\n", ii, hunkSlot );
        }
        for (int ii = 0; ii < level; ii++) {
            meta.setLevel( ii );
            txt += String.format( "level.%2d -- sbsize:%5d, size:%5d, khunk:%5d\n", 
                    ii, meta.sbsize, meta.size, meta.first );
            for (int jj = 0; jj < mhs && meta.khunk < nhunks; jj++, meta.next()) {
                if (jj%8==0) txt += String.format( "  %3d: ", jj );
                int kblock = 0;
                // kblock is Pausable -- just omit it for now ...
                // kblock = meta.kblock(txn);
                txt += String.format( "  %8d", kblock );
                if ((jj+1)%8==0) txt += "\n";
            }
        }
//        for (int ii = 0; ii < nh; ii++) {
//            txt += String.format( "superblock %4d -- %5d, %5d, %2d\n",
//                    ii, meta.kblock(), meta.sbsize, meta.level );
//            meta.setKhunk( meta.nextHunk() );
//        }
        return txt;
    }

    protected static class Vars {
        public Locals locals = new Locals();
        public final LocalInt nhunks = new LocalInt( locals );
        public final LocalInt nlive = new LocalInt( locals );
    }


    

    protected ZZ set(Db4j _db4j,String name) {
        db4j = _db4j;
        loc = new Vars();
        bs = db4j.bs;
        es = cmd().size();
        eph = bs / es;
        initConstants();
        if (name != null) this.name = name;
        return (ZZ) this;
    }
    private void initConstants() {
        bytesPerHunk = 4;
        mhs = 8;
        maxSlots = 29*mhs;
        bph = 4;
        hpp = bs / bph;
    }

    protected int create() {
        int lsize = loc.locals.size();
        int cap = lsize + maxSlots*bytesPerHunk;
        return cap;
    }
    protected void createCommit(long locBase) {
        loc.locals.set(db4j, locBase );
        hunksBase = locBase + loc.locals.size();
    }
    protected void postInit(Transaction txn) throws Pausable {
        db4j.put( txn, loc.nhunks.write(0) );
        db4j.put( txn, loc.nlive.write(0) );
    }
    protected void postLoad(Transaction txn) throws Pausable {}


    public Command.RwLongs [] setLongs(Transaction txn,long k1,long [] data) throws Pausable {
        long k2 = k1 + data.length;
        allocHunks( k2, txn );
        Command.RwLongs [] cmds = new Command.RwLongs[ (int) (k2/eph - k1/eph + 1) ];

        int kdata = 0, ii = 0;
        for (long next = Rounder.next(k1,eph); k1 < k2; k1 = next, next += eph) {
            if (next > k2) next = k2;
            int length = (int) (next - k1);
            Command.RwLongs cmd = new Command.RwLongs().init(true);
            cmd.range( kdata, length );
            cmd.set( data );
            db4j.put( txn, offset(k1, txn), cmd );
            cmds[ii++] = cmd;
            kdata += length;
        }
        return cmds;
    }

    
    public <XX> Command.RwArray [] setdata(Transaction txn,long k1,XX data,Command.RwArray<XX,?> tmp,int num) throws Pausable {
        long k2 = k1 + num;
        allocHunks( k2, txn );
        Command.RwArray [] cmds = new Command.RwArray[ (int) (k2/eph - k1/eph + 1) ];

        int kdata = 0, ii = 0;
        for (long next = Rounder.next(k1,eph); k1 < k2; k1 = next, next += eph) {
            if (next > k2) next = k2;
            int length = (int) (next - k1);
            Command.RwArray cmd = tmp.dup();
            cmd.range(kdata,length);
            cmd.set(data);
            db4j.put( txn, offset(k1, txn), cmd );
            cmds[ii++] = cmd;
            kdata += length;
        }
        return cmds;
    }
    

    /** use Command.Init to initialize pages as they're "appended" */
    protected void initPages(long index,Transaction txn) throws Pausable {
        int khunk = (int) ((index-1) / eph);
        Command.RwInt nlive = db4j.put( txn, loc.nlive.read() );
        if (txn.submit()) kilim.Task.yield();
        if (khunk >= nlive.val) {
            long ko = khunk * eph;
            long offset = offset(ko,txn);
            db4j.put( txn, offset, new Command.Init() );
            db4j.put( txn, loc.nlive.write(khunk+1) );
        }
    }

    // fixme::optimize -- do the allocs grow in size quickly enough ???

    /** allocate space in the array to ensure that index is contained */
    protected void allocHunks(long index,Transaction txn) throws Pausable {
        int khunk = (int) ((index-1) / eph);
        Command.RwInt cmd = db4j.put( txn, loc.nhunks.read() );
        if (txn.submit()) kilim.Task.yield();
        int nhunks = cmd.val;
        if (khunk < nhunks) return;

        // the first maxSlots hunks can just be stored directly in the hunkSlots
        // this assumes that maxSlots <= mhs
        // ie limited to the first level where the superblocks are size 1
        if (false && khunk < maxSlots) {
            int knext = khunk+1;
            int ngrow = knext - nhunks;
            int kpage = db4j.request( ngrow, true, txn )[0];
            int [] kblocks = new int[ngrow];
            for (int ii = 0; ii < ngrow; ii++) kblocks[ii] = kpage + ii;

            db4j.iocmd( txn, hunksBase+nhunks*bph, kblocks, true );
            db4j.put( txn, loc.nhunks.write(knext) );

            // fixme -- should we clear, commit and rollback ???
            return;
        }
        else if (nhunks < maxSlots) {
            // fixme::correctness -- the first time we exceed maxSlots need to copy the data over ...
        }


        org.srlutils.Simple.softAssert( maxSlots <= hpp,
                "contract has changed ... need to add code to backfill the superblocks on disk" );

        MetaHunk h1 = new MetaHunk().setKhunk( nhunks );
        MetaHunk h2 = new MetaHunk().setKhunk( khunk );
        int knext = h2.nextHunk();
        int sb1 = h1.sbIndex(), sb2 = h2.sbIndex()+1;
        int [] reqs = new int[ sb2-sb1 ];

        if (dbg) System.out.format( "HunkArray.alloc -- %5d, %5d, %5d\n", khunk, sb1, sb2 );
        
        int marker = h1.sbAlloc();
        int kr = 0;
        for (int ii = sb1; ii < sb2; ii++, h1 = h1.next()) {
            reqs[kr++] = h1.sbsize;
        }
        

        // the superblock allocations (ie their khunks)
        // - are stored in memory as the hunks array
        // - are stored on disk in pages pointed to by the slots
        // each slot points to a page, that page points to hpp superblocks
        // walk thru the hunks page by page
        // kh is the starting index into hunks, k2 is the ending index into hunks
        // ko is the starting index into the page
        // for each page of superblock pointers, write them out to disk
        
        boolean useRestart = false;

        if (useRestart) txn.clear();
        db4j.put( txn, loc.nhunks.read() );

        int [] kblocks = db4j.request( reqs, txn );

        db4j.iocmd( txn, hunksBase + sb1*bph, kblocks, true );
        db4j.put( txn, loc.nhunks.write(knext) );
        if (useRestart) {
            Db4j.restart();
            txn.restart = true;
            kilim.Task.yield();
            throw new Db4j.ClosedException();
        }
    }

    /**
     * the array is broken down into levels and superblocks
     * a level consists of mhs superblocks (128 at the time of this comment)
     * the superblock size is 2^(level-1) blocks, except 0 --> 1
     * ie, each level doubles the size of the array
     * the metahunk describes the location of a block
     */
    protected class MetaHunk {
        /** the hunk index                             */  public int khunk;
        /** the level that contains the hunk           */  public int level;
        /** superblock size in blocks                  */  public int sbsize;
        /** total size in blocks of this level         */  public int size;
        /** khunk of the level's first hunk            */  public int first;
        /** index of superblock that contains the hunk */  public int kmeta;

        /** initialize the metahunk using the given level, ie for the first hunk contained by level */
        public MetaHunk setLevel(int _level) {
            level = _level;
            calcSize();
            khunk = first;
            kmeta = 0;
            return this;
        }
        /** return the cumulative size of all levels up to and including this one */
        public int cumSize() { return first + size; }
        public void calcSize() {
            if (level == 0) { sbsize = 1;              size = mhs * sbsize; first = 0;    }
            else            { sbsize = 1 << (level-1); size = mhs * sbsize; first = size; }
        }
        /** initialize the metahunk using the given khunk */
        public MetaHunk setKhunk(int _khunk) {
            khunk = _khunk;
            level = 32 - Integer.numberOfLeadingZeros( khunk / mhs );
            calcSize();
            kmeta = (khunk - first) / sbsize;
            return this;
        }
        /** return the size (in hunks) of the array up to and including this superblock */
        public int nextHunk() { return (khunk/sbsize + 1) * sbsize; }
        /** 
         * cumulative superblocks for the array up to and including this level
         * ie, the number of levels times the number of superblocks per level
         */
        public int sbAlloc() { return (level+1) * mhs; }
        /** return the index into the hunks array of this hunks superblock */
        public int sbIndex() { return level * mhs + kmeta; }
        /** return the block index (ie on disk) of this hunk (mult by blocksize to get physical addr) */
        public int kblock(Transaction txn) throws Pausable {
            int sbi = sbIndex();
            long offset = hunksBase + sbi*bph;
            Command.RwInt cmd = new Command.RwInt();
            db4j.put( txn, offset, cmd );
            if (txn.submit()) kilim.Task.yield();
            int block = cmd.val;
            return block + khunk % sbsize;
        }
        /** advance to the next superblock and return This */
        public MetaHunk next() { setKhunk( nextHunk() ); return this; }

    }

    public CC set(Transaction txn,long index,TT value) throws Pausable {
        CC cmd = cmd().set(value).init(true);
        cmd.msg = "HunkArray::set";
        return set( txn, index, cmd );
    }
    public CC set(Transaction txn,long index,CC cmd) throws Pausable {
        // fixme::alignment ... make sure that the data doesn't overlap the end-of-block
        allocHunks( index+es, txn );
        boolean useInit = true;
        if (useInit) initPages( index+es, txn );
        long offset = offset( index, txn );
        db4j.put( txn, offset, cmd );
        return cmd;
    }

    public CC get(Transaction txn,long index,CC cmd) throws Pausable {
        long offset = offset( index, txn );
        db4j.put( txn, offset, cmd );
        return cmd;
    }
    public CC get(Transaction txn,long index) throws Pausable {
        CC cmd = cmd().init(false);
        cmd.msg = "HunkArray::get";
        return get( txn, index, cmd );
    }

    protected int hunkSlot(int index,Transaction txn) throws Pausable {
        long pos = hunksBase + index*bytesPerHunk;
        Command.RwInt cmd = new Command.RwInt().init(false);
        db4j.put( txn, pos, cmd );
        if (txn.submit()) kilim.Task.yield();
        return cmd.val;
    }
    
    protected long hunkOffset(int page) { return ((long) page) << db4j.bb; }
    protected long offset(long index, Transaction txn) throws Pausable {
        int kh = (int) (index / eph);
        int ke = (int) (index % eph);
        MetaHunk meta = new MetaHunk().setKhunk( kh );
        int kb = meta.kblock(txn);
        long offset = hunkOffset(kb) + ke * es;
        return offset;
    }
    /** return a description of the offset: [[ khunk:kentry --> (superblock index::pntr) kblock, offset ]] */
    public String offsetInfo(Transaction txn,long index) throws Pausable {
        int kh = (int) (index / eph);
        int ke = (int) (index % eph);
        MetaHunk meta = new MetaHunk().setKhunk( kh );
        int kb = meta.kblock(txn);
        int sbi = meta.sbIndex();
        long pntr = 0; // 1L * hunkSlots[sbi/hpp] * bs + (sbi%hpp) * bph;
        long offset = hunkOffset(kb) + ke * es;
        return String.format( "[[%8d:%5d --> (%5d::%12d) %8d, %8d]]", kh, ke, sbi, pntr, kb, offset );
    }
    public void printMetaHunkInfo(Transaction txn) throws Pausable {
        Command.RwInt cmd = db4j.put( txn, loc.nhunks.read() );
        if (txn.submit()) kilim.Task.yield();
        int nhunks = cmd.val;
        MetaHunk meta = new MetaHunk();
        for (int ii = 0; ii < nhunks; ii++) {
            meta.setKhunk(ii);
            if (ii % meta.sbsize == 0) {
                int kb = meta.kblock(txn);
                System.out.format( "%5d %5d %5d %5d %5d\n", ii, meta.level, meta.first, meta.sbsize, kb );
            }
        }
    }


    protected abstract CC cmd();

    public static class Y extends HunkArray<Byte,Command.RwByte,Y> {
        public Command.RwByte cmd() { return new Command.RwByte(); }
    }
    public static class I extends HunkArray<Integer,Command.RwInt,I> {
        public Command.RwInt cmd() { return new Command.RwInt(); }
    }
    public static class L extends HunkArray<Long,Command.RwLong,L> {
        public Command.RwLong cmd() { return new Command.RwLong(); }
    }
    public static class D extends HunkArray<Double,Command.RwDouble,D> {
        public Command.RwDouble cmd() { return new Command.RwDouble(); }
    }

    public static class L2 extends HunkArray<long[],Command.RwLongs,L2> {
        public Command.RwLongs cmd() { return new Command.RwLongs().range(0,2); }
    }
    
    
}
