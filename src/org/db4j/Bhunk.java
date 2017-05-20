// copyright 2017 nqzero - see License.txt for terms

package org.db4j;

import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import java.io.Serializable;
import org.db4j.Db4j.Hunkable;
import org.db4j.Db4j.Transaction;
import org.srlutils.btree.Bpage.Sheet;
import kilim.Pausable;
import org.db4j.Db4j;
import org.db4j.Db4j.LocalInt;
import org.db4j.Db4j.Locals;
import org.db4j.Db4j.Query;
import org.srlutils.Rand;
import org.srlutils.Simple;
import org.srlutils.Types;
import org.srlutils.btree.Butil;
import org.srlutils.TaskTimer;
import org.srlutils.Util;
import org.srlutils.btree.Bstring;
import org.srlutils.btree.TestDF;

/**
 *  a hunkable btree
*/
public abstract class Bhunk<CC extends Bhunk.Context<CC>> extends Btree<CC,Sheet> 
    implements Serializable
{
    transient protected Db4j db4j;
    transient protected Vars loc;
    
    // variables for running in-memory for troubleshooting and measuring memory effects
    //   last known use: 2017.07.06
    transient final boolean fakeLoc = false, fakePages = false;
    transient byte [][] pages = fakePages ? new byte[1<<16][] : null;
    transient int fakeNext = 1;
    transient int fakeDepth, fakeRoot;

    protected String name;

    private static final boolean useCopy = true;
    private static final boolean extraChecks = false;
    static int copy = 1, slut = 2;
    static boolean dbg = false;

    void clear() {
        if (fakePages) {
            for (int ii = 1; ii < fakeNext; ii++)
                pages[ii] = null;
            fakeNext = 1;
        }
    }
    protected static class Vars {
        public Locals locals = new Locals();
        public final LocalInt kroot = new LocalInt( locals );
        public final LocalInt depth = new LocalInt( locals );
    }
    protected class LocalCommand extends Command.Rw<LocalCommand> {
        int kroot, depth;
        public void read(Page buf,int offset) {
            kroot = buf.getInt( offset );
            depth = buf.getInt( offset+loc.depth.position );
        }
        public void write(Page buf,int offset) {}
        public int size() { return loc.locals.size(); }
    }
    Sheet rootz(Sheet page,CC context) throws Pausable {
        if (fakeLoc) {
            context.depth = fakeDepth;
            if (page==null) page = getPage(fakeRoot,context,fakeDepth==0);
            else fakeRoot = page.kpage;
            return page;
        }
        if (page==null) {
            LocalCommand cmd = new LocalCommand();
            cmd.offset = loc.locals.base;
            db4j.put( context.txn, cmd );
            if (context.txn.submit())
                kilim.Task.yield();
            context.depth = cmd.depth;
            int kpage = cmd.kroot;
            page = getPage(kpage,context,context.depth==0);
        }
        else
            db4j.put( context.txn, loc.kroot.write(page.kpage) );
        return page;
    }
    /** read a page directly from the file, ie without hunker, for debugging */
    Sheet fakeGetPage(String filename,int kpage,boolean leaf) {
        Sheet page = newPage(leaf,null,true);
        org.srlutils.Files.readbytes(filename,1L*kpage*bs,page.buf,0,-1);
        return page;
    }
    protected Sheet getPage(int kpage,CC cc,boolean leaf) throws Pausable {
        Sheet page;
        if (fakePages) {
            page = newPage(leaf,cc,false);
            page.buf = pages[kpage];
        }
        else {
            Command.Reference refcmd = new Command.Reference().init(false);
            // Command.RwBytes cmd = new Command.RwBytes().init(false).range( 0, hunker.bs );
            // cmd.msg = "Bhunk.get:" + kpage;
            db4j.put( cc.txn, offset(kpage,0), refcmd );
            if (cc.txn.submit()) kilim.Task.yield();
            page = newPage(leaf,cc,false);
            page.buf = refcmd.data;
        }
        page.load();
        page.kpage = kpage;
        // fixme -- need to record every page (in cc.txn ???) so we can cleanup when txn finishes
        if (extraChecks) Simple.softAssert(kpage > 0);
        return page;
    }
    protected long offset(int kpage,int index) { return (((long) kpage) << db4j.bb) + index; }
    protected void depth(int level,CC context) throws Pausable {
        context.depth = level;
        fakeDepth = level;
        if (!fakeLoc) db4j.put( context.txn, loc.depth.write(level) );
    }
    protected int shift(Sheet page, int ko) { return page.shift(ko); }
    protected void merge(Sheet page0,Sheet page1) {
        Simple.softAssert(page1.isset(copy));
        page0.merge(page1);
    }
    /** create a new page */
    protected Sheet createPage(boolean leaf,CC cc) throws Pausable {
        int kpage;
        Sheet page = newPage(leaf,cc,true);
        if (fakePages) {
            kpage = fakeNext++;
            pages[kpage] = page.buf;
        }
        else {
            kpage = db4j.request( 1, true, cc.txn )[0];
            Command.Init cmd = new Command.Init();
            cmd.data = page.buf; // fixme -- does this really work ???
            db4j.put( cc.txn, offset(kpage,0), cmd );
        }
        page.kpage = kpage;
        page.flag |= copy;
        if (extraChecks) Simple.softAssert(kpage > 0);
        return page;
    }
    protected Sheet newPage(boolean leaf,CC cc,boolean alloc) {
        Sheet page = new Sheet();
        page.init( bs, leaf ? mleaf:mbranch, leaf ? pval:pdex, null );
        if (alloc) page.buf = new byte[bs];
        page.leaf = leaf ? 1:0;
//        if (!fake) cc.txn.addCleaner(page);
        return page;
    }
    protected void key(Sheet p0, int k0,Sheet p1, int k1) {
        p1.rawcopy(p0,k1,k0,pkey,keysize);
    }
    protected class InsertCommand extends Command.Rw<InsertCommand> {
        { super.init(true); }
        int ko;
        Sheet page;
        CC cc;
        InsertCommand set(int $ko,Sheet $page,CC $cc) { ko=$ko; page=$page; cc=$cc; return this; }
        public void read(Page buf,int offset) { throw Simple.Exceptions.rte(null,"cmd is write only"); }
        public void write(Page buf,int offset) {}
        public void run(int offset,Page buf,Db4j db4j,boolean defer) {
            if (defer) buf.dupify();
            page.buf = buf.data;
            page.load();
            checkDel(page,false);
            compress(page,cc,ko,null,true);
            ko = shift(page,ko);
            assert(isToast(cc)==false);
            setcc(page,cc,ko);
            checkDel(page,false);
            page.commit();
        }
        public int size() { return 0; }
    }

    /**
     * read all pages in a range into the cache. this is much faster than iterating thru the range
     * @param p1 the start of the range, inclusive
     * @param p2 the end of the range, exclusive
     * @param context the context
     */
    public void slurp(Path<Sheet> p1,Path<Sheet> p2,CC context) throws Pausable {
        Path<Sheet> [] x1, x2, xi;
        
        x1 = p1.list(context.depth);
        x2 = p2.list(context.depth);
        xi = p1.dup().list(context.depth);

        Path<Sheet> xo=null, z1=null, z2=null;

        for (int level = 0; true; level++) {
            xo = xi[level];
            z1 = x1[level];
            z2 = x2[level];
            
            if (level==context.depth) break;
            
            
            xo.set(xo.prev,z1.page,z1.ko);
            while (!xo.same(z2)) {
                int kpage = xo.page.dexs(xo.ko);
                Command.Reference refcmd = new Command.Reference().init(false);
                db4j.put( context.txn, offset(kpage,0), refcmd );
                advance(xo,level,context);
            }
            context.txn.submitYield();
        }
        xo.copy(z1);

        for (; !xo.same(z2); advance(xo,context.depth,context))
            prepx(xo.page,context,xo.ko);
        
    }
    
    /** insert context into page and return the index */
    final void insert(Sheet page,CC context,int ko) throws Pausable {
        InsertCommand cmd = new InsertCommand().set(ko,page,context);
        boolean toast = isToast(context);
        if (toast || useCopy & page.isset(copy)) {
            if (!page.isset(copy))
                commit(page,context);
            checkDel(page,false);
            compress(page,context,ko,null,true);
            ko = shift(page,ko);
            setccx(page,context,ko);
            checkDel(page,false);
            page.commit();
        }
        else
            db4j.put( context.txn, offset(page.kpage,0), cmd );
    }
    // in theory, we can defer applying the delete until it's needed
    // which would save a copy of the data block for simple tasks (perhaps a 50% savings)
    // however, there's ambiguity with deferring commits like this (used for inserts)
    // so not enabling now - need to look at context.txn.cleanse and cmd.run.defer first
    private int delete_deferred(Sheet page,int index,CC context) {
        if (page.isset(copy)) return page.delete(index);
        Command cmd = new InsertCommand().set(index,page,context);
        db4j.put( context.txn, offset(page.kpage,0), cmd );
        return index < page.num-1 ? index : (index==0 ? 0:index-1);
    }
    void prep(Sheet page) {
        if (!page.isset(copy)) page.buf = Util.dup(page.buf);
        page.flag |= copy;
    }
    protected void commit(Sheet page,CC context) {
        if (extraChecks)
            Simple.softAssert(page.isset(copy) & (page.num > 0 | context.depth==0));

        page.commit();
        if (fakePages | page.isset(slut)) return;
        page.flag |= slut;
        // fixme:kludge -- modifications over-write the read cache
        Command.Reference cmd = new Command.Reference().init(true);
        cmd.data = page.buf;
        int kpage = page.kpage;
        if (useCopy & false) context.txn.cleanse(kpage);
        db4j.put( context.txn, offset(kpage,0), cmd );
    }
    protected void postInit(Transaction tid) throws Pausable {
        init(context().set(tid));
    }
    protected void postLoad(Transaction tid) throws Pausable {}
    protected Bhunk set(Db4j $db4j,String name) {
        db4j = $db4j;
        loc = new Vars();
        if (name != null) this.name = name;
        return this;
    }
    protected String name() { return name; }
    protected void createCommit(long locBase) {
        loc.locals.set(db4j, locBase );
    }
    protected int create() {
        int lsize = loc.locals.size();
        return lsize;
    }
    protected String info() { return ""; }

    protected void split(Sheet src,Sheet dst,int kb) { src.split(dst,kb); }
    
    public static class Context<CC extends Context> extends Btree.Context {
        public Transaction txn;
        public CC set(Transaction $txn) { txn = $txn; return (CC) this; }
        public CC insert(Bhunk map) throws Pausable { map.insert(this); return (CC) this; }
        public CC get(Bhunk map) throws Pausable { map.findData(this); return (CC) this; }
    }

    /** a Double-Float map for testing */
    private static class DF extends Bhunk<DF.Data> {
        public DF() { init(Types.Enum._double.size,Types.Enum._float.size); }
        public static class Data extends Context<Data> implements TestDF.DFcontext<Data> {
            public double key;
            public float val;
            public Data set(double $key,float $val) { key = $key; val = $val; return this; }
            public float get() { return val; }
            public String format(int both) {
                return String.format("%12.12f" + (both==0 ?"":" --> %f"),key,val);
            }
            public Data set(double key) { return set(key,-1f); }
            public float val() { return val; }
        }
        public Data context() { return new Data(); }
        public void setcc(Sheet page,Data cc,int ko) {
            page.put(pkey,ko,cc.key);
            page.put(pval,ko,cc.val);
        }
        public void getcc(Sheet page,Data cc,int ko) {
            cc.key = page.getd(pkey,ko);
            cc.val = page.getf(pval,ko);
        }
        double key(Sheet page,int index) { return page.getd(pkey,index); }
        protected int compare(Sheet page,int index,Data data) {
            // Double.compare is slow
            // compare explicitly, should map all nans as equal
            return Butil.compare(data.key,key(page,index));
        }
        protected int findLoop(Sheet page,int k1,int num,int step,Data context,boolean greater) {
            for (; k1<num; k1+=step) {
                int cmp = compare( page, k1, context );
                if (greater & cmp==0) cmp = 1;
                if (cmp <= 0) break;
            }
            if (step > 1)
                return findLoop(page,k1-step,num,1,context,greater);
            return k1;
        }
        
        public void check(Data cc) throws Pausable {
            Sheet rootz = rootz(null,cc);
            double key = check(rootz,0,cc);
        }
        public double check(Sheet page,int level,Data cc) throws Pausable {
            if (level==cc.depth) return key(page,page.num-1);
            double k2 = 0;
            level++;
            for (int ii = 0; ii < page.num; ii++) {
                Sheet p2 = dexs(page, ii, level==cc.depth,cc);
                k2 = check(p2,level,cc);
                if (ii < page.num-1) {
                    double key = key(page,ii);
                    Simple.softAssert( k2==key, "%f vs %f", k2, key );
                }
            }
            return k2;
        }
    }
    
    
    protected abstract class ValsVarx<TT,DD> extends Bstring.ValsVar<TT,DD> {
        public void setx(Transaction tid,Sheet page,int index,TT val2,Object cmpr) throws Pausable {
            byte [] val = convert(val2,cmpr);
            if (under(val))
                set2(page,index,val);
            else {
                int nb = db4j.util.nblocks(val.length);
                int [] blocks = db4j.request(nb,true,tid);
                int kblock = blocks[0], len = val.length;
                int offset = setx(page,index);
                page.puti(offset,val.length);
                page.puti(offset+4,blocks[0]);
                db4j.iocmd(tid,db4j.util.address(blocks[0]),val,true);
                if (dbg)
                    System.out.format("setx @ %5d: %5d %5d\n",offset,len,kblock);
            }
        }
        public void prepx(Transaction tid,Sheet page,int index) {
            int offset = getx(page,index);
            if (offset < 0)
                return;
            // fixme - abuse of slot-based addressing
            int len = page.geti(offset,0);
            int kblock = page.geti(offset+4,0);
            byte [] bytes = new byte[len];
            long address = db4j.util.address(kblock);
            db4j.iocmd(tid,address,bytes,false);
        }
        public TT getx(Transaction tid,Sheet page,int index) throws Pausable {
            int offset = getx(page,index);
            if (offset < 0)
                return get(page,index);
            // fixme - abuse of slot-based addressing
            int len = page.geti(offset,0);
            int kblock = page.geti(offset+4,0);
            if (dbg)
                System.out.format("getx @ %5d: %5d %5d\n",offset,len,kblock);
            byte [] bytes = new byte[len];
            long address = db4j.util.address(kblock);
            db4j.iocmd(tid,address,bytes,false);
            tid.submitYield();
            return convert(bytes);
        }
        protected abstract byte [] convert(TT val,Object cmpr);
        protected abstract TT convert(byte [] val);
        boolean under(byte [] val) { return val.length <= maxlen; }
        public boolean isToast(TT val,Object cmpr) {
            return !under(convert(val,cmpr));
        }
        protected int len(int bits) {
            int len = super.len(bits);
            return len==Bstring.mask ? toastSize : len;
        }

        public int size(TT val,Object cmpr) {
            byte [] data = convert(val,cmpr);
            return size() + (under(data) ? data.length : toastSize);
        }
        public TT get(Sheet page,int index) {
            byte[] val = get2(page,index);
            return convert(val);
        }
        public void set(Sheet page,int index,TT val,Object cmpr) {
            byte [] data = convert(val,cmpr);
            set2(page,index,data);
        }
        public DD compareData(TT val,boolean prefix,Object past) {
            byte [] data = convert(val,null);
            return (DD) super.compareData2(data,prefix);
        }
        int maxlen = 1000;
    }
    protected class ValsBytex extends ValsVarx<byte [],Bstring.Cmpr> {
        protected byte[] convert(byte[] val,Object cmpr) { return val; }
        protected byte[] convert(byte[] val) { return val; }
    }
    protected class ValsKryo<TT> extends ValsVarx<TT,Bstring.Cmpr> {
        protected byte[] convert(TT val,Object cmpr) {
            return cmpr==null
                    ? save(val) 
                    : ((Bstring.Cmpr) cmpr).bytes;
        }
        protected TT convert(byte[] bytes) {
            Input input = new Input(bytes);
            return (TT) db4j.kryo().get(input);
        }
        byte [] save(TT val) {
            Output buffer = new Output(2048,-1);
            db4j.kryo().put(buffer,val,false);
            return buffer.toBytes();
        }
    }
    protected class ValsObject<TT> extends ValsVarx<TT,Bstring.Cmpr> {
        protected byte[] convert(TT val,Object cmpr) {
            return cmpr==null
                    ? org.srlutils.Files.save(val)
                    : ((Bstring.Cmpr) cmpr).bytes;
        }
        protected TT convert(byte[] bytes) {
            return (TT) org.srlutils.Files.load(bytes,0,-1,Bhunk.this.db4j.userClassLoader);
        }
    }
    protected class ValsStringx extends ValsVarx<String,Bstring.Cmpr> {
        protected byte[] convert(String val,Object cmpr) {
            return val.getBytes();
        }
        protected String convert(byte[] bytes) {
            return new String(bytes);
        }
    }

    /*
        any insert or update that depends on a class that was added to kryomap during execution
        whether a self add or added by another task
        can't complete till that kryomap update finishes
        kryomap values are immutable (ie, no deletions or modifications)
        
        should be able to add an immediate journal entry 
        and update a locked structure (both in memory operations, ie not pausable or blocking)
        and then deal with the actual structure add at leisure
        (need to be sure there's space for the structure add first)
    
        there could be large sets of adds to this list
        hate to have a designated (pre-allocated) log-structure area but that would be easiest
        need to scan entire region on startup

        dedicated log-structured disk region, position and length stored as locals
        null terminated
        each element comprises: type, payload
        kryo payload: id, classname
        
    
    
    
    
    
    
    
    
    */
    
    
    /** base class for comparing hunk-based trees with kilim */
    static class Mindir
    <CC extends Bhunk.Context<CC> & TestDF.DFcontext<CC>,TT extends Bhunk<CC>>
    extends TaskTimer.Runner<Void> {
        int nn;
        TT map;
        Rand.Seeded rand = new Rand.Seeded();
        Long seedSeed = null, seed = null;
        {
            rand.init(seedSeed,true);
        }
        String filename = "./db_files/b6.mmap";
        Db4j db4j;
        boolean ok = true;
        boolean reopen = false;
        public Mindir(int $nn,TT $map) { nn=$nn; map=$map; }
        { stageNames = "put look rem chk".split(" "); }
        public void alloc() { setup(stageNames.length, "Bhunk.DF"); }
        void close() { db4j.shutdown(); db4j.close(); }
        public void init() {
            seed = rand.setSeed(null,false);
            db4j = new Db4j().init( filename, null ); // 1L << 32 );
            db4j.register(map,"Bushy Tree");
            db4j.create();
            db4j.fence(null,100);
            db4j.forceCommit(100);
            if (reopen) close();
        }
        public void run(final int stage) throws Exception {
            rand.setSeed(seed,false);
            if (reopen) db4j = Db4j.load(filename);
            map = (TT) db4j.arrays.get(0);
            for (int ii = 0; ii < nn; ii++) {
                final int jj = ii;
                final float v1 = 0.01f*jj, goal = stage==3 ? -1f:v1;
                final float vo = stage==0 ? v1:-1f;
                final double key = rand.nextDouble();
                new Query() { public void task() throws Pausable {
                    CC cc = map.context().set(key,vo);
                    cc.set(tid);
                    if      (stage==0) map.insert  (cc);
                    else if (stage==2) {
                        cc.mode = modes.eq;
                        Path<Sheet> path = map.findPath(cc,true);
                        if (!cc.match | cc.val() != goal) {
                            boolean done = true;
                            if (done) Simple.spinDebug(false,"findPath fail ... %d, %f",jj,key);
                            cc.set(key,vo);
                            path = map.findPath(cc,true);
                            System.out.format("findPath fail: %d, %f\n", jj,key);
                        }
                        map.remove(path,cc,false);
                    }
                    else               map.findData(cc);
                    if (stage > 0 && (cc.val() != goal))
                        ok = false;
                } }.offer(db4j);
            }
            db4j.fence(null,10);
            if (reopen) close();
        }
        public boolean finish() throws Exception {
            if (!reopen) close();
            map.clear();
            return ok;
        }
    }
    static class Demo {
        DF lt;
        String name = "./db_files/b6.mmap";
        Db4j db4j;
        float val, vo = 97f;
        double ko = 7.1;
        int nb = 0;
        
        class PutTask extends Db4j.Query {
            double k1;
            float v1;
            int i1;
            PutTask(int $i1,double $k1,float $v1) { i1=$i1; k1=$k1; v1=$v1; }
            public void task() throws Pausable {
                lt.insert(lt.context().set(tid).set(k1,v1));
            };
        }
        class GetTask extends Db4j.Query {
            DF.Data cc = lt.context();
            double k1;
            float v1;
            GetTask(double $k1,float $v1) { k1=$k1; v1=$v1; }
            public void task() throws Pausable {
                    cc.set(tid).set(k1,-1f);
                    lt.findData(cc);
                    boolean bad = v1 != cc.val;
                    if ((k1-ko)%10==0 || bad)
                        System.out.format( "find: %8.3f --> %8.3f%s\n", cc.key, cc.val,
                                bad ? "  ------bad------" : "" );
            };
        }
        class CheckTask extends Db4j.Query {
            public void task() throws Pausable {
                lt.check( lt.context().set(tid).set(-1d,-1f) );
            };
        }
        
        public void demo() {
            db4j = new Db4j().init( name, null ); // 1L << 32 );
            lt = db4j.register(new DF(),"Bushy Tree");
            int nn = 1347-7;
            db4j.create();
            // break out the final iter to allow tracing in the debugger
            for (int ii = 0; ii < nn; ii++)
                new PutTask(ii,ii+ko,ii+vo).offer(db4j);
            db4j.fence(null,100);
            new PutTask(nn,nn+ko,nn+vo).offer(db4j).awaitb();
            for (int ii = 0; ii < nn; ii++) 
                new GetTask(ii+ko,ii+vo).offer(db4j);
            new GetTask(nn+ko,nn+vo).offer(db4j);
            db4j.fence(null,100);
            new CheckTask().offer(db4j).awaitb();
            lt.db4j.shutdown();
        }
        public static void auto(int passes,int npp,TaskTimer.Runner ... runners) throws Exception {
            Simple.Scripts.cpufreqStash( 2300000 );
            TaskTimer tt = new TaskTimer().init( npp, 0, true, false );
            tt.widths( 8, 3 );
            for (int ii=0; ii<passes; ii++)
                tt.autoTimer(runners);
        }
        public static void main(String [] args) throws Exception {
            if (true) {
                auto(2, 2,
                        new Mindir(1000000,new DF())
                        );
//                auto(1, 1, new Mindir(1000000));
//                auto(1, 3, new Mindir(1000000));
                return;
            }
            Demo demo = new Demo();
            demo.demo();
        }
    }
}

/*

* comparing various hunker based double-float maps (2 runs within 5% total time)
* Bhunk.DF :    3.003    3.579    5.064    0.845   12.491 - df-specific
* Bmeta.DF :    5.964    3.630    6.254    0.903   16.751 - generic key/val, autoboxed
* Bmeta.DF2:    6.927    6.812   10.125    2.150   26.014 - string/string, includes cost of conversion (expensive)
* good.1~15:    2.962    3.426                      6.388 - an older df-specific without remove
*   Bhunk.DF looks consistent with older results (ie 6s for put+look only)

* change to using Bface in Mindir (to allow other maps to be used) --> 11.0s to 13.0s, more or less unchanged
 
 
* comparing TestDF performance across different backing systems, all non-meta impls
*   based on 10^6 items, "put look rem chk"
*     srlutils   --  1.6s
*     b6.Btree   --  2.0s, like srlutils, but woven
*     b6.Bhunk   -- 12.0s, like b6.Btree, but QueRunner and data is in cache (as opposed to array of pages)
*   todo::performane - quantify why Bhunk is so much slower than Btree, especially for "look"
*     all cache should be in memory, so zero io required
*     nothing should rollback
*     Pausable should not be "thrown"
*   known causes are cache lookup (tree vs array) and context switching
*     intuitively doesn't seem like runtime should be 6x as long

* 10^8 random key stress test, insert+remove, ie Demo.auto(1, 1, new Mindir(100000000))
*   Average   : 35492.174 54780.426 54069.868   91.020 144433.489 
*   1.2 GB of leaf node data (maybe 2GB total), with 256 MB of cache --> only about 12% of the data is cached
*   on the order of 2000 operations per second (put, remove or find) which seems decent
*     sequential blocks --> 25k iops, which scales with read density (above 2% read density)
*     so equiv to reading 160k blocks each disk loop, ie seems reasonable


* using rand.nextDouble() in the Mindir.run loop, instead of an array of doubles pregenerated in init()
*   is slower, 12.3s vs 12.1 for 10^6 entries over multiple runs
*   however, it enables larger tests so switching to it


* nn = 1000000
    b6.Btree runs in 2.1 seconds (single run - 2.3 seconds)
    with a somewhat realistic hunker, 2.7 seconds (single: 3.1)
      removed the kilim.Task superclass from Kask
      debug flags are final
      rapid fire the command polling (rep=120)
      all state vars are bypassing hunker (ie, root, depth and pages)
    with locals (depth, root) using hunker, 4.3 seconds
    with locals + hunker runCache improvements, 3.16 seconds
      ByteBuffer.wrap the cache data instead of tmpbuf.put
      combining depth and root into a single read should speed things up by .25 seconds
  
  * using Sheet backed by an array (instead of direct memory)
  *   fake==true (ie cached pages) is uneffected, but fake==false is 29 seconds vs 22 for direct memory
    caching only the array, ie not the actual Sheet, and re-initing the sheet on each use
        3.51 seconds vs 3.21 (for cached Sheets)
        * difference should just be sheet.commit() and .load(), seems reasonable

  * assume that the array backing with hunker is slower due to gc
  *   direct memory doesn't count against the vm allocation and doesn't need to be scanned for references
  *   modified direct (bhunk.fast branch) to allocate both direct mem + an array (uses direct)
  *     and this array version (btree.array) as well (uses array)
  *     29.5 seconds in both cases (so more or less verified)



* using Command.ByteBuffer + unsafe ... 28.9 seconds (and chickenSoup.Build is successful)


* using Command.Reference -- brute force impl, not safe (writes directly to the read cache)
*   but works for this test ... (all tasks finish in single pass)
*   this is all with bypassWrites==true (io bandwidth gets saturated by writes)
* normal:       Mindir  |    2.904      0.038  |    2.115      0.016  |    5.019      0.050 
* no getPage:   Mindir  |    2.388      0.065  |    1.394      0.008  |    3.783      0.059 
* or commit:    Mindir  |    1.926      0.064  |    1.409      0.031  |    3.335      0.033 (faster of 2 runs)
* with fake:    Mindir  |    1.851      0.038  |    1.383      0.017  |    3.234      0.024 

    * hunker.request is 0.09-ish
    * handleWrite       0.45-ish
    * handleRead        0.75-ish (for lookup only), ie 3 reads*0.25 per read (ie depth is 2)
                        0.50-ish (for insert) - 0.25*average of 1 and 3 ???
    * pretty much confirms the 0.25 seconds per million preReads (lookup cache, update stats and run cmd)

    * diff: cache.get() for each getPage and commit ... depth is probably 2
    *   so 4-ish cache.get()s --> would expect 1 second slower

* using Command.Referene + prep() + InsertCommand ... 5.8 seconds

    with a stripped down bhunk (ie just using hunker for the task loop, lots of stuff disabled)
      Qrunner.process - only pred and cmd polling, with rep=1200
      single run 2.9 seconds







































*/