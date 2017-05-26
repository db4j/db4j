// copyright 2017 nqzero - see License.txt for terms

package org.db4j;

import java.util.ArrayList;
import java.util.Arrays;
import org.srlutils.btree.BtTests2;
import org.srlutils.btree.Btypes.Element;
import org.srlutils.btree.Bpage.Sheet;
import kilim.Pausable;
import org.db4j.Db4j;
import org.db4j.Db4j.Query;
import org.db4j.Db4j.Transaction;
import org.db4j.perf.DemoHunker;
import org.srlutils.Rand;
import org.srlutils.Simple;
import org.srlutils.TaskTimer;
import org.srlutils.btree.Bpage;
import org.srlutils.btree.Btypes.ValsDouble;
import org.srlutils.btree.Btypes.ValsFloat;
import org.srlutils.btree.Bstring;
import org.srlutils.btree.Bstring.ValsString;
import org.srlutils.btree.Btypes;
import org.srlutils.btree.Btypes.ValsTuple;
import org.srlutils.btree.Btypes.ValsVoid;
import org.srlutils.btree.TestDF;


/**
 * a btree subclass that uses a generic Element to access the keys and values
 * @param <KK> key type
 * @param <VV> val type
 * @param <PP> page type
 */
public abstract class Bmeta<CC extends Bmeta.Context<KK,VV,CC>,KK,VV,EE extends Element<KK,?>>
        extends Bhunk<CC> {

    static final long serialVersionUID = -7295474170097215526L;
    static boolean checkDel = false;

    protected EE keys;
    protected Element<VV,?> vals;

    protected void setup(EE $keys,Element<VV,?> $vals) {
        keys = $keys;
        vals = $vals;
        keys.config(0);
        vals.config( keys.size() );
        init(keys.size(),vals.size());
    }
    

    public static class Context<KK,VV,CC extends Context<KK,VV,CC>> extends Bhunk.Context<CC> {
        public KK key;
        public VV val;
        protected Object keydata, valdata;
        private boolean prepped;
        public CC set(KK $key,VV $val) { key = $key; val = $val; return (CC) this; }
        protected void init(Bmeta<?,KK,VV,?> map) {
            boolean prefix = Btree.modes.prefix(mode);
            keydata = map.keys.compareData(key,prefix,keydata);
            if (val != null) valdata = map.vals.compareData(val,prefix,null);
        }
        public CC find(Bmeta map) throws Pausable {
            map.findData( (CC) this);
            return (CC) this;
        }
        public VV get() { return val; }
    }
    /** read the state variables - potentially expensive */
    void initContext(CC cc) throws Pausable {
        super.initContext(cc);
        // fixme -- should we skip cc.init in some cases eg pop() and first()
        cc.init(this);
    }
    protected void merge(Sheet src,Sheet dst) {
        int size = size(dst,src,null,src.leaf==1,null,0);
        boolean dbg = false;
        if (dbg) {
            dump(src,"src: ");
            dump(dst,"dst: ");
        }
        // fixme -- both src and dst are sparse, sort and write directly to dst instead of using tmp ???
        Sheet tmp = newPage(true,null,true);
        int jar = dst.bs-dst.pmeta, jo = jar;
        jar = compress(src,tmp,src,jar,0);
        jar = compress(dst,tmp,dst,jar,0);
        dst.jar = jar;
        dst.del = 0;
        super.merge(src,dst);
        tmp.rawcopy(dst,jar,jar,jo-jar);
        if (dbg) {
            dump(dst,"merg:");
            int s2 = size(dst,null,null,src.leaf==1,null,0);
            System.out.format( "sizes: %d vs %d\n", size, s2 );
            System.out.println( "--------------------------------------------------------------------------------" );
        }
    }
    protected int compress(Sheet src,Sheet dst,Sheet ref,int jar,int shift) {
        int numk = (ref.leaf==1) ? ref.num : ref.num-1;
        if (keys.dynlen)
            for (int ii = numk-1; ii >= 0; ii--)
                jar -= keys.copyPayload(src,dst,ref,ii,jar+shift,jar);
        if (vals.dynlen & src.leaf==1)
            for (int ii = ref.num-1; ii >= 0; ii--)
                jar -= vals.copyPayload(src,dst,ref,ii,jar+shift,jar);
        return jar;
    }
    protected void copyPayload(Sheet src,Sheet dst) {
        int jar = dst.bs-dst.pmeta;
        jar = compress(src,dst,dst,jar,0);
        dst.jar = jar;
        int shift = src.bs-src.pmeta-jar;
        jar = compress(src,dst,src,jar,shift);
        dst.del = src.del = 0;
        // make the last key a "ghost" so that it can be propogated to the parent
        if (src.leaf==0) {
            jar -= src.del = keys.copyPayload(src,dst,src,src.num-1,jar+shift,jar);
            // fixme::hack (or kludge) -- ghosting seems prone for trouble
            // fixme::checks -- need to verify that there's space, ie that compress() won't crush the ghost
        }
        src.jar = jar+shift;
        dst.rawcopy( src, jar, src.jar, src.bs-src.pmeta-src.jar );
    }
    void check(Sheet page) {
        Simple.softAssert(page.num*page.size <= page.jar);
    }
    Sheet splitPage(Path<Sheet> path,CC context,Sheet left,Sheet right) throws Pausable {
        Sheet    page0       = path.page;
        Sheet          page1 = createPage(right==null,context);
        int ksplit = bisect(path,context,page1,left,right);
        return page1;
    }
    protected int bisect(Path<Sheet> path,CC cc,Sheet page1,Sheet left,Sheet right) throws Pausable {
        Sheet page = path.page;
        int ko=path.ko, kb=0;
        int cs = left==null 
                ? keys.size(cc.key,cc.keydata) + vals.size(cc.val,cc.valdata)
                : keys.size(left,left.num-1) + dexsize;
        int size2 = page.size()+cs+mmeta;
        int thresh = size2/2;
        int size = mmeta;
        boolean dyn = keys.dynlen | vals.dynlen & page.leaf==1;
        if (dyn) for (; kb < page.num; kb++) {
            if (kb==ko) {
                if (size+cs >= thresh) {
                    if (thresh-size < size+cs-thresh) { path.page = page1; path.ko = 0; }
                    else size += cs;
                    break;
                }
                size += cs;
            }
            int delta = keys.size(page,kb) + (left==null ? vals.size(page,kb):dexsize);
            if (size+delta >= thresh) {
                int ii = kb;
                if (size+delta-thresh <= thresh-size) { kb++; size += delta; }
                if (ko > ii) { path.page = page1; path.ko -= kb; }
                break;
            }
            size += delta;
        }
        else
            kb = bisectFixed(path,page1);
        // fixme -- if confident in the bisect size calc, move the split to the caller
        //   better modularization
        //   but for now, this allows the debug info below
        bisect(path,page,cc,page1,left,right,kb);
        boolean dbg = false;
        if (dbg & dyn) {
            int d2 = size(page1) - size;
            int d3 = keys.size(page1,0)+4;
            int diff = size(page)+page.del-size;
            int d4 = size2 - size;
            if (d2 > d3 | diff != 0 | d4 != size(page1))
                Simple.softAssert(false);
        }
        return kb;
    }
    void branch(Path<Sheet> path,Sheet page,CC cc,Sheet page1,Sheet left,Sheet right,int kb) {
        shift(path.page,path.ko);
        path.page.dexs(path.ko,left.kpage);
        int jo = page.jar;
        key(path.page,path.ko,left,left.num-1);
        if (path.ko==path.page.num-1) {
            Simple.softAssert(page==path.page);
            // ie, it's the left page and add as the final element
            page.del = jo-page.jar; // replace the ghost
            page1.dexs(0,right.kpage);
        }
        else
            path.page.dexs(path.ko+1,right.kpage);
    }
    void bisect(Path<Sheet> path,Sheet page,CC cc,Sheet page1,Sheet left,Sheet right,int kb) throws Pausable {
        prep(page);
        /* fixme::perf -- rather than precomputing the split-point, should be able to compute it on the fly
         * using TestDF.SSmeta.DF+Testers, dynamic bisect is 15% slower than bisectFixed (4.6s vs 4s)
         * walk thru the src left to right, copying the payload to dst
         * when dst.jar reaches threshhold, copy the rest of the key/vals to dst and continue */
        page.split(page1,kb);
        if (keys.dynlen | vals.dynlen & page.leaf==1)
            copyPayload(page,page1);
        // soup.b6 does weird things with insert and requires changes to already be committed in that case
        //   keeping it the same in srlutils.btree for easier sync in the future even tho it's a kludge
        if (right==null) {
            commit(page,cc);
            commit(page1,cc);
            insert(path.page,cc,path.ko);
        }
        else {
            if (right != null) branch(path,page,cc,page1,left,right,kb);
            commit(page,cc);
            commit(page1,cc);
        }
        checkDel(page,false);
        checkDel(page1,false);
    }
    int size(Sheet page) {
        int size2 = bs - (page.jar - page.num*page.size) - page.del;
        return size2;
    }

    void checkDel(Sheet page,boolean force) {
        if (!(checkDel | force)) return;
        int size1 = mmeta;
        if (page.leaf==1)
            for (int ii = 0; ii < page.num; ii++)
                size1 += keys.size(page,ii) + vals.size(page,ii);
        else {
            for (int ii = 0; ii < page.numkeys(); ii++)
                size1 += keys.size(page,ii) + dexsize;
            if (page.num > 0) 
                size1 += keysize+dexsize;
        }
        int size2 = bs - (page.jar - page.num*page.size) - page.del;
        Simple.softAssert(size1==size2);
    }
    
    
    
    /*
     * performance note: deferred-compress (ie sparse jar) vs shift-in-place (dense), ie deletion strategies
     *   can either leave holes in jar and compress when needed on insert
     *   or immediately shift the jar (to fill the hole) and offset all the key/value pointers into the jar
     * best case: batch removes (eg TestDF.SSmeta), dense jar is 42% slower than the sparse jar (ie deferred)
     * worst case: alternating inserts and removes at near capacity ... don't have a test case yet
     * fixme::performance -- need to test alternating inserts and removes
     */
    /**
     * compress page if needed
     * prereq: space exists to add cc or left[ko] to page
     */
    void compress(Sheet page,CC cc,int ko,Sheet left,boolean leaf) {
        if (!Bstring.ValsVar.sparseDelete) return;
        if (space(page,cc,leaf,left) < page.del) {
            checkDel(page,false);
            Sheet tmp = newPage(leaf,cc,true);
            int jo=tmp.jar, jar = page.jar = compress(page,tmp,page,tmp.jar,0);
            tmp.rawcopy(page,jar,jar,jo-jar);
            page.del = 0;
            checkDel(page,false);
        }
    }
    protected void setcc(Sheet page,CC cc,int ko) {
        check(page);
        keys.set(page,ko,cc.key,cc.keydata);
        vals.set(page,ko,cc.val,cc.valdata);
    }
    protected void getcc(Sheet page,CC cc,int ko) {
        cc.key = keys.get(page,ko);
        cc.val = vals.get(page,ko);
    }
    protected void key(Sheet p0,int k0,Sheet p1,int k1) {
        p1.rawcopy(p0,k1,k0,pkey,keysize);
        if (keys.dynlen)
            p0.jar -= keys.copyPayload(p1,p0,p0,k0,p0.jar,p0.jar);
        check(p0);
    }

    protected void merge(Sheet p1,Sheet p2,Sheet parent,int n1,int n2,int kp,CC context) {
        checkDel(p1,false);
        checkDel(p2,false);
        int ko = p1.num-1;
        merge(p2,p1);
        free(p2);
        if (p1.leaf==0)
            key(p1,ko,parent,kp);
        checkDel(p1,false);
        parent.rawcopy(parent,kp,kp+1,pdex,dexsize);
        delete(parent,kp);
        commit(p1,context);
        commit(parent,context);
        check(parent);
    }
    
    boolean overcap(Sheet page,CC cc,boolean leaf,Sheet left) {
        return space(page,cc,leaf,left) < 0;
    }
    /** return the number of bytes of free space that would remain after the insertion of cc or left */
    protected int space(Sheet page,CC cc,boolean leaf,Sheet left) {
        int delta = leaf
                ? keys.size(cc.key,cc.keydata) + vals.size(cc.val,cc.valdata)
                : keys.size(left,left.num-1) + dexsize;
        return (page.jar+page.del) - (page.size*page.num + delta);
    }
    public CC insert(Transaction tid,KK key,VV value) throws Pausable {
        CC context = context().set(tid).set(key,value);
        insert(context);
        return context;
    }
    public final void insert(CC context) throws Pausable {
        if (keys.dynlen | vals.dynlen)       insert2(context);
        else                           super.insert (context);
    }
    int delete(Sheet page,int index) {
        prep(page);
        // copy the lower jar into the gap
        // iterate the keys+vals and offset any in the lower jar
        boolean dbg = false;
        if (dbg) dump(page,"pre : ");
        if (dbg) System.out.println("--------------------------------------------------");
        {
            boolean keyed = page.leaf==1 || index < page.num-1;
            boolean kd = keys.dynlen, vd = vals.dynlen && page.leaf==1;
            Bstring.ValsFace ko = kd ? (Bstring.ValsFace) keys : null;
            Bstring.ValsFace vo = vd ? (Bstring.ValsFace) vals : null;
            if (kd && keyed)         ko.shift(page,index  ,ko,vo);
            else if (kd & index > 0) ko.shift(page,index-1,ko,vo);
            if (vd)                  vo.shift(page,index  ,ko,vo);
        }
        int ret = page.delete(index);
        check(page);
        if (dbg) dump(page,"post: ");
        if (dbg) System.exit(0);
        checkDel(page,dbg);
        return ret;
    }
    protected int size(Sheet page,Sheet other,CC cc,boolean leaf,Sheet parent,int kp) {
        int base = super.size(page,other,null,leaf,parent,kp);
        if (cc != null) {
            base += keys.size(cc.key,cc.keydata);
            base += leaf ? vals.size(cc.val,cc.valdata) : dexsize;
        }
        base += (bs-mmeta-page.jar-page.del);
        if (other != null) base += (bs-mmeta-other.jar-other.del);
        if (parent != null) base += keys.size(parent,kp) - keysize;
        return base;
    }
    public CC context() { return (CC) new Context(); }
    protected int compare(Sheet page,int index,CC data) {
        return keys.compare( data.key, page, index, data.keydata );
    }
    public VV find(Transaction tid,KK key) throws Pausable {
        CC context = (CC) context().set(tid).set(key,null);
        findData(context);
        return context.match ? context.val : null;
    }
    public class Range extends Btree.Range<CC> {
        public Range() { super(Bmeta.this); }
        public ArrayList<KK> keys() throws Pausable {
            ArrayList<KK> vals = new ArrayList();
            while (next()) vals.add(cc.key);
            return vals;
        }
        public ArrayList<VV> vals() throws Pausable {
            ArrayList<VV> vals = new ArrayList();
            slurp(p1,p2,cc);
            while (next())
                vals.add(cc.val);
            return vals;
        }
        public Range prep() throws Pausable {
            slurp(p1,p2,cc);
            return this;
        }
    }
    public Range findRange(Transaction tid,KK key1,KK key2) throws Pausable {
        return (Range) findRange(context().set( tid ).set(key1, null),
                context().set( tid ).set(key2, null));
    }
    public Range findPrefix(Transaction tid,KK key) throws Pausable {
        return (Range) findPrefix(context().set( tid ).set(key, null));
    }

    public Range getall(CC context) throws Pausable { return (Range) super.getall(context); }
    public Range getall(Transaction tid) throws Pausable { return (Range) super.getall(context().set(tid)); }
    
    protected Range range() { return new Range(); }

    void toastPage(Path<Sheet> path,CC context) throws Pausable {
        for (int ii=0; ii < path.page.num; ii++)
            prepx(path.page,context,ii);
        context.txn.submitYield();
    }

    
    
    public static abstract class Toast
    <KK,VV,EE extends Btypes.Element<KK,?>> extends Bmeta<Toast<KK,VV,EE>.Data,KK,VV,EE> {
        ValsVarx<VV,Data> v2;
        public class Data extends Bmeta.Context<KK,VV,Data> {}
        public Data context() { return new Data(); }
        protected int compare(Bpage.Sheet page,int index,Data data) {
            return keys.compare( data.key, page, index, data.keydata );
        }
        protected void setccx(Bpage.Sheet page,Data cc,int ko) throws Pausable {
            check(page);
            keys.set(page,ko,cc.key,cc.keydata);
            v2.setx(cc.txn,page,ko,cc.val,cc.valdata);
        }
        protected void getccx(Bpage.Sheet page,Data cc,int ko) throws Pausable {
            check(page);
            cc.key = keys.get(page,ko);
            cc.val = v2.getx(cc.txn,page,ko);
        }

        protected void prepx(Sheet page,Data context,int ko) {
            v2.prepx(context.txn,page,ko);
        }
        
        protected boolean isToast(Data data) { return v2.isToast(data.val,data.valdata); }
        protected int findLoop(Bpage.Sheet page,int k1,int num,int step,Data context,boolean greater) {
            for (; k1<num; k1+=step) {
                int cmp = keys.compare(context.key,page,k1,null);
                if (greater & cmp==0) cmp = 1;
                if (cmp <= 0) break;
            }
            if (step > 1)
                return findLoop(page,k1-step,num,1,context,greater);
            return k1;
        }
    }
    public static abstract class Itoast<TT> extends Toast<Integer,TT,Btypes.ValsInt> {
        protected int findLoop(Bpage.Sheet page,int k1,int num,int step,Toast<Integer,TT,Btypes.ValsInt>.Data context,boolean greater) {
            for (; k1<num; k1+=step) {
                int cmp = keys.compare(context.key,page,k1,null);
                if (greater & cmp==0) cmp = 1;
                if (cmp <= 0) break;
            }
            if (step > 1)
                return findLoop(page,k1-step,num,1,context,greater);
            return k1;
        }
    }






    static class DF extends Bmeta<DF.Data,Double,Float,Btypes.ValsDouble>
    {
        public DF() { setup(new Btypes.ValsDouble(),new Btypes.ValsFloat()); }
        public static class Data extends Bmeta.Context<Double,Float,Data> implements TestDF.DFcontext<Data> {
            public Data set(double key) { return super.set(key,-1f); }
            public Data set(double key,float val) { return super.set(key,val); }
            public float val() { return val; }
        }
        protected int findLoop(Sheet page,int k1,int num,int step,Data context,boolean greater) {
            for (; k1<num; k1+=step) {
                int cmp = keys.compare(context.key,page,k1,null);
                if (greater & cmp==0) cmp = 1;
                if (cmp <= 0) break;
            }
            if (step > 1)
                return findLoop(page,k1-step,num,1,context,greater);
            return k1;
        }
        public Data context() { return new Data(); }
    }    
    static class DF2 extends Bmeta<DF2.Data,String,String,ValsString> {
        { setup(new ValsString(),new ValsString()); }
        public static class Data extends Bmeta.Context<String,String,Data> implements TestDF.DFcontext<Data> {
            public Data set(double $key,float $val) { key = $key+""; val = $val+""; return this; }
            public Float get2() { return Float.parseFloat(val); }
            public Data set(double key) { return set(key,-1f); }
            public float val() { return Float.parseFloat(val); }
        }
        public Data context() { return new Data(); }
        protected int findLoop(Sheet page,int k1,int num,int step,Data context,boolean greater) {
            for (; k1<num; k1+=step) {
                int cmp = keys.compare(context.key,page,k1,context.keydata);
                if (greater & cmp==0) cmp = 1;
                if (cmp <= 0) break;
            }
            if (step > 1)
                return findLoop(page,k1-step,num,1,context,greater);
            return k1;
        }
        public void dump(Sheet page,Data cc) {
            boolean brah = page.leaf==0;
            System.out.format( "page %5d, %5d items, %5d jar, %s\n", page.kpage, page.num, page.jar,
                    brah ? "branch":"leaf" );
            if (brah) for (int ii = 0; ii < page.num; ii++) {
                String key = keys.get(page,ii);
                int kp = page.dexs(ii);
                System.out.format( "\t%4d: %8s %5d\n", ii, key, kp );
            }
            else for (int ii = 0; ii < page.num; ii++) {
                getcc(page,cc,ii);
                System.out.format( "\t%4d: %8s %8s\n", ii, cc.key, cc.val );
            }
        }
    }
    abstract static class T20<K1,K2,CC extends T20.Context<K1,K2,CC>>
                                                    extends Bmeta<CC,Object [],Void,ValsTuple> {
        public T20(Element<K1,?> e1,Element<K2,?> e2) { setup( new ValsTuple(e1,e2), new ValsVoid() ); }
        public static class Context<K1,K2,CC extends Context<K1,K2,CC>> extends Bmeta.Context<Object [],Void,CC> {
            public CC setx(K1 key1,K2 key2) { return super.set(new Object[]{key1,key2},null); }
        }
    }
    
    static class DF4 extends T20<Double,Float,DF4.Data> {
        public DF4() { super(new ValsDouble(),new ValsFloat()); }
        public static class Data extends T20.Context<Double,Float,Data> {}
        public Data context() { return new Data(); }
    }

    static class Demo {
        Btrees.IA lt;
        String filename = DemoHunker.resolve("./db_files/b6.mmap");
        Db4j db4j;
        int [] keys;
        public static void main2(String [] args) {
            Long seed = null;
            Rand.source.setSeed(seed,true);
            new Demo().demo();
        }
        public void demo() {
            db4j = new Db4j().init( filename, null );
            lt = db4j.register(new Btrees.IA(),"Bushy Tree");
            db4j.create();
            db4j.guts.fence(null,100);
            db4j.guts.forceCommit(100);
            final int ipt=8, bpe=770, size=1<<25, len=Simple.Rounder.rup(size/bpe,ipt);
            keys = Rand.source.rand( new int[len], 0, 1<<30 );
            for (int ii=0; ii < keys.length; ii += ipt) {
                final int i2 = ii;
                class Insert extends Query { public void task() throws Pausable {
                    for (int jj = 0; jj < ipt; jj++) {
                        int nn = 4 + Rand.source.rand(0,bpe);
                        byte [] bytes = new byte[nn];
                        int ko = i2+jj;
                        Arrays.fill(bytes,(byte) ko);
                        Command.Page.wrap(bytes).putInt(0,ko);
                        int key = keys[ko];
                        lt.context().set(tid).set(key,bytes).insert(lt);
                    }
                }};
                new Insert().offer(db4j);
            }
            db4j.guts.fence(null,100);
            db4j.shutdown();
            db4j = Db4j.load(filename);
            lt = (Btrees.IA) db4j.arrays.get(0);
            check(keys.length);
            db4j.shutdown();
            lt.clear();
        }
        void check(int nn) {
            for (int ii=0; ii < nn; ii++) {
                final int i2 = ii;
                class Check extends Db4j.Query { public void task() throws Pausable {
                    int key = keys[i2];
                    Btrees.IA.Data data = lt.context().set(tid).set(key,null).find(lt);
                    int val = Command.Page.wrap(data.val).getInt(0);
                    if (val != i2) {
                        System.out.format( "check -- %5d %5d %5d %5d\n", val, i2, keys[val], key );
                        Simple.softAssert(val==i2 || keys[val]==key);
                    }
                }};
                new Check().offer(db4j);
            }
            db4j.guts.fence(null,100);
        }
    }
    static class Mindir2 extends BtTests2 {
        DF2 lt;
        String filename = DemoHunker.resolve("./db_files/b6.mmap");
        Db4j db4j;
        boolean ok = true;
        boolean nocheck = true;
        boolean reopen = false;
        { sntext = "put get rem chk"; }
        public void init2() {
            db4j = new Db4j().init( filename, null ); // 1L << 32 );
            lt = db4j.register(new DF2(),"Bushy Tree");
            db4j.create();
            db4j.guts.fence(null,100);
            db4j.guts.forceCommit(100);
        }
        public void run(int stage) throws Exception {
            if (reopen)
                db4j = Db4j.load(filename);
            lt = (DF2) db4j.arrays.get(0);
            stage(stage);
            if (reopen) {
                db4j.shutdown();
            }
        }
        public void stage(final int stage) {
            for (int ii = 0; ii < tc.nn; ii++) {
                final int jj = ii;
                final float v1 = 0.01f*jj;
                final int jo = 1000000, step = 1;
                final boolean chk = !nocheck && stage==2 && jj >= jo && (jj%step==0);
                Db4j.Query task = new Db4j.Query() { public void task() throws Pausable {
                    DF2.Data context = lt.context().set(tid).set(keys[jj],stage==0 ? v1:-1f);
                    if (jj==0 && stage==2) check(1,tc.nn,1);
                    if (chk && stage==2)
                        Simple.nop();
                    if      (stage==0) lt.insert(context);
                    else if (stage==2) {
                        lt.remove(context);
                        Simple.softAssert(context.match,"Mindir.remove.nomatch %d %f",jj,keys[jj]);
                    }
                    else {
                        lt.findData(context);
                        boolean aok = true;
                        if (context.get2() != (stage==1 ? v1:-1f)) ok = aok = false;
                        if (!aok)
                            System.out.format( "not ok %3d:%5d ... %8.3f --> %8.3f\n",
                                    stage, jj, keys[jj], context.get2() );
                    }
                    if (chk) {
                        check(0,tc.nn,1);
                        System.out.format( "chk.2 completed -- %5d\n", jj );
                    }
                }
                public void check(int ko,int nn,int delta) throws Pausable {
                    if (nocheck) return;
                    for (int kk = ko; kk < nn; kk += delta) {
                        DF2.Data context = lt.context().set(tid).set(keys[kk],-1f).find(lt);
                        float goal = (kk <= jj) ? -1f : 0.01f*kk;
                        Float val = context.match ? context.get2() : -1f;
                        if (val != goal)
                            Simple.softAssert(false,"insert corrupted: %d, %d, %8.3f <> %8.3f\n",jj,kk,val,goal);
                    }
                }
                };
                task.offer(db4j);
                if (chk) task.awaitb();
            }
            db4j.guts.fence(null,10);
        }
        public boolean finish() throws Exception {
            db4j.shutdown();
            lt.clear();
            return ok;
        }
        public static void auto(int passes,int npp,TaskTimer.Runner ... runners) throws Exception {
            Long seed = null;
//            seed = 8063614542396112692L;
            Rand.source.setSeed( seed, true );
            Simple.Scripts.cpufreqStash( 2300000 );
            int nn = 20000;
            BtTests2.Config tc = new BtTests2.Config().set( nn);
            TaskTimer tt = new TaskTimer().config( tc ).init( npp, 0, true, false );
            tt.widths( 8, 3 );
            for (int ii=0; ii<passes; ii++)
                tt.autoTimer(runners);
        }
        public static void main(String [] args) throws Exception {
            Mindir2 mindir = new Mindir2();
            if (args.length > 0) mindir.nocheck = false;
            auto(1,1,mindir);
        }
    }
    
}
