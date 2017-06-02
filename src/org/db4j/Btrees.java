// copyright 2017 nqzero - see License.txt for terms

package org.db4j;

import kilim.Pausable;
import java.io.Serializable;
import org.db4j.Bmeta.Itoast;
import org.db4j.Db4j;
import org.srlutils.DynArray;
import org.srlutils.Types;
import org.srlutils.btree.Bpage;
import org.srlutils.btree.Bstring;
import org.srlutils.btree.Btypes;
import org.srlutils.btree.Butil;

public class Btrees {
    // fixme - compare performance with toasted (and generic) Btrees.IS
    public static class IS2 extends Bmeta<IS2.Data,Integer,String,Btypes.ValsInt> {
        { setup(new Btypes.ValsInt(),new Bstring.ValsString()); }
        public static class Data extends Bmeta.Context<Integer,String,Data> {
            public Data set(int $key,String $val) { key = $key; val = $val; return this; }
        }
        public Data context() { return new Data(); }
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
        protected int compare(Bpage.Sheet page,int index,Data data) {
            return keys.compare( data.key, page, index, data.keydata );
        }
    }
    public static class IS extends Itoast<String> {
        { setup(new Btypes.ValsInt(),v2 = new Bhunk.ValsStringx()); }
    }
    public static class IO<TT extends Serializable> extends Itoast<TT> {
        { setup(new Btypes.ValsInt(),v2 = new Bhunk.ValsObject()); }
    }
    public static class IX extends Itoast<byte []> {
        { setup(new Btypes.ValsInt(),v2 = new Bhunk.ValsBytex()); }
    }
    public static class IK<TT> extends Itoast<TT> {
        { setup(new Btypes.ValsInt(),v2 = new Bhunk.ValsKryo()); }
    }
    public static class IA extends Bmeta<IA.Data,Integer,byte[],Btypes.ValsInt> {
        { setup(new Btypes.ValsInt(),new Bstring.ValsBytes()); }
        public static class Data extends Bmeta.Context<Integer,byte[],Data> {
            public Data set(int $key,byte [] $val) { key = $key; val = $val; return this; }
        }
        public Data context() { return new Data(); }
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
        protected int compare(Bpage.Sheet page,int index,Data data) {
            return keys.compare( data.key, page, index, data.keydata );
        }
    }
    public static class SI extends Bmeta<SI.Data,String,Integer,Bstring.ValsString> {
        { setup(new Bstring.ValsString(),new Btypes.ValsInt()); }
        public static class Data extends Bmeta.Context<String,Integer,Data> {
            public Data set(String $key,int $val) { key = $key; val = $val; return this; }
        }
        public Data context() { return new Data(); }
        protected int findLoop(Bpage.Sheet page,int k1,int num,int step,Data context,boolean greater) {
            for (; k1<num; k1+=step) {
                int cmp = keys.compare(context.key,page,k1,context.keydata);
                if (greater & cmp==0) cmp = 1;
                if (cmp <= 0) break;
            }
            if (step > 1)
                return findLoop(page,k1-step,num,1,context,greater);
            return k1;
        }
        protected int compare(Bpage.Sheet page,int index,Data data) {
            return keys.compare( data.key, page, index, data.keydata );
        }
        /** return a Join for entries that match all terms */
        public Join<Btrees.SI.Data> search(Db4j.Transaction txn,String ... terms) throws Pausable {
            Btree.Range<Btrees.SI.Data> [] ranges = new Btree.Range[terms.length];
            for (int ii=0; ii < terms.length; ii++)
                ranges[ii] = findPrefix(context().set(txn).set(terms[ii],null));
            return join(txn,ranges);
        }
    }
    
    public static class II extends Bhunk<II.Data> {
        public II() { init(Types.Enum._int.size,Types.Enum._int.size); }
        public class Data extends Bhunk.Context<Data> {
            public int key;
            public int val;
            public Data set(int $key,int $val) { key = $key; val = $val; return this; }
            public int get() { return val; }
            public void insert() throws Pausable { II.this.insert(this); }
            public Data find() throws Pausable { findData(this); return this; }
        }
        public Data context() { return new Data(); }
        public void setcc(Bpage.Sheet page,Data cc,int ko) {
            page.put(pkey,ko,cc.key);
            page.put(pval,ko,cc.val);
        }
        public void getcc(Bpage.Sheet page,Data cc,int ko) {
            cc.key = page.geti(pkey,ko);
            cc.val = page.geti(pval,ko);
        }
        int key(Bpage.Sheet page,int index) { return page.geti(pkey,index); }
        protected int compare(Bpage.Sheet page,int index,Data data) {
            return Butil.compare(data.key,key(page,index));
        }
        protected int findLoop(Bpage.Sheet page,int k1,int num,int step,Data context,boolean greater) {
            for (; k1<num; k1+=step) {
                int cmp = compare( page, k1, context );
                if (greater & cmp==0) cmp = 1;
                if (cmp <= 0) break;
            }
            if (step > 1)
                return findLoop(page,k1-step,num,1,context,greater);
            return k1;
        }
    }
    public static class IID extends Bhunk<IID.Data> {
        static int p1 = Types.Enum._int.size;
        public IID() { init(p1*2,Types.Enum._double.size); }
        public class Data extends Bhunk.Context<Data> {
            public int key1, key2;
            public double val;
            public int prefix;
            public Data set(int $key1,int $key2,double $val) { key1=$key1; key2=$key2; val = $val; return this; }
            public void insert() throws Pausable { insert(IID.this); }
            public Data find() throws Pausable { findData(this); return this; }
        }
        public Data context() { return new Data(); }
        public void setcc(Bpage.Sheet page,Data cc,int ko) {
            page.put(pkey,ko,cc.key1);
            page.put(pkey+p1,ko,cc.key2);
            page.put(pval,ko,cc.val);
        }
        public void getcc(Bpage.Sheet page,Data cc,int ko) {
            cc.key1 = page.geti(pkey,ko);
            cc.key2 = page.geti(pkey+p1,ko);
            cc.val = page.getd(pval,ko);
        }
        int key(Bpage.Sheet page,int index) { return page.geti(pkey,index); }
        protected int compare(Bpage.Sheet page,int index,Data data) {
            int x1=data.key1, x2=data.key2;
            int y1 = page.geti(pkey,index);
            if (x1 > y1) return 1;
            if (x1 < y1) return -1;
            if (data.prefix==1) return 0;
            return Butil.compare(x2,page.geti(pkey+p1,index));
        }
        protected int findLoop(Bpage.Sheet page,int k1,int num,int step,Data context,boolean greater) {
            for (; k1<num; k1+=step) {
                int cmp = compare( page, k1, context );
                if (greater & cmp==0) cmp = 1;
                if (cmp <= 0) break;
            }
            if (step > 1)
                return findLoop(page,k1-step,num,1,context,greater);
            return k1;
        }
    }
    public static class Idiv extends Bhunk<Idiv.Data> {
        static int p1 = Types.Enum._int.size, p2 = p1+Types.Enum._double.size;
        public Idiv() { init(p1+p2,0); }
        public class Data extends Bhunk.Context<Data> {
            public int key1, key3;
            public double key2;
            public Data set(int $key1,double $key2,int $key3) { key1=$key1; key2=$key2; key3=$key3; return this; }
            public void insert() throws Pausable { insert(Idiv.this); }
            public Data find() throws Pausable { findData(this); return this; }
        }
        public Data context() { return new Data(); }
        public void setcc(Bpage.Sheet page,Data cc,int ko) {
            page.put(pkey,ko,cc.key1);
            page.put(pkey+p1,ko,cc.key2);
            page.put(pkey+p2,ko,cc.key3);
        }
        public void getcc(Bpage.Sheet page,Data cc,int ko) {
            cc.key1 = page.geti(pkey,ko);
            cc.key2 = page.getd(pkey+p1,ko);
            cc.key3 = page.geti(pkey+p2,ko);
        }
        protected int compare(Bpage.Sheet page,int index,Data data) {
            int x1=data.key1, x3=data.key3;
            double x2=data.key2;
            int y1 = page.geti(pkey,index);
            if (x1 > y1) return 1;
            if (x1 < y1) return -1;
            double y2 = page.getd(pkey+p1,index);
            if (x2 > y2) return 1;
            if (x2 < y2) return -1;
            return Butil.compare(x3,page.geti(pkey+p2,index));
        }
        protected int findLoop(Bpage.Sheet page,int k1,int num,int step,Data context,boolean greater) {
            for (; k1<num; k1+=step) {
                int cmp = compare( page, k1, context );
                if (greater & cmp==0) cmp = 1;
                if (cmp <= 0) break;
            }
            if (step > 1)
                return findLoop(page,k1-step,num,1,context,greater);
            return k1;
        }
    }
    public static class Iiiv extends Bhunk<Iiiv.Data> {
        private static final int bpi = Types.Enum._int.size, p1=bpi, p2=2*bpi, size=3*bpi;
        public Iiiv() { init(size,0); }
        public class Data extends Bhunk.Context<Data> {
            public int key1, key2, key3;
            public Data set(int $key1,int $key2,int $key3) { key1=$key1; key2=$key2; key3=$key3; return this; }
            public void insert() throws Pausable { insert(Iiiv.this); }
            public Data find() throws Pausable { findData(this); return this; }
        }
        public Data context() { return new Data(); }
        public void setcc(Bpage.Sheet page,Data cc,int ko) {
            page.put(pkey,ko,cc.key1);
            page.put(pkey+p1,ko,cc.key2);
            page.put(pkey+p2,ko,cc.key3);
        }
        public void getcc(Bpage.Sheet page,Data cc,int ko) {
            cc.key1 = page.geti(pkey,ko);
            cc.key2 = page.geti(pkey+p1,ko);
            cc.key3 = page.geti(pkey+p2,ko);
        }
        protected int compare(Bpage.Sheet page,int index,Data data) {
            int x1=data.key1, x2=data.key2, x3=data.key3;
            int y1 = page.geti(pkey   ,index);
            int y2 = page.geti(pkey+p1,index);
            int y3 = page.geti(pkey+p2,index);
            return x1==y1
                    ? (x2==y2)
                        ? (x3>y3 ? 1 : (x3==y3 ? 0:-1))
                        : (x2>y2 ? 1 : -1)
                    :     (x1>y1 ? 1 : -1);
        }
        protected int findLoop(Bpage.Sheet page,int k1,int num,int step,Data context,boolean greater) {
            for (; k1<num; k1+=step) {
                int cmp = compare( page, k1, context );
                if (greater & cmp==0) cmp = 1;
                if (cmp <= 0) break;
            }
            if (step > 1)
                return findLoop(page,k1-step,num,1,context,greater);
            return k1;
        }
    }
    public static class IL extends Bmeta<IL.Data,Integer,Long,Btypes.ValsInt> {
        public IL() { setup(new Btypes.ValsInt(),new Btypes.ValsLong()); }
        public static class Data extends Bmeta.Context<Integer,Long,Data> {}
        public Data context() { return new Data(); }
        protected int findLoop(Bpage.Sheet page,int k1,int num,int step,Data context,boolean greater) {
            for (; k1<num; k1+=step) {
                int cmp = compare( page, k1, context );
                if (greater & cmp==0) cmp = 1;
                if (cmp <= 0) break;
            }
            if (step > 1)
                return findLoop(page,k1-step,num,1,context,greater);
            return k1;
        }
    }    
    public static class IF extends Bmeta<IF.Data,Integer,Float,Btypes.ValsInt> {
        public IF() { setup(new Btypes.ValsInt(),new Btypes.ValsFloat()); }
        public static class Data extends Bmeta.Context<Integer,Float,Data> {}
        public Data context() { return new Data(); }
        protected int findLoop(Bpage.Sheet page,int k1,int num,int step,Data context,boolean greater) {
            for (; k1<num; k1+=step) {
                int cmp = compare( page, k1, context );
                if (greater & cmp==0) cmp = 1;
                if (cmp <= 0) break;
            }
            if (step > 1)
                return findLoop(page,k1-step,num,1,context,greater);
            return k1;
        }
    }    
    public static class ID extends Bmeta<ID.Data,Integer,Double,Btypes.ValsInt> {
        public ID() { setup(new Btypes.ValsInt(),new Btypes.ValsDouble()); }
        public static class Data extends Bmeta.Context<Integer,Double,Data> {}
        public Data context() { return new Data(); }
        protected int findLoop(Bpage.Sheet page,int k1,int num,int step,Data context,boolean greater) {
            for (; k1<num; k1+=step) {
                int cmp = compare( page, k1, context );
                if (greater & cmp==0) cmp = 1;
                if (cmp <= 0) break;
            }
            if (step > 1)
                return findLoop(page,k1-step,num,1,context,greater);
            return k1;
        }
    }    
    public static class LL extends Bmeta<LL.Data,Long,Long,Btypes.ValsLong> {
        public LL() { setup(new Btypes.ValsLong(),new Btypes.ValsLong()); }
        public static class Data extends Bmeta.Context<Long,Long,Data> {}
        public Data context() { return new Data(); }
        protected int findLoop(Bpage.Sheet page,int k1,int num,int step,Data context,boolean greater) {
            for (; k1<num; k1+=step) {
                int cmp = compare( page, k1, context );
                if (greater & cmp==0) cmp = 1;
                if (cmp <= 0) break;
            }
            if (step > 1)
                return findLoop(page,k1-step,num,1,context,greater);
            return k1;
        }
    }    
    public static class LF extends Bmeta<LF.Data,Long,Float,Btypes.ValsLong> {
        public LF() { setup(new Btypes.ValsLong(),new Btypes.ValsFloat()); }
        public static class Data extends Bmeta.Context<Long,Float,Data> {}
        public Data context() { return new Data(); }
        protected int findLoop(Bpage.Sheet page,int k1,int num,int step,Data context,boolean greater) {
            for (; k1<num; k1 += step) {
                int cmp = compare(page,k1,context);
                if (greater&cmp==0) cmp = 1;
                if (cmp<=0) break;
            }
            if (step>1)
                return findLoop(page,k1-step,num,1,context,greater);
            return k1;
        }
    }    
    public static class LD extends Bmeta<LD.Data,Long,Double,Btypes.ValsLong> {
        public LD() { setup(new Btypes.ValsLong(),new Btypes.ValsDouble()); }
        public static class Data extends Bmeta.Context<Long,Double,Data> {}
        public Data context() { return new Data(); }
        protected int findLoop(Bpage.Sheet page,int k1,int num,int step,Data context,boolean greater) {
            for (; k1<num; k1+=step) {
                int cmp = compare( page, k1, context );
                if (greater & cmp==0) cmp = 1;
                if (cmp <= 0) break;
            }
            if (step > 1)
                return findLoop(page,k1-step,num,1,context,greater);
            return k1;
        }
    }    


    /** the join of several monotonic ranges */
    public static class Join<CC extends Bmeta.Context<?,Integer,?>> {
        public Btree.Range<? extends Bmeta.Context<?,Integer,?>> [] ranges;
        public CC cc;

        Db4j.Transaction txn;
        int num, max=-1, neq;
        int [] vals;
        boolean remain = true;
        boolean first = true;
        
        /**
         * join several monotonic ranges (note: prefix searches will not be monotonic). 
         * must call init() before usage
         * @param txn
         * @param ranges must be monotonic
         */
        Join(Db4j.Transaction txn,Btree.Range<CC> ... ranges) {
            this.txn = txn;
            this.ranges = ranges;
            num = ranges.length;
            vals = new int[num];
            if (ranges.length > 0)
                cc = ranges[0].cc;
        }
        /** initialize the join */
        Join<CC> init() throws Pausable {
            for (int ii=0; ii < num; ii++) {
                if (ranges[ii].init()) calc(ii);
                else remain = false;
            }
            return this;
        }
        void calc(int ii) {
            int val = vals[ii] = ranges[ii].cc.val;
            if (val > max) { max = val; neq = 1; }
            else if (val==max) neq++;
        }
        /** return an array of matching values */
        public int [] dump() throws Pausable {
            DynArray.ints r2 = new DynArray.ints();
            r2.grow(16);
            while (next())
                r2.add(vals[0]);
            return r2.trim();
        }
        /** advance to the next match and return true if it exists */
        public boolean next() throws Pausable {
            hasnext();
            first = true;
            return remain;
        }
        /** advance to the next match but do not consume it, and return true if it exists */
        public boolean hasnext() throws Pausable {
            if (first & remain) remain = gonext();
            first = false;
            return remain;
        }
        boolean gonext() throws Pausable {
            for (int ii=0; neq < num; ii = (++ii==num) ? 0:ii)
                while (vals[ii] < max)
                    if (! ranges[ii].next()) return false;
                    else calc(ii);
            neq = 0;
            max++;
            return true;
        }
    }
    
    
    /** return an initialized Join for the monotonic ranges */
    public static <CC extends Bmeta.Context<?,Integer,?>> Join<CC> join
            (Db4j.Transaction txn,Btree.Range<CC>... ranges) throws Pausable {
        Join joiner = new Join(txn,ranges).init();
        return joiner;
    }
    
}
