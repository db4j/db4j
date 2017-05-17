// copyright 2017 nqzero - see License.txt for terms

package org.db4j;

import java.util.Iterator;
import kilim.Pausable;
import org.srlutils.Simple;
import org.srlutils.Util;

public abstract class Command implements Cloneable {
    
    protected static class Page {
        private static final sun.misc.Unsafe uu = Simple.Reflect.getUnsafe();
        static final long bao = uu.arrayBaseOffset(byte[].class);
        public byte [] data;
        public boolean dup;

        public Page() {}

        public int    getInt   (int offset) { return uu.getInt   (data,bao+offset); }
        public byte   getByte  (int offset) { return uu.getByte  (data,bao+offset); }
        public double getDouble(int offset) { return uu.getDouble(data,bao+offset); }
        public float  getFloat (int offset) { return uu.getFloat (data,bao+offset); }
        public long   getLong  (int offset) { return uu.getLong  (data,bao+offset); }
        public short  getShort (int offset) { return uu.getShort (data,bao+offset); }
        public void putDouble(int offset,double val) { uu.putDouble(data,bao+offset,val); }
        public void putByte  (int offset,byte   val) { uu.putByte  (data,bao+offset,val); }
        public void putInt   (int offset,int    val) { uu.putInt   (data,bao+offset,val); }
        public void putFloat (int offset,float  val) { uu.putFloat (data,bao+offset,val); }
        public void putLong  (int offset,long   val) { uu.putLong  (data,bao+offset,val); }
        public void putShort (int offset,short  val) { uu.putShort (data,bao+offset,val); }
        public void put      (int offset,byte[] val) { uu.copyMemory(val,bao,data,bao+offset,val.length); }
        public void get      (int offset,byte[] val) { uu.copyMemory(data,bao+offset,val,bao,val.length); }
        
        public void copy(int offset,Object array,int arrayOffset,int len) {
            long ao = uu.arrayBaseOffset(array.getClass());
            uu.copyMemory(data,bao+offset,array,ao+arrayOffset,len);
        }
        public void copyFrom(int offset,Object array,int arrayOffset,int len) {
            long ao = uu.arrayBaseOffset(array.getClass());
            uu.copyMemory(array,ao+arrayOffset,data,bao+offset,len);
        }
        public static long arrayOffset(Class klass) { return uu.arrayBaseOffset(klass); }
        public static Page wrap(byte [] $data) {
            Page buf = new Page();
            buf.data = $data;
            return buf;
        }
        public Page order(Object dummy) { return this; }
        public Page dupify() {
            if (!dup & data != null) data = Util.dup(data);
            dup = true;
            return this;
        }
    }
    
    /** the txn that owns this command, may be null for immediate writes eg HunkLog.set */
    protected Db4j.Transaction txn;
    /** link to the next cmd, used in Transaction.reads and Transaction.cmds */
    protected Command next;
    protected String msg;
    protected long offset;
    /** 
     *  txn count at the time the read is executed
     *  @see Db4j.QueRunner#genc
     */
    protected long gen = Db4j.QueRunner.gencUsageMarker(0);

    
    protected static void print(Db4j db4j,Command cmd) {
        long kp = cmd.offset >> db4j.bb;
        long ko = cmd.offset & db4j.bm;
        String info = cmd.info();
        char status = cmd.write() ? 'w':'r';
        if (cmd.done()) status = Character.toUpperCase(status);
        System.out.format( "%8d.%4d%c %s\n", kp, ko, status, info );
    }
    protected static void printAll(Db4j db4j,Command cmd) {
        for (; cmd != null; cmd = cmd.next) print(db4j,cmd);
    }
    protected static void print(Db4j db4j,Iterable<Command> cmds) {
        for (Command cmd : cmds) print(db4j,cmd);
    }
    protected String info() {
        return getClass().getSimpleName();
    }

    /** is the command a write ??? */
    protected abstract boolean write();
    protected abstract int size();
    public abstract boolean done();
    protected abstract void read(Page buf,int offset);
    protected abstract void write(Page buf,int offset);

    // fixme - add documentation on the command lifecycle
    
    /**
     * run the command
     * @param offset the offset within the buffer to apply the command at
     * @param buf the buffer to read from / write to
     * @param db4j db4j instance
     * @param defer the buffer needs to be dupified before use
     */
    protected void run(int offset,Page buf,Db4j db4j,boolean defer) {
        boolean write = write();
        // fixme::kludge -- this works with the current set of commands (Nop isn't a write, Init overrides)
        if (write && buf.data==null) return;
        if (write) write( buf, offset );
        else        read( buf, offset );
    }

    /**
     * only called for reads, after the read has been run
     * @param db4j the db4j instance
     */
    protected void book(Db4j db4j) {}

    /** free resources associated with the command after the command has been run */
    protected void clean() {}

    /** return a clone of the command */
    protected Command dup() {
        try { return (Command) this.clone(); }
        catch (CloneNotSupportedException ex) { throw new RuntimeException( ex ); }
    }
    /**
     * if the command were applied to the buffer would it result in a potentially different value,
     * called only for read commands. the default implementation returns true
     * @param buf the buffer to apply
     * @param offset the offset within the to apply the command at
     * @return false if it is known that the value would be identical
     */
    protected boolean changed(Page buf,int offset) { return true; }
    /** if the underlying cache has changed, does that automatically imply that the cmd has changed */
    protected boolean unchangeable() { return true; }

    protected static class Nop extends Command {
        protected boolean done, write = false;
        public Nop() {}
        protected void book(Db4j db4j) { done = true; }
        protected boolean write() { return write; }
        protected int size() { return 0; }
        public boolean done() { return done; }
        protected void read(Page buf,int offset) {}
        protected void write(Page buf,int offset) {}
    }

    protected static class Init extends Nop {
        protected byte [] data;
        { write = true; }
        public Init() {}
        protected void run(int offset,Page buf,Db4j db4j,boolean defer) {
            if (true) { buf.data = new byte[db4j.bs]; return; }
            if (data==null) data = new byte[db4j.bs];
            buf.data = data;
        }
    }
    
    /** read/write command. SS: the type of the command */
    public static abstract class Rw<SS extends Rw> extends Command {
        protected byte status;
        // notify: notify the object's monitor once the command has run
        protected static byte done = 1, write = 1<<1;
        public Rw() {}

        public SS init(boolean dowrite) {
            if (dowrite) status |= write;
            return (SS) this;
        }
        protected SS dup() { return (SS) super.dup(); }

        public boolean done() { return (status & done) != 0; }
        protected boolean write() { return (status & write) != 0; }

        protected void book(Db4j db4j) {
            status |= done;
        }
        public SS yield() throws Pausable {
            txn.submitYield();
            return (SS) this;
        }
        public SS add(Db4j db4j,Db4j.Transaction txn) { db4j.put(txn,this); return (SS) this; }
    }


    /**
     * an abstract command that represents a backing value
     * @param <TT> the (possibly boxed) type of the backing value
     * @param <SS> the type of the command (to allow pretty return values and This-chaining)
     */
    public static abstract class RwPrimitive<TT,SS extends RwPrimitive> extends Rw<SS> {
        /** return the backing value           */  public abstract TT val();
        /** set the backing value              */  protected abstract void set2(TT val);
        /** set the backing value, return this */  public SS set(TT value) { set2( value ); return (SS) this; }
        public String toString() { return (write() ? "w" : "r") + val(); }
        protected boolean changed(Page buf,int offset) {
            TT vo = val();
            read(buf,offset);
            TT v1 = val();
            return ! vo.equals( v1 );
        }
        protected boolean unchangeable() { return false; }
        protected String info() {
            return getClass().getSimpleName() + " val:" + val();
        }
    }

    public static class RwByte extends RwPrimitive<Byte,RwByte> {
        public RwByte() {}
        public byte val;
        protected void read(Page buf,int offset) { val = buf.getByte( offset ); }
        protected void write(Page buf,int offset) { buf.putByte( offset, val ); }
        public Byte val() { return val; }
        public void set2(Byte value) { val = value; }
        public int size() { return org.srlutils.Types.Enum._int.size; }
    }
    public static class RwInt extends RwPrimitive<Integer,RwInt> {
        public RwInt() {}
        public int val;
        protected void read(Page buf,int offset) { val = buf.getInt( offset ); }
        protected void write(Page buf,int offset) { buf.putInt( offset, val ); }
        public Integer val() { return val; }
        protected void set2(Integer value) { val = value; }
        public int size() { return org.srlutils.Types.Enum._int.size; }
    }
    public static class RwLong extends RwPrimitive<Long,RwLong> {
        public RwLong() {}
        public long val;
        protected void read(Page buf,int offset) { val = buf.getLong( offset ); }
        protected void write(Page buf,int offset) { buf.putLong( offset, val ); }
        public Long val() { return val; }
        protected void set2(Long value) { val = value; }
        public int size() { return org.srlutils.Types.Enum._long.size; }
    }
    public static class RwDouble extends RwPrimitive<Double,RwDouble> {
        public double val;
        protected void read(Page buf,int offset) { val = buf.getDouble( offset ); }
        protected void write(Page buf,int offset) { buf.putDouble( offset, val ); }
        public Double val() { return val; }
        protected void set2(Double value) { val = value; }
        public int size() { return org.srlutils.Types.Enum._double.size; }
    }
    public static class RwFloat extends RwPrimitive<Float,RwFloat> {
        public float val;
        protected void read(Page buf,int offset) { val = buf.getFloat( offset ); }
        protected void write(Page buf,int offset) { buf.putFloat( offset, val ); }
        public Float val() { return val; }
        protected void set2(Float value) { val = value; }
        public int size() { return org.srlutils.Types.Enum._float.size; }
    }
    public static abstract class RwObj<TT,SS extends RwObj> extends RwPrimitive<TT,SS> {
        public TT val;
        public TT val() { return val; }
        protected void set2(TT value) { val = value; }
        protected void clean() { val = null; }
    }

    /** 
     * a command backed by an array
     * that transfers a range of values between that array and the buffer
     * @param <TT> the array type
     * @param <SS> the self type (for chaining)
     */
    public static abstract class RwArray<TT,SS extends RwArray> extends RwObj<TT,SS> {
        /** the start of the range to transfer */  protected int index;
        /** the number of elements to transfer */  protected int num;

        /**
         * set the offset into the backing array k1 and the number of elements nn, return this
         * @param index
         * @param num
         * @return 
         */
        public SS range(int index,int num) { this.index = index; this.num = num; return (SS) this; }
        protected void read(Page buf,int offset) {
            prep();
            int pos = size(index), len = size(num);
            buf.copy(offset,val,pos,len);
        }
        protected void write(Page buf,int offset) {
            int pos = size(index), len = size(num);
            buf.copyFrom(offset,val,pos,len);
        }
        public String toString() {
            String txt = getClass().getSimpleName() + "::" + offset + "::" + num;
            return txt;
        }
        protected void prep() {}
        protected boolean changed(Page buf,int offset) { return true; }
        protected boolean unchangeable() { return true; }
        public int size() { return size(num); }
        protected abstract int size(int num);
    }

    public static class RwBytes extends RwArray<byte [],RwBytes> {
        protected void prep() { if (val==null) val = new byte[num]; }
        protected int size(int num) { return num * org.srlutils.Types.Enum._byte.size; }
    }
    public static class RwInts extends RwArray<int [],RwInts> {
        protected void prep() { if (val==null) val = new int[num]; }
        protected int size(int num) { return num * org.srlutils.Types.Enum._int.size; }
    }
    public static class RwLongs extends RwArray<long [],RwLongs> {
        protected void prep() { if (val==null) val = new long[num]; }
        protected int size(int num) { return num * org.srlutils.Types.Enum._long.size; }
    }

    
    
    protected static class Reference extends Rw<Reference> {
        public byte [] data;
        public boolean dup;
        public int size() {
            throw new UnsupportedOperationException("Not supported yet.");
        }
        protected void read(Page buf,int offset) {
            data = buf.data;
            dup = buf.dup;
        }
        protected void write(Page buf,int offset) {
            // fixme::perf -- only copy if needed
            buf.data = Util.dup(data);
        }
    }

}
