// copyright 2017 nqzero - see License.txt for terms

package org.db4j;

import java.util.Iterator;
import kilim.Pausable;
import org.db4j.Db4j.CachedBlock;
import org.db4j.Db4j;
import org.srlutils.Simple;
import org.srlutils.Util;

public abstract class Command implements Cloneable, Iterable<Command> {
    
    public static class Page {
        private static final sun.misc.Unsafe uu = Simple.Reflect.getUnsafe();
        static final long bao = uu.arrayBaseOffset(byte[].class);
        public byte [] data;
        public CachedBlock cb;
        public boolean dup;
        
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
    public Iter iterator() { return new Iter(); }
    /** remove is a nop */
    public class Iter implements Iterator<Command> {
        public Command curr, next = Command.this;
        public boolean hasNext() { return next != null; }
        public Command next() { curr = next; next = next.next; return curr; }
        public void remove() { throw new UnsupportedOperationException(); }
    }
    
    public static void print(Db4j hunker,boolean iterate,Iterable<Command> cmds) {
        for (Command cmd : cmds) {
            long kp = cmd.offset >> hunker.bb;
            long ko = cmd.offset & hunker.bm;
            String info = cmd.info();
            char status = cmd.write() ? 'w':'r';
            if (cmd.done()) status = Character.toUpperCase(status);
            System.out.format( "%8d.%4d%c %s\n", kp, ko, status, info );
            if (!iterate) return;
        }
    }
    public String info() {
        return getClass().getSimpleName();
    }
    
    /** the txn that owns this command, may be null for immediate writes eg HunkLog.set */
    public Db4j.Transaction txn;
    /** link to the next cmd, used in Transaction.reads and Transaction.cmds */
    public Command next;
    public String msg;
    public long offset;
    /** 
     *  txn count at the time the read is executed
     *  @see Db4j.QueRunner#genc
     */
    public long gen = Db4j.QueRunner.gencUsageMarker(0);


    /** is the command a write ??? */
    public abstract boolean write();
    public abstract int size();
    public abstract boolean done();
    public abstract void read(Page buf,int offset);
    public abstract void write(Page buf,int offset);

    public void run(int offset,Page buf,Db4j hunker,boolean defer) {
        boolean write = write();
        // fixme::kludge -- this works with the current set of commands (Nop isn't a write, Init overrides)
        if (write && buf.data==null) return;
        if (write) write( buf, offset );
        else        read( buf, offset );
    }
    public void handle(Db4j.QueRunner qr) { qr.handleWrite(this); }

    public void book(Db4j hunker) {}
    public void clean() {}

    /** return a clone of the command */
    public Command dup() {
        try { return (Command) this.clone(); }
        catch (CloneNotSupportedException ex) { throw new RuntimeException( ex ); }
    }
    /** test whether the data in v1 is different than the data in v2 at offset */
    public boolean diff(int offset,byte [] v1,byte [] v2) {
        for (int ii = offset; ii < offset + size(); ii++) if (v1[ii]!=v2[ii]) return true;
        return false;
    }
    public boolean changed(Page buf,int offset) { return true; }
    /** if the underlying cache has changed, does that automatically imply that the cmd has changed */
    public boolean unchangeable() { return true; }

    /** does this Command contain cmd */
    public boolean contains(Command cmd) {
        return (cmd.offset >= offset && cmd.offset + cmd.size() <= offset + size());
    }

    public static class Nop extends Command {
        boolean done, write = false;
        public void book(Db4j hunker) { done = true; }
        public boolean write() { return write; }
        public int size() { return 0; }
        public boolean done() { return done; }
        public void read(Page buf,int offset) {}
        public void write(Page buf,int offset) {}
    }

    public static class Init extends Nop {
        public byte [] data;
        { write = true; }
        public Init() {}
        public void run(int offset,Page buf,Db4j hunker,boolean defer) {
            if (true) { buf.data = new byte[hunker.bs]; return; }
            if (data==null) data = new byte[hunker.bs];
            buf.data = data;
        }
        public void handle(Db4j.QueRunner qr) { qr.handleInit(this); }
    }
    
    /** read/write command. SS: the type of the command */
    public static abstract class Rw<SS extends Rw> extends Command {
        public byte status;
        // notify: notify the object's monitor once the command has run
        public static byte done = 1, write = 1<<1;

        public SS init(boolean dowrite) {
            if (dowrite) status |= write;
            return (SS) this;
        }
        public SS dup() { return (SS) super.dup(); }

        public boolean done() { return (status & done) != 0; }
        public boolean write() { return (status & write) != 0; }

        public void book(Db4j hunker) {
            status |= done;
        }
        public SS yield() throws Pausable {
            txn.submitYield();
            return (SS) this;
        }
        public SS add(Db4j hunker,Db4j.Transaction txn) { hunker.put(txn,this); return (SS) this; }
    }


    /**
     * an abstract command that represents a backing value
     * @param <TT> the (possibly boxed) type of the backing value
     * @param <SS> the type of the command (to allow pretty return values and This-chaining)
     */
    public static abstract class RwPrimitive<TT,SS extends RwPrimitive> extends Rw<SS> {
        /** return the backing value           */  public abstract TT val();
        /** set the backing value              */  public abstract void set2(TT val);
        /** set the backing value, return this */  public SS set(TT value) { set2( value ); return (SS) this; }
        public String toString() { return (write() ? "w" : "r") + val(); }
        public boolean changed(Page buf,int offset) {
            TT vo = val();
            read(buf,offset);
            TT v1 = val();
            return ! vo.equals( v1 );
        }
        public boolean unchangeable() { return false; }
        public String info() {
            return getClass().getSimpleName() + " val:" + val();
        }
    }

    public static class RwByte extends RwPrimitive<Byte,RwByte> {
        public RwByte() {}
        public byte val;
        public void read(Page buf,int offset) { val = buf.getByte( offset ); }
        public void write(Page buf,int offset) { buf.putByte( offset, val ); }
        public Byte val() { return val; }
        public void set2(Byte value) { val = value; }
        public int size() { return org.srlutils.Types.Enum._int.size; }
    }
    public static class RwInt extends RwPrimitive<Integer,RwInt> {
        public RwInt() {}
        public int val;
        public void read(Page buf,int offset) { val = buf.getInt( offset ); }
        public void write(Page buf,int offset) { buf.putInt( offset, val ); }
        public Integer val() { return val; }
        public void set2(Integer value) { val = value; }
        public int size() { return org.srlutils.Types.Enum._int.size; }
    }
    public static class RwLong extends RwPrimitive<Long,RwLong> {
        public RwLong() {}
        public long val;
        public void read(Page buf,int offset) { val = buf.getLong( offset ); }
        public void write(Page buf,int offset) { buf.putLong( offset, val ); }
        public Long val() { return val; }
        public void set2(Long value) { val = value; }
        public int size() { return org.srlutils.Types.Enum._long.size; }
    }
    public static class RwDouble extends RwPrimitive<Double,RwDouble> {
        public double val;
        public void read(Page buf,int offset) { val = buf.getDouble( offset ); }
        public void write(Page buf,int offset) { buf.putDouble( offset, val ); }
        public Double val() { return val; }
        public void set2(Double value) { val = value; }
        public int size() { return org.srlutils.Types.Enum._double.size; }
    }
    public static class RwFloat extends RwPrimitive<Float,RwFloat> {
        public float val;
        public void read(Page buf,int offset) { val = buf.getFloat( offset ); }
        public void write(Page buf,int offset) { buf.putFloat( offset, val ); }
        public Float val() { return val; }
        public void set2(Float value) { val = value; }
        public int size() { return org.srlutils.Types.Enum._float.size; }
    }
    public static abstract class RwObj<TT,SS extends RwObj> extends RwPrimitive<TT,SS> {
        public TT val;
        public TT val() { return val; }
        public void set2(TT value) { val = value; }
        public void clean() { val = null; }
    }

    /** an array-like command - read/write over a range val[k1:k1+n) */
    public static abstract class RwArray<TT,SS extends RwArray> extends RwObj<TT,SS> {
        /** the offset into the backing array */  public int k1;
        /** the number of elements            */  public int nn;

        /** set the offset into the backing array k1 and the number of elements nn, return this */
        public SS range(int k1,int nn) { this.k1 = k1; this.nn = nn; return (SS) this; }
        public void set2(TT value) { val = value; }
        public void read(Page buf,int offset) {
            prep();
            int pos = size(k1), len = size(nn);
            buf.copy(offset,val,pos,len);
        }
        public void write(Page buf,int offset) {
            int pos = size(k1), len = size(nn);
            buf.copyFrom(offset,val,pos,len);
        }
        public String toString() {
            String txt = getClass().getSimpleName() + "::" + offset + "::" + nn;
            return txt;
        }
        public void prep() {}
        public boolean changed(Page buf,int offset) { return true; }
        public boolean unchangeable() { return true; }
        public int size() { return size(nn); }
        public abstract int size(int num);
    }

    public static class RwBytes extends RwArray<byte [],RwBytes> {
        public void prep() { if (val==null) val = new byte[nn]; }
        public int size(int num) { return num * org.srlutils.Types.Enum._byte.size; }
    }
    public static class RwInts extends RwArray<int [],RwInts> {
        public void prep() { if (val==null) val = new int[nn]; }
        public int size(int num) { return num * org.srlutils.Types.Enum._int.size; }
    }
    public static class RwLongs extends RwArray<long [],RwLongs> {
        public void prep() { if (val==null) val = new long[nn]; }
        public int size(int num) { return num * org.srlutils.Types.Enum._long.size; }
    }

    
    
    public static class Reference extends Rw<Reference> {
        public byte [] data;
        public boolean dup;
        public int size() {
            throw new UnsupportedOperationException("Not supported yet.");
        }
        public void read(Page buf,int offset) {
            data = buf.data;
            dup = buf.dup;
        }
        public void write(Page buf,int offset) {
            // fixme::perf -- only copy if needed
            buf.data = Util.dup(data);
        }
        public void handle(Db4j.QueRunner qr) { qr.handleRef(this); }
    }
    public static class Demo {
        public static void main(String [] args) {
            new org.db4j.Bmeta.Demo().demo();
        }
    }
}
