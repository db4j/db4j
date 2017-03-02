// copyright 2017 nqzero - see License.txt for terms

package org.db4j;

import java.lang.reflect.Field;
import kilim.Pausable;
import org.db4j.Db4j.Hunkable;
import org.db4j.Db4j.Hunker;
import org.srlutils.DynArray;
import org.srlutils.Simple;

/** a generic database that takes it's configuration from it's variables */
public class Database {
    public Hunker hunker;
    public Table [] tables;
    public Database init(Hunker $hunker) { hunker = $hunker; return this; }
    public static class Self extends Table {}
    public Self self = new Self();

    public static String base(Field field) {
        return field.getDeclaringClass().getSimpleName() + "/";
    }

    public <TT> TT offer(Hunker.InvokeAble<TT> body) throws Pausable { return hunker.offer( body ); }

    
    
    /** get all the fields of obj assignable from filter using reflection */
    public static Field[] getFields(Object obj,Class filter) {
        DynArray.Objects<Field> result = new DynArray.Objects().init(Field.class);
        for (Class klass = obj.getClass(); klass != Object.class; klass = klass.getSuperclass()) {
            Field[] fields = klass.getDeclaredFields();
            for (Field field : fields) {
                Class fc = field.getType();
                if (filter.isAssignableFrom( fc )) result.add( field );
            }
        }
        return result.trim();
    }
    /** build all the tables in the database, overwrite (existing Loadables) if set */
    public void build(boolean overwrite) {
        Field [] fields = getFields(this,Table.class);
        tables = new Table[ fields.length ];
        int ktable = 0;
        for (Field field : fields) {
            Table table = (Table) Simple.Reflect.alloc(  field.getType() );
            tables[ ktable++ ] = table;
            table.init( base(field), hunker );
            table.build(table,overwrite);
            Simple.Reflect.set( this, field.getName(), table );
        }
        self.init(null,hunker);
        self.build(this,overwrite);
    }
    public synchronized void shutdown(boolean orig) {
        if (orig && shutdownThread != null)
            Runtime.getRuntime().removeShutdownHook(shutdownThread);
        shutdownThread = null;
        hunker.shutdown();
        hunker.close();
    }
    Thread shutdownThread;
    public Database start(String base,boolean init) {
        hunker = init ? new Hunker() : Hunker.load( base );
        hunker.userClassLoader = this.getClass().getClassLoader();
        if (init) {
            hunker.init( base, -(2L<<30) );
            build(true);
            hunker.create();
        }
        else load();
        shutdownThread = new Thread(new Runnable() {
            public void run() {
                shutdown(false);
            }
        });
        Runtime.getRuntime().addShutdownHook(shutdownThread);
        return this;
    }
    /** load all the tables in the database */
    public void load() {
        Field [] fields = getFields(this,Table.class);
        tables = new Table[ fields.length ];
        int ktable = 0;
        for (Field field : fields) {
            Table table = (Table) Simple.Reflect.alloc(  field.getType() );
            table.init( base(field), hunker );
            table.load(table);
            tables[ ktable++ ] = table;
            Simple.Reflect.set( this, field.getName(), table );
        }
        self.init(null,hunker);
        self.load(this);
    }
    /** close the resourses associated with the database */
    public void close() {
        for (Table table : tables) table.close();
    }
    public abstract class Task<TT extends Task> extends Db4j.Tasky<TT> {
        // fixme -- report bug in kilim. if task() is added here as abstract, weave fails
        public TT offer() { return (TT) hunker.offerTask(this); }
    }

    
    
    
    /** a colection of columns */
    public static class Table {
        String root;
        public Hunker hunker;
        public Hunkable [] columns;
        public Table init(String _root,Hunker hunker) {
            this.hunker = hunker;
            root = _root;
            return this;
        }
        public void build(Object source,boolean overwrite) {
            Field [] fields = Database.getFields(source,Hunkable.class);
            columns = new Hunkable[ fields.length ];
            int ktable = 0;
            for (Field field : fields) {
                String name = filename(field);
                Hunkable composite = hunker.lookup( name );
                if (overwrite || composite == null) {
                    composite = (Hunkable) Simple.Reflect.alloc( field.getType() );
                    composite.set( hunker );
                    composite.init( name );
                }
                columns[ ktable++ ] = composite;
                Simple.Reflect.set(source, field.getName(), composite);
            }
        }

        public void load(Object source) {
            Field [] fields = Database.getFields(source,Hunkable.class);
            columns = new Hunkable[ fields.length ];
            int kcol = 0;
            for (Field field : fields) {
                Hunkable composite = null;
                String filename = filename(field);
                composite = (Hunkable) hunker.lookup( filename );
                if (composite == null) {
                    String err = String.format( "failed to find Hunkable: %s, as field: %s",
                            filename, field.getName() );
                    throw new RuntimeException( err );
                }
                columns[ kcol++ ] = composite;
                Simple.Reflect.set(source, field.getName(), composite);
            }
        }
        public void close() {
            for (Hunkable column : columns) {}
        }
        public String filename(Field field) {
            String base = root==null ? base(field) : root;
            String table = "/" + this.getClass().getSimpleName() + "/";
            return base + table + field.getName();
        }
    }




}











