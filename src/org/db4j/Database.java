// copyright 2017 nqzero - see License.txt for terms

package org.db4j;

import java.lang.reflect.Field;
import kilim.Pausable;
import org.db4j.Db4j.Hunkable;
import org.db4j.Db4j;
import org.srlutils.DynArray;
import org.srlutils.Simple;

/** a generic database that takes it's configuration from it's variables */
public class Database {
    public Db4j db4j;
    public Table [] tables;
    public Database init(Db4j $db4j) { db4j = $db4j; return this; }
    public static class Self extends Table {}
    public Self self = new Self();

    public static String base(Field field) {
        return field.getDeclaringClass().getSimpleName() + "/";
    }

    public <TT> Db4j.LambdaQuery<TT> future(Db4j.Queryable<TT> body) throws Pausable {
        return db4j.submit(body);
    }

    
    
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
            table.init(base(field), db4j );
            table.build(table,overwrite);
            Simple.Reflect.set( this, field.getName(), table );
        }
        self.init(null,db4j);
        self.build(this,overwrite);
    }
    public synchronized void shutdown(boolean orig) {
        if (orig && shutdownThread != null)
            Runtime.getRuntime().removeShutdownHook(shutdownThread);
        shutdownThread = null;
        db4j.shutdown();
        db4j.close();
    }
    Thread shutdownThread;
    public Database start(String base,boolean init) {
        db4j = init ? new Db4j() : Db4j.load( base );
        db4j.userClassLoader = this.getClass().getClassLoader();
        if (init) {
            db4j.init( base, -(2L<<30) );
            build(true);
            db4j.create();
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
            table.init(base(field), db4j );
            table.load(table);
            tables[ ktable++ ] = table;
            Simple.Reflect.set( this, field.getName(), table );
        }
        self.init(null,db4j);
        self.load(this);
    }
    /** close the resourses associated with the database */
    public void close() {
        for (Table table : tables) table.close();
    }
    public abstract class Task<TT extends Task> extends Db4j.Query<TT> {
        // fixme -- report bug in kilim. if task() is added here as abstract, weave fails
        public TT offer() { return (TT) db4j.submitQuery(this); }
    }

    
    
    
    /** a colection of columns */
    public static class Table {
        String root;
        public Db4j db4j;
        public Hunkable [] columns;
        public Table init(String _root,Db4j db4j) {
            this.db4j = db4j;
            root = _root;
            return this;
        }
        public void build(Object source,boolean overwrite) {
            Field [] fields = Database.getFields(source,Hunkable.class);
            columns = new Hunkable[ fields.length ];
            int ktable = 0;
            for (Field field : fields) {
                String name = filename(field);
                Hunkable composite = db4j.lookup( name );
                if (overwrite || composite == null) {
                    composite = (Hunkable) Simple.Reflect.alloc( field.getType() );
                    composite.set(db4j );
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
                composite = (Hunkable) db4j.lookup( filename );
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











