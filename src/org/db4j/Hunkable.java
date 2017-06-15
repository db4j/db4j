package org.db4j;

import java.io.Serializable;
import kilim.Pausable;
import org.srlutils.Types;

public abstract class Hunkable<TT extends Hunkable> implements Serializable {
    long kloc;
    protected abstract String info();
    protected abstract int create();
    protected abstract void createCommit(long locBase);
    protected abstract String name();
    protected abstract TT set(Db4j db4j,String name);
    protected abstract void postInit(Db4j.Transaction txn) throws Pausable;
    protected abstract void postLoad(Db4j.Transaction txn) throws Pausable;

    /** a convenience class for handling offsets for the local allocation variables */
    public static class Locals {
        public Db4j db4j;
        public int size;
        public long base;
        public Locals() { size = 0; }
        public Locals set(Db4j db4j,long base) {
            this.db4j = db4j;
            this.base = base;
            return this;
        }
        public int size() { return size; }
    }

    // fixme - use abstract base, break out cmd(), and allow Locals to read all values

    /** an integer that occupies a spot in the local allocation */
    public static class LocalInt {
        /** the byte position within the Locals tuple */
        protected final int position;
        /** the Locals tuple that contains this value */
        protected Locals locals;
        public LocalInt(Locals locals) {
            position = locals.size;
            locals.size += size();
            this.locals = locals;
        }
        protected int size() { return Types.Enum._int.size(); }
        protected long offset() { return locals.base + position; }
        /**
         * add a command to the transaction that will write a value to the stored location on disk
         * @param txn the transaction
         * @param val the new value
         * @return 
         */
        public Command.RwInt write(Db4j.Transaction txn,int val) {
            return write(val).add(locals.db4j,txn);
        }
        /** return a command that will write val to disk */
        protected Command.RwInt write(int val) {
            Command.RwInt cmd = new Command.RwInt().init( true);
            cmd.msg = "LocalInt::write";
            cmd.offset = offset();
            cmd.val = val;
            return cmd;
        }
        /**
         * add a command to the transaction that will read the value from the stored location on disk
         * @param txn the transaction to add the command to
         * @return the command
         */
        public Command.RwInt read(Db4j.Transaction txn) {
            return read().add(locals.db4j,txn);
        }
        /** return a command that will read the value from the stored location on disk */
        protected Command.RwInt read() {
            Command.RwInt cmd = new Command.RwInt().init( false);
            cmd.msg = "LocalInt::read";
            cmd.offset = offset();
            return cmd;
        }
    }



}
/*
trying to find a name for tables, columns, and indexes
oracle seems to call them schema objects
https://www.reddit.com/r/Database/comments/6gpf6c/what_is_the_name_for_the_structures_stored_in_a/
https://en.wikipedia.org/wiki/Database_schema#Oracle_database_specificity

xodus has something called a store which may be similar
https://github.com/JetBrains/xodus/blob/master/openAPI/src/main/java/jetbrains/exodus/env/Store.java

H2 database has MVStore which appears to be a store that's exposed to the user
http://www.h2database.com/html/mvstore.html

https://www.google.com/search?safe=off&q=java+embedded+database+mapdb+h2&oq=java+embedded+database+mapdb+h2




*/