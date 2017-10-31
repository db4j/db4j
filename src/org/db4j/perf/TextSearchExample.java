// copyright 2017 nqzero - licensed under the terms of the MIT license

package org.db4j.perf;

import org.db4j.TextSearchTable;
import org.db4j.TextSearchTable.Ibox;
import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import kilim.Scheduler;
import org.db4j.Btrees;
import org.db4j.Database;
import org.db4j.Db4j;
import org.db4j.TextSearchTable.Prep;

public class TextSearchExample {
    ArrayList<String> docs = new ArrayList();

    static class Store extends Database {
        Btrees.IS posts;
        TextSearchTable inverted;

        void query(String query,int max) {
            ArrayList<String> docs = new ArrayList();
            Integer num = db4j.submit(txn -> {
                ArrayList<Integer> kdocs = inverted.search(txn,query);
                for (int ii=0; ii < Math.min(max,kdocs.size()); ii++)
                    docs.add(posts.find(txn,kdocs.get(ii)));
                return kdocs.size();
            }).awaitb().val;
            System.out.format("%40s --> %4d%s",query,num,docs.isEmpty() ? "\n":":");
            for (String doc : docs)
                System.out.println("\t"+doc);
        }
        void add(Db4j.Connection conn,String doc,int kdoc) {
            Prep prep = inverted.prep(doc);
            conn.submitCall(txn -> {
                posts.insert(txn,kdoc,doc);
                prep.add(txn,kdoc);
            });
        }
        
    }
    
    
    
    public static void main(String[] args) throws Exception {
        TextSearchExample example = new TextSearchExample();
        String postsName = args.length==0 ? "./demos/demo-text/doc/Posts.txt":args[0];
        String posts = DemoHunker.resolve(postsName);
        String dbname = args.length >= 2 ? args[1]:"./db_files/hunk2.mmap";
        String dbfile = DemoHunker.resolve(dbname);
        example.read(posts);
        example.run(dbfile);
        Scheduler.getDefaultScheduler().idledown();
    }
    public TextSearchExample read(String filename) {
        try {
            BufferedReader br = new BufferedReader(new FileReader(filename));
            for (String line; (line = br.readLine()) != null; )
                docs.add(line);
        }
        catch (Exception ex) { throw new RuntimeException(ex); }
        return this;
    }

    // verified that the total matches the result from cruden's BaseInverted
    AtomicInteger total = new AtomicInteger();
    void run(String filename) {
        Store store = new Store();
        boolean build = false;
        Db4j db4j = store.start(filename,build);
        Db4j.Connection conn = db4j.connect();
        Ibox kdoc = new Ibox();
        if (build) {
            for (String doc : docs)
                store.add(conn,doc,kdoc.val++);
            conn.awaitb();
        }

        store.query("world taste",1);
        store.query("hops",1);
        store.query("co2 hops",1);
        store.query("darker warmer lighter",1);
        store.query("dark warm light",1);
        for (String doc : docs)
            for (String word : doc.split(" "))
                conn.submit(txn -> total.addAndGet(store.inverted.search(txn,word,true).size()));
        conn.awaitb();
        System.out.println(total);
        store.shutdown(true);
    }
    
    

    
}
