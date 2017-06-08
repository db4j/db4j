// copyright 2017 nqzero - see License.txt for terms

package org.db4j.perf;

import kilim.Pausable;
import java.io.Serializable;
import java.util.stream.Collectors;
import org.db4j.Btrees;
import org.db4j.Database;
import org.db4j.HunkCount;
import org.apache.commons.lang3.RandomStringUtils;
import org.db4j.Db4j;
import org.db4j.Db4j.Query;
import org.srlutils.Rand;

public class Chat1 extends Database {
    HunkCount count;
    Btrees.IO<User> users;
    Btrees.SI namemap;
    Btrees.IS messages;
    public static class User implements Serializable {
        public String name, bio;
        public User(String name,String bio) { this.name=name; this.bio=bio; }
        String format() { return "user: " + name + " <" + bio + ">"; }
    }    
    public String route(String query) throws Pausable {
        String cmds[]=query.split("/"), cmd=cmds.length > 1 ? cmds[1]:"none";
        Integer id = parse(cmds,2);
        return db4j.submit(txn -> { switch (cmd) {
            case "dir" : return users.getall(txn).vals().stream().map(User::format).collect(Collectors.joining("\n"));
            case "get" : return users.context().set(txn).set(id,null).get(users).val.format();
            case "list": return messages.findPrefix(txn,id).vals().stream().collect(Collectors.joining("\n"));
            case "msg" : return "sent: " + messages.insert(txn,id,cmds[3]).val;
            case "user": return "" + namemap.find(txn,cmds[2]);
            case "new" : 
                    User user = new User(cmds[2],cmds[3]);
                    int krow = count.plus(txn,1);
                    users.insert(txn,krow,user);
                    namemap.insert(txn,user.name,krow);
                    return "" + krow;
            case "random":
                int num = count.get(txn), rid = Rand.source.nextInt(0,num);
                if (num > 0)
                    messages.insert(txn,rid,RandomStringUtils.randomAscii(33));
                return "random insert";
            default: return "";
        }
        }).await().val;
    }
    
    public static void main(String[] args) {
        String filename = DemoHunker.resolve("./db_files/hunk2.mmap");
        String PATH_K2 = "///chat1/k2";
        int key = 707;
        Btrees.IS k2 = new Btrees.IS();
        
        if (args.length==0) {
            Chat1 hello = new Chat1();
            Db4j db4j = hello.start(filename,true);
            new kilim.Task() {
                public void execute() throws Pausable {
                    hello.route("/new/hello/world");
                    hello.route("/new/static/main");
                    hello.route("/new/thomas/paine");
                    System.out.println(hello.route("/get/2"));
                }
            }.start().joinb();
            db4j.submitCall(txn -> { db4j.create(txn,k2,PATH_K2); }).awaitb();
            db4j.submitCall(txn -> { k2.insert(txn,key,"hello world"); }).awaitb();
            hello.shutdown(true);
        }
        
        {
            Chat1 hello = new Chat1();
            Db4j db4j = hello.start(filename,false);
            // can lookup by either class or value, it's just used for type inference
            String klass = db4j.submit(txn ->
                    txn.lookup(Btrees.IS.class,PATH_K2).find(txn,key)
            ).awaitb().val;
            String klass3 = db4j.submit(txn -> 
                    txn.lookup(k2,PATH_K2).find(txn,key)
            ).awaitb().val;
            Db4j.Connection conn = db4j.connect();
            Db4j.Utils.LambdaQuery query;
            for (int ii = 0; ii < 4; ii++) {
                query = conn.submit(txn -> txn.lookup(Btrees.IS.class,PATH_K2).find(txn,key));
                if (ii<2)          query.awaitb();
                if (ii==0 | ii==2)  conn.awaitb();
                if (ii==1 | ii==2) query.awaitb();
                if (ii >= 2)        conn.awaitb();
                System.out.println(ii + " -- " + query.val);
            }
            
            System.out.println(klass);
            System.out.println(klass3);
        }
        System.exit(0);
    }
    public static void main2(String[] args) throws Exception {
        Chat1 chat = new Chat1();
        chat.start(DemoHunker.resolve("../db_files/hunk2.mmap"),args.length > 0);
        new kilim.http.HttpServer(8080, req -> chat.route(req.uriPath));
        System.in.read();
        System.exit(0);
    }
    static Integer parse(String vals[],int index) {
        try { return Integer.parseInt(vals[index]); } catch(Exception ex) { return 0; }
    }
}
