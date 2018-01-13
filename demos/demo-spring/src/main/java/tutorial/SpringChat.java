package tutorial;

import java.math.BigInteger;
import java.util.Random;
import org.db4j.Btrees;
import org.db4j.Database;
import org.db4j.Db4j;
import org.db4j.HunkCount;
import static org.db4j.perf.DemoHunker.resolve;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.context.request.async.DeferredResult;
    
@RestController
@ResponseBody
@SpringBootApplication
public class SpringChat extends Database {

    // schema
    HunkCount count;
    Btrees.IK<User> users;
    Btrees.SI namemap;
    Btrees.IS messages;


    public static class User {
        public String name, bio;
    }    


    // endpoints
    
    @RequestMapping("/dir")
    public Object dir() {
        return defer(txn ->
                users.getall(txn).vals());
    }
    @RequestMapping("/get/{id}")
    public Object getUser(@PathVariable int id) {
        return defer(txn ->
                users.context().set(txn).set(id,null).get(users).val);
    }
    @RequestMapping("/list/{id}")
    public Object list(@PathVariable int id) {
        return defer(txn ->
                messages.findPrefix(txn,id).vals());
    }
    @RequestMapping("/msg/{id}/{msg}")
    public Object msg(@PathVariable int id,@PathVariable String msg) {
        return defer(txn ->
                messages.insert(txn,id,msg).val);
    }
    @RequestMapping("/user/{name}")
    public Object user(@PathVariable String name) {
        return defer(txn ->
                namemap.find(txn,name));
    }
    @RequestMapping("/new/{name}/{bio}")
    public Object newUser(@PathVariable String name,@PathVariable String bio) {
        return defer(txn -> {
            User user = new User();
            user.name = name;
            user.bio = bio;
            if (namemap.find(txn,user.name) != null) return "username is not available";
            int krow = count.plus(txn,1);
            users.insert(txn,krow,user);
            namemap.insert(txn,user.name,krow);
            return krow;
        });
    }


    
    
    // random endpoints

    @RequestMapping("/random/msg")
    public Object randomMsg() {
        return defer(txn -> {
            int num = count.get(txn), rid = random.nextInt(num);
            if (num > 0)
                messages.insert(txn,rid,secret());
            return "random insert";
        });
    }
    @RequestMapping("/random/user")
    public Object randomUser() {
        String name = "bot-"+random.nextInt(Integer.MAX_VALUE);
        return newUser(name,secret());
    }
    @RequestMapping("/random/both")
    public Object randomBulk() {
        for (int ii=0; ii < 10; ii++) randomUser();
        for (int ii=0; ii < 30; ii++) randomMsg();
        return "random bulk insert started";
    }
    @RequestMapping("/random/user/{num}")
    public Object bulkUsers(@PathVariable int num) {
        for (int ii=0; ii < num; ii++) randomUser();
        return "random bulk user insert started";
    }
    @RequestMapping("/random/msg/{num}")
    public Object bulkMsg(@PathVariable int num) {
        for (int ii=0; ii < num; ii++) randomMsg();
        return "random bulk message insert started";
    }

    
    // utility methods and fields

    <TT> DeferredResult<ResponseEntity<TT>> defer(Db4j.Utils.QueryFunction<TT> body) {
        DeferredResult<ResponseEntity<TT>> result = new DeferredResult<>();
        db4j.submit(txn ->
            result.setResult(ResponseEntity.ok(body.query(txn)))
        );
        return result;
    }
    
    Random random = new Random();
    String secret() {
        String val = "";
        while (val.length() != 26)
            val = new BigInteger(134,random).toString(36);
        return val;
    }

    public SpringChat() {
        this(false);
    }
    public SpringChat(boolean build) {
        start(resolve("./db_files/chat.mmap"),build);
    }
    
    public static void main(final String[] args) throws Exception {
        SpringApplication.run(SpringChat.class, args);
    }
}
