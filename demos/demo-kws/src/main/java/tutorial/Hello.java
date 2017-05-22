// copyright 2017 nqzero - see License.txt for terms

package tutorial;

import kilim.Pausable;
import static org.db4j.perf.DemoHunker.resolve;

public class Hello extends Chat {
    
    public static void main(String[] args) throws Exception {
        Hello hello = new Hello();
        hello.start(resolve("./db_files/chat.mmap"),args.length==0);
        
        new kilim.Task() { public void execute() throws Pausable {
            hello.route("/new/hello/world");
            hello.route("/new/static/main");
            hello.route("/new/thomas/paine");
            System.out.println(hello.route("/get/2"));
        }}.start().joinb();
        System.exit(0);
    }
}
