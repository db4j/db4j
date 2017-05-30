package org.db4j;

import java.io.Serializable;
import org.db4j.Db4j.Query;

public class ConnectionBase implements Serializable {
    private Db4j proxy;
    private boolean track;
    
    
    /**
     * create a new proxy
     * @param proxy the db4j instance
     * @param track enable tracking
     */
    ConnectionBase connectionSetProxy(Db4j proxy,boolean track) {
        this.track = track;
        this.proxy = proxy==null ? (Db4j) this:proxy;
        return this;
    }
    
    /**
     * create a new query that delegates to body, capturing the return value, and submit it to the execution engine
     * @param <TT> the return type of body
     * @param body a lambda or equivalent that is called during the query task execution, with a return value
     * @return the new query
     */
    public <TT> Db4j.Utils.LambdaQuery<TT> submit(Db4j.Utils.QueryFunction<TT> body) {
        Db4j.Utils.LambdaQuery<TT> invoke = new Db4j.Utils.LambdaQuery(body);
        return submitQuery(invoke);
    }
    /**
     * create a new query that delegates to return-value-less body, and submit it to the execution engine
     * @param body a lambda or equivalent that is called during the query task execution, without a return value
     * @return the new query
     */
    public Db4j.Utils.LambdaCallQuery submitCall(Db4j.Utils.QueryCallable body) {
        Db4j.Utils.LambdaCallQuery implore = new Db4j.Utils.LambdaCallQuery(body);
        return submitQuery(implore);
    }
    /**
     * submit query to the dbms execution engine
     * @param <TT> the type of the query, which is used for the return value to allow chaining
     * @param query the query to execute
     * @return query, preserving the type to allow chaining
     */
    public <TT extends Db4j.Query> TT submitQuery(TT query) {
        if (track) connectionAddQuery(query);
        return proxy.guts.offerTask(query);
    }

    /**
     * user function.
     * called when a query is submitted to the processing engine if tracking is enabled.
     * default implementation is a no-op
     * @param query the query
     */    
    protected void connectionAddQuery(Query query) {}
    
    
}
