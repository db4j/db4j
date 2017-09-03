// copyright 2017 nqzero - see License.txt for terms

package org.db4j;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.function.Consumer;
import java.util.function.Function;
import kilim.Fiber;
import org.srlutils.Rand;
import org.srlutils.Simple;
import org.srlutils.Types;
import org.srlutils.btree.Butil.Modes;
import org.srlutils.btree.Bpage.Page;
import org.srlutils.btree.Bpage.Sheet;
import org.srlutils.DynArray;
import org.srlutils.TaskTimer;
import org.srlutils.btree.Butil;

// can run either kilimized or kilim-free (comment out the following import)
import kilim.Pausable;
import org.db4j.Db4j.Transaction;

/*
 * 
 * 
 * the Bplus legacy
 * 
 * there have been many Btree variants
 * Bplus was based on them but stripped off most of the abstraction
 *   - direct access to indexes
 *   - no abstract key, val (BtTypes) ie subclasses access the pages directly
 *   - "completed" the btree impl, ie remove, and some faster variants - push, pop, removeHinted
 *   - elements are stored in arrays
 *   - added some experimental page layouts, eg partially-sorted (ceil and iterable never impld)
 *   
 * 
 * Bminus - Bplus + dexs are abstract, without the order experiments + experimental backings, eg direct, pourous
 * Bparr - Bplus, array backed, without order experiments
 * Bmarr - Bminus, array backed - difers from Bparr in dexs only (which seem to make a performance difference)
 * Btree - Bminus, direct memory backed
 * b6.Btree - getPage is Pausable (either fake Pausable or kilim.Pausable)
 */


/**
 * based on Bminus, backed by direct memory
 * 
 * an in-memory btree backed by direct memory
 *   no intra-level page links are maintained, ie only an array of children
 * CC: compare context type
 * PP: page type
 */
public abstract class Btree<CC extends Btree.Context,PP extends Page<PP>>
    extends Hunkable<Btree> implements Serializable
{
    public static final Modes modes = new Modes();
    public int bb = 12;
    public int bs = 1 << bb;
    /** number of elements in a branch and leaf */
    int nbranch, nleaf, qbranch, qleaf;
    /** length of an element in a branch and leaf, length of the page metadata */
    int mbranch, mleaf, mmeta = 8;
    /** size of the key, value and index */
    int keysize, valsize, dexsize = Types.Enum._int.size;
    /** position in the element of the key, value and index */
    int pkey, pval, pdex;
    double minfill = 3.0/8.0;
    protected int zeroMerge = 0;

    void init(int $keysize,int $valsize) {
        keysize = $keysize;
        valsize = $valsize;
        mbranch = keysize + dexsize;
        mleaf = keysize + valsize;
        pkey = 0;
        pval = pdex = keysize;
        nbranch = (bs - mmeta) / mbranch;
        nleaf   = (bs - mmeta) / mleaf;
        qbranch = (int) (minfill * nbranch);
        qleaf   = (int) (minfill * nleaf);
    }

    /** get the root and init context.depth if page is null, else set the page */
    abstract PP rootz(PP page,CC context) throws Pausable;
    /** set the map depth and update context.depth */
    abstract void depth(int level,CC context) throws Pausable;
    

    void init(CC context) throws Pausable {
        PP root = createPage(true,context);
        rootz(root,context);
        depth(0,context);
        commit(root,context);
    }

    /** find the index of the first key in the page >= key - stepped linear search */
    int findIndex(PP page,int mode,CC context) {
        boolean greater = modes.greater( mode );
        int step = 16, num = page.numkeys(), k1;
        k1 = findLoop(page,step,num,step,context,greater);
        return k1;
    }
    protected int findLoop(PP page,int k1,int num,int step,CC context,boolean greater) {
        for (; k1<num; k1+=step) {
            int cmp = compare( page, k1, context );
            if (greater & cmp==0) cmp = 1;
            if (cmp <= 0) break;
        }
        if (step > 1)
            return findLoop(page,k1-step,num,1,context,greater);
        return k1;
    }
    public Range<CC> findPrefix(CC c1) throws Pausable {
        return findRange( c1, (CC) c1.clone().mode( modes.gtp ) );
    }
    public Range<CC> findRange(CC c1,CC c2) throws Pausable {
        Path b1 = findPath(c1,false);
        Path b2 = findPath(c2,false);
        return range().set(b1,b2,c1);
    }
    protected Range<CC> range() { return new Range(this); }
    /** 
     * search the tree using the provided context
     * @param context the context, including the key to search for
     */
    public void findData(CC context) throws Pausable {
        findPath(context,true);
        if (true) return;

        initContext(context);
        PP page = rootz(null,context);
        int ko=0, level=0, depth=context.depth;
        while (true) {
            context.match = false;
            ko = findIndex( page, context.mode, context );
            if (level==depth) break;
            page = dexs(page,ko,level+1==depth,context);
            level++;
        }
        if (ko < page.num && (!modes.eq(context.mode) | compare(page,ko,context)==0)) {
            context.match = true;
            getccx(page, context, ko);
        }
    }
    /** search the tree for the data represented by context and return the path to the matching node
     * @param context the context, including the key to search for
     * @param get if true, store the matching key and value in context
     * @return the path to the matching node
     */
    public Path findPath(CC context,boolean get) throws Pausable {
        initContext(context);
        PP page = rootz(null,context);
        Path<PP> path = null;
        int ko, level=0, depth=context.depth;
        boolean right = true;
        while (true) {
            context.match = false;
            ko = findIndex( page, context.mode, context );
            Path p3 = path;
            path = new Path();
            path.set(p3,page,ko);
            path.right = right;
            if (level==depth) break;
            if (ko < page.num-1) right = false;
            page = dexs(page,ko,level+1==depth,context);
            level++;
        }
        if (get & ko==page.num) {
            Path<PP> p2 = next(path,context);
            if (p2 != null) {
                path = p2;
                page = path.page;
                ko = path.ko;
            }
        }
        if (ko < page.num && (!modes.eq(context.mode) | compare(page,ko,context)==0)) {
            context.match = true;
            if (get) getccx(page, context, ko);
        }
        return path;
    }
    /** verify the integrity of the tree, throwing a runtime error for problems */
    void verify(CC cc) throws Pausable {
        PP page = rootz(null,cc);
        if (cc.depth > 0) verify(page,0,cc,null);
    }
    void verify(PP page,int level) throws Pausable {
        CC cc = context();
        getccx(page,cc,0);
        initContext(cc);
        for (int ii = 1; ii < page.numkeys(); ii++) {
            int cmp = compare(page,ii,cc);
            if (cmp > 0) {
                dump(page,"verify page: ",cc,level,1);
                System.out.format( "verify page fail, page:%5d, index:%5d\n", page.kpage, ii );
                Simple.softAssert(false);
            }
            getccx(page,cc,ii);
            initContext(cc);
        }
    }
    void verify(PP page,int level,CC cc,CC cp) throws Pausable {
        Simple.softAssert(page.leaf==0);
        verify(page,level);
        checkDel(page,true);
        for (int ii = 0; ii < page.num; ii++) {
            PP child = dexs(page,ii,level+1==cc.depth,cc), child2 = null;
            CC c2 = cp;
            if (ii+1 < page.num) {
                c2 = context();
                getccx(page,c2,ii);
                initContext(c2);
                child2 = dexs(page,ii+1,level+1==cc.depth,cc);
            }
            if (level+1 < cc.depth)
                verify(child,level+1,cc,c2);
            else if (c2 != null) {
                int cmp = compare(child,child.num-1,c2);
                Simple.softAssert(cmp >= 0);
            }
            if (child2 != null && (child2.num > 1 | level+1==cc.depth)) {
                int cmp = compare(child2,0,c2);
                if (cmp > 0) {
                    dump(page,"verify: ",cc,level,2);
                    System.out.format( "verify fail, page:%5d, index:%5d\n", page.kpage, ii );
                    Simple.softAssert(false);
                }
            }
        }
    }
    
    /** 
     * split must be done during decent of the tree if the page is full, ie, the parent is never full
     * split page0 with parent index kp into page1 (ie page1 is the new page)
     * modifies the depth if parent is null -- fixme - makes level-tracking complicated
     * @param raft the index at which a new pair will be inserted, which is updated by the split 
     */
    PP split(PP page0,Raft<PP> raft,PP parent,int kp,boolean leaf,CC context) throws Pausable {
        PP page1 = createPage(leaf,context);
        if (parent == null) {
            parent = createPage(false,context);
            rootz(parent,context);
            kp = shift(parent,0);
            key(parent,kp,page0, page0.num-1);
            parent.dexs(kp,page0.kpage);
            depth(context.depth+1,context);
        }
        else prep(parent);
        prep(page0);
        int kb = page0.num/2;
        split(page0,page1,kb);
        kp = shift(parent,kp);
        int kp1 = nextIndex(parent,kp);
        // for non-leafs, this isn't usually a "valid" key ... but we just did the split so it's ok
        key(parent, kp, page0, page0.num-1);
        parent.dexs(kp , page0.kpage);
        parent.dexs(kp1, page1.kpage);
        commit(parent,context);
        // can potentially skip a commit if it's a leaf cause we're going to insert below
        //   but not a big win and complicates copy-on-write
        //   so just commit everything
        commit(page0,context);
        commit(page1,context);
        if (raft.ko >= page0.num) {
            raft.ko -= page0.num;
            page0 = page1;
        }
        return page0;
    }
    /** collect garbage for the page, causing the page to be sequential */
    void prep(PP page) {}
    /** a modifiable index, used to adjust insert location post split */
    static class Raft<PP> { int ko; }

    /** insert the key/value pair in context into the tree */
    public void insert(CC context) throws Pausable { insert1(context); }
    /** 
     *  single pass insert
     *  limitations - need to know where we're going to split a child (which then promotes a key)
     *    so that we can calculate the potential parent size and split it if needed
     *  slightly faster (5%-ish) than the 2pass
     */
    protected void insert1(CC context) throws Pausable {
        initContext(context);
        Raft<PP> raft = new Raft();
        PP page = rootz(null,context), parent=null, left=page;
        for (int kp, level=context.depth; level >= 0; level--, parent=page, page=left) {
            kp = raft.ko;
            raft.ko = findIndex(page, modes.gt, context);
            if (level > 0)
                left = dexs(page,raft.ko,level==1,context);
            if (overcap(page,context,level==0,left))
                page = split(page, raft, parent, kp, level==0, context);
        }
        insert(parent,context,raft.ko);
    }
    
    
    
    
    /** insert the key value pair in context into the map */
    protected void insert2(CC context) throws Pausable {
        context.mode = modes.gt;
        Path<PP> path = findPath(context,false);
        insertPath(path,context);
//        insert(path.page,context,path.ko);
    }
    int bisectFixed(Path<PP> path,PP page1) {
        int num = (path.page.num+1)/2;
        if (path.ko < num)
            return num-1;
        path.page = page1;
        path.ko -= num;
        return num;
    }
    /** split and update path and insert left and right (if non-null) else context, returning the new page */
    PP splitPage(Path<PP> path,CC context,PP left,PP right) throws Pausable {
        return null; // must be overrided
    }
    /** traverse path, splitting each page if needed and insert the key/value pair in context */
    protected void insertPath(Path<PP> path,CC context) throws Pausable {
        PP left=null, right=null, page0, page1;
        for (; path != null && overcap(page0=path.page,context,right==null,left);
                left=page0, right=page1, path=path.prev) {
            page1 = splitPage(path,context,left,right);
        }
        if      (path  == null) setroot  (                  left,right,context);
        else if (right != null) setchilds(path.page,path.ko,left,right,context);
        else insert(path.page,context,path.ko);
    }
    /** create a new root node and set left and right as children */
    private void setroot(PP left,PP right,CC context) throws Pausable {
        PP parent = createPage(false,context);
        parent.num = 2;
        rootz(parent,context);
        depth(context.depth+1,context);
        key( parent,0, left,left.num-1);
        parent.dexs(0, left.kpage);
        parent.dexs(1,right.kpage);
        commit(parent,context);
    }
    /** set left and right as children of parent at ko */
    void setchilds(PP parent,int ko,PP left,PP right,CC context) {
        prep(parent);
        compress(parent,context,ko,left,false);
        shift(parent,ko);
        // for branches, the last key isn't valid, but left was *just split*, so it's still a ghost, hence valid
        key(  parent,ko,   left,left.num-1);
        parent.dexs( ko,   left.kpage);
        parent.dexs( ko+1,right.kpage);
        commit(parent,context);
    }
    void compress(PP page,CC context,int ko,PP left,boolean leaf) {}
    /** insert context into page and return the index */
    void insert(PP page,CC context,int ko) throws Pausable {
        checkDel(page,false);
        compress(page,context,ko,null,true);
        ko = shift(page,ko);
        setccx(page,context,ko);
        commit(page,context);
        checkDel(page,false);
    }
    /** delete the first element equal to context.key */
    public CC remove(CC context) throws Pausable {
        context.mode = modes.eq;
        Path<PP> path = findPath(context,true);
        if (context.match) 
            remove(path,context,path.right);
        return context;
    }
    int delete(PP page,int index) {
        prep(page);
        return page.delete(index);
    }
    /**  remove the element located at path, using the transaction info in context
     * @param path the location of the element to remove
     * @param context the context
     */
    public void remove(Path<PP> path,CC context) throws Pausable { remove(path,context,false); }
    /** 
     *  remove the element described by path and rebalance the tree
     *  right means that the element is the last element in the tree, ie furthest-right
     */
    protected void remove(Path<PP> path,CC context,boolean right) throws Pausable {
        PP page = path.page;
        int size = size(page,null,null,true,null,0);
        if (!right) cleanDups(path,context);
        delete(page,path.ko);
        commit(page,context);
        int s2 = size(page,null,null,true,null,0);
        int so = (int) (minfill*bs);
//        System.out.format( "Btree.remove -- page:%5d num:%5d, %5d %5d %5d\n", page.kpage, page.num, size, s2, so );
        if (page.num==0 || s2 < so && size >= so)
            combine( path, context );
    }
    public void update(Path<PP> path,CC context) throws Pausable {
        // simple case ... val sizes are fixed, so just update the value
        int cmp = compare(path.page,path.ko,context);
        Simple.softAssert(cmp==0,"updating a value requires the key is unchanged");
        setccx(path.page,context,path.ko);
    }
    /** 
     * if we've removed the last element in the page and it's a duplicate
     * need to propagate the deletion up the path
     * not required for a furthest-right element
     */
    void cleanDups(Path<PP> path,CC context) {
        if (true) return;
        // climb the path till find a page that's not going to be empty
        // use that path elements previous entry as key
        // continue to climb the path till reaching a non-last-element, setting the keys
        while (path.prev != null && path.page.num==1)
            path = path.prev;
        Path<PP> po = path;
        while (path.prev != null && path.ko==path.page.num-1) {
            path = path.prev;
            key(path.page,path.ko,po.page,po.ko-1);
            commit(path.page,context);
        }
    }
    void combine(Path<PP> path,CC context) throws Pausable {
        Path<PP> po = path;
        boolean yes = true;
        int level = context.depth;
        for (; yes && path.prev != null; path = path.prev, level--)
            yes &= merge2( path, context, level );
        if (yes & context.depth > 0 && path.page.num()==1) {
            PP root = path.page;
            PP child = dexs(root,0,context.depth==1,context);
            free(root);
            rootz(child,context);
            depth(context.depth-1,context);
        }
    }
    /**
     * merge p2 (the right element) into p1 (the left element) and update parent
     * kp is index of p1 in parent
     */
    void merge(PP p1,PP p2,PP parent,int n1,int n2,int kp,CC context) {
        int k1 = p1.num-1;
        merge(p2,p1);
        free(p2);
        // the last key in a branch is omited so on merge need to retrieve it from the parent
        if (p1.leaf==0) key(p1,k1,parent,kp);
        key(parent,kp,parent,kp+1);
        delete(parent,kp+1);
        commit(p1,context);
        commit(parent,context);
    }
    void checkDel(PP page,boolean force) {}
    /** 
     * merge path either to the right or left
     * leafs only merge at 0 or pac
     */
    boolean merge2(Path<PP> path,CC context,int level) throws Pausable {
        PP page = path.page;
        int num = page.num();
        boolean leaf = (level==context.depth);
        PP parent = path.prev.page;
        int kp    = path.prev.ko;
        if (num==0) {
            zeroMerge++;
            kp = delete(parent,kp);
            commit(parent,context);
            free(page);
            if (kp >= 0 && kp < parent.num) {
                path.page = dexs(parent,kp,leaf,context);
            }
            else {
                // this happens when pop() empties the branch and then remove() gets the last element
                path.page = null;
            }
            return true;
        }
        int size = size(page,null,null,leaf,null,0);
        if (size > minfill*bs) return false;
        if (kp > 0) {
            PP p2 = dexs(parent,kp-1,leaf,context);
            int s2 = size(p2,page,null,leaf,null,0);
            int n2 = p2.num();
            if (s2 <= 2*minfill*bs) {
                path.page = p2;
                path.ko += p2.num;
                prep(p2);
                merge(p2, page, parent, n2, num, kp-1, context);
                return true;
            }
        }
        if (kp+1 < parent.num) {
            PP p2 = dexs(parent,kp+1,leaf,context);
            int s2 = size(page,p2,null,leaf,null,0);
            int n2 = p2.num();
            if (s2 <= 2*minfill*bs) {
                merge(page, p2, parent, num, n2, kp, context);
                return true;
            }
        }
        return false;
    }
    /** an opaque handle to allow passing pages publicly without loss of encapsulation */
    public static class OpaquePage<PP extends Page> {
        PP page;
    }
    /** a linked list representing a position in the tree */
    public static class Path<PP extends Page> {
        // fixme:encapsulation - btree subclasses could need access to private members
        //                       use static protected methods
        Path() {}
        // fixme - prev is the link to the parent level, ie they move vertically
        // but this creates confusion as the path also has operations prev() and next() that
        // move horizontally, and these are part of the public api
        // should probably rename prev, perhaps to "link"
        Path<PP> prev;
        PP page;
        int ko;
        boolean right;
        public OpaquePage<PP> getPage() {
            OpaquePage<PP> op = new OpaquePage<>();
            op.page = page;
            return op;
        }
        public boolean isEqual(OpaquePage op) { return op.page==page; }
        Path set(Path $prev,PP $page,int $ko) { prev = $prev; page = $page; ko = $ko; return this; }
        Path<PP> [] list(int depth) {
            Path<PP> head = this;
            Path<PP> [] list = new Path[depth+1];
            for (int ii=depth; ii >= 0; ii--, head=head.prev)
                list[ii] = head;
            return list;
        }
        Path dup() {
            Path po = prev, dup = new Path(), head = dup;
            dup.set(null,page,ko);
            for (; po != null; po = po.prev) {
                head = head.prev = new Path();
                head.set(null,po.page,po.ko);
            }
            return dup;
        }
        boolean same(Path p2) {
            return page.kpage==p2.page.kpage & ko==p2.ko;
        }
        /** page has just been split into page1, update the path if needed and return the non-path page */
        PP updateAfterSplit(PP page1) {
            if (ko < page.num) return page1;
            PP page0 = page;
            ko -= page0.num;
            page = page1;
            return page0;
        }
        Path<PP> flip() {
            Path cur, nxt, prv;
            for (cur=this, nxt=null; cur.prev != null; ) {
                prv = cur.prev;
                cur.prev = nxt;
                nxt = cur;
                cur = prv;
            }
            cur.prev = nxt;
            return cur;
        }
        void copy(Path<PP> zi) {
            Path<PP> xi = this;
            for (; xi != null; xi=xi.prev, zi=zi.prev)
                xi.set(xi.prev,zi.page,zi.ko);
        }
    }

    public Path<PP> next(Path<PP> path,CC context) throws Pausable {
        path.ko++;
        if (path.ko >= path.page.num) return nextPage(path, context);
        return path;
    }
    public Path<PP> prev(Path<PP> path,CC context) throws Pausable {
        path.ko--;
        if (path.ko < 0) return prevPage(path, context);
        return path;
    }

    public void getPath(Path<PP> path,CC context) throws Pausable {
        context.match = true;
        getccx( path.page, context, path.ko );
    }
    
    public Range<CC> getall(CC context) throws Pausable {
        initContext(context);
        Path<PP> p1 = firstPath(context);
        Path<PP> p2 = lastPath(context);
        return range().set(p1,p2,context);
    }
    
    protected boolean isToast(CC context) { return false; }
    void toastPage(Path<PP> path,CC context) throws Pausable {}
    public void slurp(Path<PP> p1,Path<PP> p2,CC context) throws Pausable {}
    
    public static class Range<CC extends Btree.Context> {
        // from c1 to c2 (exclussive)
        Path p1, p2, px;
        public CC cc;
        boolean first = true, preinit = true, init = true;
        Btree btree;
        public Range(Btree $btree) { btree = $btree; }
        public Range set(Path $c1,Path $c2,CC $cc) { p1=$c1; p2=$c2; cc=$cc; return this; }

        public void update() throws Pausable { btree.update(p1,cc); }
        public void remove() throws Pausable { btree.remove(p1,cc); }
        
        public <TT> ArrayList<TT> getall(Function<CC,TT> map) throws Pausable {
            ArrayList vals = new ArrayList();
            btree.slurp(p1,p2,cc);
            while (next()) vals.add(map.apply(cc));
            return vals;
        }
        public void visit(Consumer<CC> consumer) throws Pausable {
            btree.slurp(p1,p2,cc);
            while (next()) consumer.accept(cc);
        }
        public int count() throws Pausable {
            btree.slurp(p1,p2,cc);
            if (p2 != null && p1.page.kpage==p2.page.kpage) return p2.ko - p1.ko;
            int cnt = p1.page.num - p1.ko;
            Path po = p1.dup();
            while ( (po = btree.nextPage(po,cc)) != null && po.page.kpage != p2.page.kpage )
                cnt += po.page.num;
            if (po != null)
                cnt += p2.ko - po.ko;
            if (po==null & p2 != null)
                Simple.softAssert(false);
            return cnt;
        }
        // fixme::broken - px never gets set, replace with next() ???
        public CC refresh() throws Pausable { btree.getccx(p1.page,cc,p1.ko); return cc; }
        public boolean valid() { return p1 != null && (p2==null || !p1.same(p2)); }
        public boolean init() throws Pausable {
            boolean valid = valid();
            if (valid) refresh();
            return valid;
        }
        public boolean hasnext() throws Pausable {
            if (init)        px = p1.dup();
            else if (!first) px = btree.next(px,cc);
            first = true;
            init = false;
            boolean valid = px != null && (p2==null || !px.same(p2));
            return valid;
        }
        public boolean hasprev() throws Pausable {
            if (init)          px = p2.dup();
            boolean valid = px != null && (p1==null || !px.same(p1));
            if (init | !first) px = btree.prev(px,cc);
            first = true;
            init = false;
            if (px==null) valid = false;
            return valid;
        }
        /**
         * make the next element in the range current and store the key/value pair in context
         * @return true if the element is valid
         */
        public boolean next() throws Pausable {
            boolean valid = hasnext();
            first = false;
            if (valid) btree.getccx(px.page,cc,px.ko);
            return valid;
        }
        public boolean prev() throws Pausable {
            boolean valid = hasprev();
            first = false;
            if (valid) btree.getccx(px.page,cc,px.ko);
            return valid;
        }
        /**
         * make the next element in the range current and store the key/value pair in context.
         * additionally, when a new page is accessed for the first time, toast all elements
         * @return true if the element is valid
         */
        public boolean nextGreedy() throws Pausable {
            Page page = px.page;
            boolean valid = hasnext();
            if (valid && (preinit | page != px.page)) btree.toastPage(px,cc);
            first = preinit = false;
            if (valid) btree.getccx(px.page,cc,px.ko);
            return valid;
        }
    }
    /**
     * advance path to the next page in the path
     * @param path the initial path, which is modified to point to the next page
     * @param start the depth of the path
     * @param context the context
     */
    void advance(Path<PP> path,int start,CC context) throws Pausable {
        path.ko++;
        if (path.ko < path.page.num)
            return;

        int level = start;
        Path<PP> [] paths = new Path[start];
        while (true) {
            if (path.prev==null)
                return;
            level--;
            paths[level] = path;
            path = path.prev;
            path.ko++;
            if (path.ko < path.page.num) break;
        }
        while (level < start) {
            PP page = dexs(path.page,path.ko,level+1==context.depth,context);
            path = paths[level];
            path.set(path.prev,page,0);
            level++;
        }
    }
    Path<PP> nextPage(Path<PP> path,CC context) throws Pausable {
        Path<PP> prev = path.prev;
        if (prev==null) return null;
        if (prev.ko+1 < prev.page.num) {
            prev.ko++;
            PP page = dexs(prev.page,prev.ko,true,context);
            path.page = page;
            path.ko = 0;
            return path;
        }
        int level = context.depth;
        while (true) {
            path = path.prev;
            level--;
            if (path==null) return null;
            path.ko++;
            if (path.ko < path.page.num) break;
        }
        while (level < context.depth) {
            PP page = dexs(path.page,path.ko,level+1==context.depth,context);
            level++;
            Path tmp = path;
            path = new Path();
            path.set(tmp,page,0);
        }
        return path;
    }
    Path<PP> prevPage(Path<PP> path,CC context) throws Pausable {
        Path<PP> prev = path.prev;
        if (prev==null) return null;
        if (prev.ko > 0) {
            prev.ko--;
            PP page = dexs(prev.page,prev.ko,true,context);
            path.page = page;
            path.ko = page.num-1;
            return path;
        }
        int level = context.depth;
        while (true) {
            path = path.prev;
            level--;
            if (path==null) return null;
            path.ko--;
            if (path.ko >= 0) break;
        }
        while (level < context.depth) {
            PP page = dexs(path.page,path.ko,level+1==context.depth,context);
            level++;
            Path tmp = path;
            path = new Path();
            path.set(tmp,page,page.num-1);
        }
        return path;
    }
    public static class Stats {
        int np, num, level;
        public String toString() {
            return String.format( "stats: np:%5d, num:%5d, level:%5d", np, num, level );
        }
    }
    public Stats stats(CC context) throws Pausable {
        initContext(context);
        Stats xx = new Stats();
        int num = 0, np = 0;
        for (Path path = firstPath(context); path != null; path = nextPage(path, context)) {
            np++;
            int n2 = path.page.num();
            num += n2;
        }
        int level = 0;
        for (Path path = firstPath(context); path != null; path = path.prev) level++;
        xx.np = np;
        xx.num = num;
        xx.level = level;
        return xx;
    }

    /** return the path to the first page. note the leaf index is 0, not necessarily the first element */
    protected PP lastPage(CC context) throws Pausable {
        PP page = rootz(null,context);
        for (int level = 0; level < context.depth; level++)
            page = dexs(page,page.num-1,level+1==context.depth,context);
        return page;
    }
    /** return the path to the first page. note the leaf index is 0, not necessarily the first element */
    protected PP firstPage(CC context) throws Pausable {
        PP page = rootz(null,context);
        for (int level = 0; level < context.depth; level++)
            page = dexs(page,0,level+1==context.depth,context);
        return page;
    }
    /** 
     * return the path to the first page.
     * note the leaf index is 0, not necessarily the first element
     */
    public Path<PP> first(CC context) throws Pausable {
        initContext(context);
        return firstPath(context);
    }
    protected Path<PP> firstPath(CC context) throws Pausable {
        Path path = null;
        PP page = rootz(null,context);
        int level = 0;
        while (true) {
            Path tmp = new Path();
            tmp.set(path,page,0);
            path = tmp;
            if (level==context.depth) break;
            page = dexs(page,0,level+1==context.depth,context);
            level++;
        }
        return path;
    }
    /** return the path to the last page. note the leaf index is 0, not the last element */
    public Path<PP> last(CC context) throws Pausable {
        initContext(context);
        Path<PP> path = lastPath(context);
        path.ko--;
        context.match = false;
        if (path.ko >= 0) {
            context.match = true;
            getPath(path,context);
        }
        return path;
    }
    Path<PP> lastPath(CC context) throws Pausable {
        Path path = null;
        PP page = rootz(null,context);
        int level = 0;
        while (true) {
            int ko = page.num-1;
            Path tmp = new Path();
            tmp.set(path,page,ko);
            path = tmp;
            if (level==context.depth) break;
            page = dexs(page,ko,level+1==context.depth,context);
            level++;
        }
        path.ko = page.num;
        return path;
    }

    

    protected void  prepx(PP page,CC context,int ko) {}
    protected void getccx(PP page,CC context,int ko) throws Pausable { getcc(page,context,ko); }
    protected void setccx(PP page,CC context,int ko) throws Pausable { setcc(page,context,ko); }

    /** set the key and val (if not leaf) in context to the values from page at ko */
    protected abstract void getcc(PP page,CC context,int ko);
    /** set the key and val (if not leaf) in page at ko to the vals in context */
    protected abstract void setcc(PP page,CC context,int ko);
    /** copy the value from page1[k1] to page0[k0] */
    protected abstract void key(PP page0,int k0,PP page1,int k1);
    /** compare the key in page at index with the key in data */
    protected abstract int compare(PP page,int index,CC data);
    /** return a new tree-specific Context with key and val */
    public abstract CC context();

    /** split page src(kb,:) into dst, ie move half-ish of src to dst (which is empty) */
    protected abstract void split(PP src,PP dst,int kb);
    /** create space for an insertion into page at ko and return the insertion point */
    protected abstract int shift(PP page,int ko);

    /** merge src into dst */
    protected abstract void merge(PP src,PP dst);

    
    /** read the state variables - potentially expensive */
    void initContext(CC context) throws Pausable {
        // note: cc.depth gets initialized in rootz()
    }

    
    
    protected abstract PP getPage(int kpage,CC cc,boolean leaf) throws Pausable;
    /** create a new page */
    protected abstract PP createPage(boolean leaf,CC context) throws Pausable;

    int pac(boolean leaf) { return leaf ? qleaf:qbranch; }
    int cap(boolean leaf) { return leaf ? nleaf:nbranch; }
    /** is the page too full to add this key/val pair ? */
    boolean overcap(PP page,CC cc,boolean leaf,PP left) { return page.num() == cap(leaf); }
    /** return the length of the data in page merged with other and cc inserted */
    protected final int nextpos(PP page,PP other,CC cc,boolean leaf) {
        int num = page.num;
        if (other != null) num += other.num;
        if (cc != null) num++;
        int size = leaf ? mleaf : mbranch;
        return size*num;
    }
    protected int size(PP page,PP other,CC cc,boolean leaf,PP parent,int kp) {
        return mmeta + nextpos(page,other,cc,leaf);
    }
    /** 
     *  clear any in-memory resources. for db4j this is not public api as there are no such resources,
     *  but for troubleshooting and measuring performance it can be useful (DirectMap, Bhunk.fake).
     *  for srlutils it is public api
     */
    void clear() {}
    protected int nextIndex(PP page,int ko) { return ko+1; }
    protected void commit(PP page,CC cc) {}
    

    /** get the page pointed to by parent[index] */
    PP dexs(PP parent,int index,boolean leaf,CC cc) throws Pausable {
        int kpage = parent.dexs(index);
        PP page = getPage(kpage,cc,leaf);
        boolean l2 = page.leaf==1;
        Simple.softAssert(l2==leaf);
        return page;
    }
    
    
    
    
    public static class Context implements Cloneable {
        /** did the key represented by this match a key in the tree */
        public boolean match;
        /** the mode for the search */
        public int mode;
        public int depth;
        public Context clone() {
            try { return (Context) super.clone(); }
            catch (Exception ex) { return null; }
        }
        public Context mode(int $mode) { mode=$mode; return this; }
        public String format(int both) { return ""; }
    }
    

    protected void free(PP page) {}

    
    static abstract class DirectMap<CC extends Context>
        extends Btree<CC,Sheet> {
        /** the root page */
        protected Sheet rootz;
        DynArray.ints kdels = new DynArray.ints();
        DynArray.Objects<Sheet> pages = new DynArray.Objects().init(Sheet.class);
        protected int depth;
        Sheet rootz(Sheet page,CC context) throws Pausable {
            if (page==null) context.depth = depth;
            else rootz = page;
            return rootz;
        }
        protected void free(Sheet page) {
            pages.vo[page.kpage] = null;
            kdels.add(page.kpage);
            page.clean();
        }
        void depth(int level,CC context) throws Pausable {
            context.depth = depth = level;
        }
        protected void split(Sheet src,Sheet dst,int kb) { src.split(dst,kb); }
        protected int shift(Sheet page, int ko) { return page.shift(ko); }
        protected void merge(Sheet page0,Sheet page1) { page0.merge(page1); }
        // access for srlutils differs from db4j to allow Bface-based testing
        void clear() {
            for (Sheet page : pages)
                if (page != null) page.clean();
            pages.clear();
            kdels.size = 0;
            rootz = null;
        }
        protected Sheet getPage(int kpage,CC cc,boolean leaf) throws Pausable { return pages.vo[kpage]; }
        /** create a new page */
        protected Sheet createPage(boolean leaf,CC context) throws Pausable {
            int index;
            if (kdels.size > 0) index = kdels.vo[--kdels.size];
            else                index = pages.add(null);
            Sheet page = newPage(leaf,context,true);
            page.leaf = leaf ? 1:0;
            page.kpage = index;
            pages.vo[index] = page;
            return page;
        }
        public int countPages() {
            int count = 0;
            for (int ii = 0; ii < pages.size; ii++)
                if (pages.vo[ii] != null) count++;
            return count;
        }
        protected Sheet newPage(boolean leaf,CC cc,boolean alloc) {
            Sheet page = new Sheet();
            page.init( bs, leaf ? mleaf:mbranch, leaf ? pval:pdex, null );
            if (alloc) page.buf = new byte[bs];
            return page;
        }
        protected void key(Sheet p0, int k0,Sheet p1, int k1) {
            p1.rawcopy(p0,k1,k0,pkey,keysize);
        }
        public int [] getInfo() { return new int[] {depth, pages.size-kdels.size, zeroMerge}; }

        // shim implementation of Hunkable
        protected int create() { return 0; }
        protected void createCommit(long locBase) {}
        protected String name() { return null; }
        protected Bhunk set(Db4j db4j,String name) { return null; }
        protected void postInit(Transaction txn) throws Pausable {}
        protected void postLoad(Transaction txn) throws Pausable {}
    }
    /** optional method, return an array of depth, number of pages, and zero merges */
    public int [] getInfo() { return new int[] {-1,-1,-1}; }
    protected String info() {
        int [] info = getInfo();
        return String.format("Btree depth:%d, pages:%d\n", info[0], info[1]);
    }
    public void dump(CC cc) throws Pausable {
        PP root = rootz(null,cc);
        System.out.println( "-----------------------------------------------------------" );
        dump( root, "", cc, 0, cc.depth+1 );
        System.out.println( "-----------------------------------------------------------" );
    }
    protected void dump(PP page,String prefix) {
        CC cc = context();
        dump(page,prefix,cc,page.leaf==1);
    }
    protected void dump(PP page,String prefix,CC cc,boolean leaf) {
        System.out.format("%spage %5d.%s, %5d entries, %5d size\n",
                prefix, page.kpage, leaf ? "leaf":"branch", page.num, size(page,null,null,leaf,null,0));
        if (leaf)
            for (int k1=0; k1 < page.num; k1++) {
                getcc(page,cc,k1);
                System.out.format("%s  | %5d: %s\n", prefix, k1, cc.format(1));
            }
        else
            for (int k1=0; k1 < page.num; k1++) {
                CC c2 = cc;
                if (k1+1 < page.num) getcc(page,cc,k1);
                else c2 = context();
                int kpage = page.dexs(k1);
                System.out.format("%s | - branch:%5d key:%s --> %5d\n", prefix, k1, c2.format(0), kpage);
            }
    }
    protected void dump(PP page,String prefix,CC cc,int level,int num) throws Pausable {
        if (num==0) return;
        boolean leaf = (cc.depth==level);
        dump(page,prefix,cc,leaf);
        if (!leaf & num > 1)
            for (int k1=0; k1 < page.num; k1++) {
                PP p2 = dexs(page,k1,level+1==cc.depth,cc);
                dump(p2,prefix+"  ",cc,level+1,num-1);
                System.out.format("%s    -branch: %s\n", prefix, cc.format(0));
            }
    }
    static abstract class Tester extends TaskTimer.Runner<Void> {
        boolean ok;
        { stageNames = "put look rem chk".split(" "); }
        public void alloc() { setup(stageNames.length, "DF"); }
        public boolean finish() { return ok; }
        DF map = new DF();
        Data cc;
        int nn;
        double [] keys;

        /* this block is kilim-specific code in db4j that is omitted or shimmed in srlutils */
        boolean pable = (kilim.Pausable.class==(Class)Pausable.class);
        public static boolean forceFiber = true;
        Fiber fiber = new kilim.Fiber(null);
        public void doinit(Fiber fiber) { throw new RuntimeException(); }
        public void dorun(int stage,Fiber fiber) { throw new RuntimeException(); }

        
        public void doinit() throws Pausable {}
        public void init() throws kilim.NotPausable {
            keys = Rand.rand(nn+1);
            cc = map.context();
            ok = true;
            if (pable) { doinit(fiber.begin()); fiber.end(); }
            else   try { doinit(); } catch (Exception ex) {}
        }
        public void kiss() { map.clear(); }

        public void dorun(int stage) throws Pausable {}
        public void run(final int stage) throws kilim.NotPausable {
            if (pable) { dorun(stage,fiber.begin()); fiber.end(); }
            else
                try {
                    if (forceFiber) fiber.begin();
                    dorun(stage);
                    if (forceFiber) fiber.end();
                }
                catch (Exception ex) {}
        }
        // seems kilim won't weave a method if the class already has the method's Fiber-enabled dual
        //   use a subclass to get around it
        static class Sub extends Tester {
            public Sub(int $nn) { nn = $nn; }
            public void doinit() throws Pausable { map.init(cc); }
            public void dorun(int stage) throws Pausable {
                for (int jj = 0; jj < nn; jj++) {
                    final float v1 = 0.01f*jj, goal = stage==3 ? -1f:v1;
                    if      (stage==0) map.insert  (cc.set(keys[jj],v1));
                    else if (stage==2) map.remove  (cc.set(keys[jj]));
                    else               map.findData(cc.set(keys[jj]));
                    if (stage > 0 && cc.val() != goal) ok = false;
                }
            }
        }
        public static class Data extends Btree.Context implements org.srlutils.btree.TestDF.DFcontext {
            public double key;
            public float val;
            public Data set(double $key,float $val) { key = $key; val = $val; return this; }
            public Data set(double key) { return set(key,-1f); }
            public float val() { return match ? val:-1f; }
        }
        public static class DF extends Btree.DirectMap<Data> {
            { init(Types.Enum._double.size,Types.Enum._float.size); }
            public void setcc(Sheet po,Data cc,int ko) { po.put(pkey,ko,cc.key);  po.put(pval,ko,cc.val);  }
            public void getcc(Sheet po,Data cc,int ko) { cc.key=po.getd(pkey,ko); cc.val=po.getf(pval,ko); }
            double key(Sheet page,int index) { return page.getd(pkey,index); }
            protected int compare(Sheet page,int index,Data data) { return Butil.compare(data.key,key(page,index)); }
            protected int findLoop(Sheet page,int k1,int num,int step,Data context,boolean greater) {
                for (; k1<num; k1+=step) {
                    int cmp = compare( page, k1, context );
                    if (greater & cmp==0) cmp = 1;
                    if (cmp <= 0) break;
                }
                if (step > 1)
                    return findLoop(page,k1-step,num,1,context,greater);
                return k1;
            }
            public Data context() { return new Data(); }
        }
    }


    static class Demo {
        public static void main(String[] args) throws Exception {
            org.srlutils.btree.TestDF.auto(null,0,1,1,new Tester.Sub(1000000));
            org.srlutils.btree.TestDF.auto(null,0,3,3,new Tester.Sub(1000000));
        }
    }
    
    
    
}
    
/*
 * Tester performance: 1000000 items
 *                  fake      real pausable   next()
 *   put+get    :   0.94      1.12
 *   put+get+rem:   1.5       1.9             1.58/1.95
 * forceFiber has no effect
 * weaving (ie real pausable) is 25% slower than native, which is consistent with kilim docs
 */


