// copyright nqzero 2017 - see License.txt for terms

package org.db4j;

import java.util.Iterator;
import org.srlutils.Simple;
import static org.srlutils.Simple.Exceptions.rte;




/*
 * Note: this class duplicates org.srlutils.data.Listee with reduced privileges
 * this allows next and prev to be default access
 * many of the methods are unused, but they're kept here to maintain parity with the original
 * the only difference between the two sources should be TT --> Task
 * and reduced privileges
 */


/**
 * a doubly linking list node, intended to be extended ... making the extended class a node itself
 *   ie, payload as node
 *   as opposed to an external list node with a reference to a payload
 * head <--> next.node.prev <--> tail
 * TT is the node type
 * for the case where direct inheritance is impossible, Listee can be mixed in using an inner class
 */
@Deprecated
public class SoupListee<TT extends SoupListee<TT>> {
    /** the next element, ie in the direction of head, null indicating the end of the list */
    TT next;
    /** the previous element, ie in the direction of tail, null indicating the end of the list */
    TT prev;

    /** null out the links to the rest of the list - unsafe, ie the list is not maintained */
    void cleanup() { next = prev = null; }

    /**
     * only for circular lists, ie ones created with the static append or equivalent
     * return the next element in the list or null to indicate the list is terminated
     * @param base the first element in the list
     * @return the next element, or null if already the last element
     */
    TT next(TT base) {
        return next==base ? null : next;
    }
    /**
     * maintain a circular linked list, inserting node before base
     * @param <TT> the node type
     * @param base the first element of the list or null to indicate the list is empty
     * @param node the node to insert
     * @return the new base, which is unchanged unless base is null
     */
    public static <TT extends SoupListee<TT>> TT append(TT base,TT node) {
        if (base==null) { node.next = node.prev = node; return node; }
        TT prev = base.prev;
        node.next = base;
        node.prev = prev;
        prev.next = node;
        base.prev = node;
        return base;
    }

    
    /** the base of a doubly linked list */
    public static class Lister<TT extends SoupListee<TT>> implements Iterable<TT> {
        // note: check is useful for debugging, but is made final for speed ... remove if needed
        /** perform checks prior to most operations */
        public final boolean check = false;
        /** the head of the list, ie the end that push and pop */
        public TT head;
        /** the tail of the list, ie the end that append and drop */
        public TT tail;
        private Thread thread;
        private final boolean dbg = false;
        public int size;


        public void checkThreads() {
            Thread t2 = Thread.currentThread();
            if (thread==null) thread = t2;
            if (thread != t2)
                org.srlutils.Simple.softAssert( false, "multi-thread modifications: %s and %s", thread, t2 );
        }
        public boolean dump(String txt) {
            if (txt==null) txt = "";
            int cnt = 0;
            TT last = null;
            boolean match = true;
            for (TT node = head; node != null; last = node, node = node.prev, cnt++) {
                match &= (last==node.next && (last==null || last.prev==node));
            }
            System.out.format( "%sSummary: %6d nodes, for:%5b, rev:%5b\n", txt, cnt, last==tail, match );
            return last==tail && match;
        }
        public int size() { return size; }
        
        
        /** add node to the tail of the list */
        public void append(TT node) {
            if ( check ) checkUnlinked( node );
            node.next = tail;
            if (tail==null)      head = node;
            else            tail.prev = node;
            tail = node;
            size++;
        }
        /** push node into the head of the list */
        public void push(TT node) {
            if (check) checkUnlinked( node );
            node.prev = head;
            if (head==null)      tail = node;
            else            head.next = node;
            head = node;
            size++;
        }
        /** add node to the list behind base, ie closer to tail */
        public void addAfter(TT node,TT base) {
            if (check) { checkUnlinked(node); checkLinked(base); }
            node.next = base;
            node.prev = base.prev;
            if (base.prev != null) base.prev.next = node;
            base.prev = node;
            if (tail == base) tail = node;
            size++;
        }
        /** drop the last entry off the tail of the list, null its links and return it */
        public TT drop() {
            TT node = tail;
            if (check) checkLinked(node);
            if (node!=null) {
                tail = node.next;
                node.next = node.prev = null;
                if (tail==null) head = null;
                size--;
            }
            return node;
        }
        /** pop the first entry off the head of the list, null its links and return it */
        public TT pop() {
            TT node = head;
            if (check) checkLinked(node);
            if (node!=null) {
                head = node.prev;
                node.next = node.prev = null;
                if (head==null) tail = null;
                else head.next = null;
                size--;
            }
            return node;
        }
        /** remove node from the list */
        public void remove(TT node) {
            if (check) checkLinked(node);
            if (head==node) head = node.prev;
            if (tail==node) tail = node.next;
            if (node.prev != null) node.prev.next = node.next;
            if (node.next != null) node.next.prev = node.prev;
            node.next = node.prev = null;
            size--;
        }

        /** move node to the tail, node *must* already be inserted into the list */
        public void moveToTail(TT node) {
            if (check) checkLinked(node);
            if (node != tail) {
                remove(node);
                append(node);
            }
        }
        
        
        public class Iter implements Iterator<TT> {
            public TT curr, next = Lister.this.head;
            public boolean hasNext() { return next != null; }
            public TT next() { curr = next; next = next.prev; return curr; }
            public void remove() { Lister.this.remove(curr); }
        }

        public Iterator<TT> iterator() { return new Iter(); }

        /** verify that the list is consistent, ie size is the number of elements and head leads to tail */
        public void check() {
            int c1 = 0, c2 = 0;
            TT t2 = tail, h2 = head;
            for (TT val = head; val != null; t2 = val, val = val.prev) c1++;
            for (TT val = tail; val != null; h2 = val, val = val.next) c2++;
            if (c1 != c2 || c1 != size || h2 != head || t2 != tail)
                throw rte( null,
                        "Listee.inconsistent -- size:%d %d %d, connect:%b %b",
                        size, c1, c2, h2==head, t2==tail );
        }
        /** check that the node is a legal node to be removed */
        public void checkUnlinked(TT node) {
            Simple.softAssert(
                    node.next==null && node.prev==null && node!=head,
                    "node invalid for insertion" );
        }
        /** check that the node is a legal node to be removed */
        public void checkLinked(TT node) {
            Simple.softAssert(
                    (node==head || node.next != null) && (node==tail || node.prev != null ),
                    "node invalid for removal" );
        }
        public boolean isnode(TT node) { return node==head || node.next != null; }
    }



}


