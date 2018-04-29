package org.imdea.vcd.queue;

/**
 *
 * Some methods are copied from java.util.LinkedList.
 *
 * @author Vitor Enes
 * @param <E>
 */
public class DependencyQueue<E extends DepBox> {

    Node<E> first;
    Node<E> last;

    public DependencyQueue() {
    }

    public DepBox add(E e) {
        Node<E> pred = findPredecessor(e);
        Node<E> succ = findSuccessor(e);
        if (pred == null && succ == null) {
            linkFirst(e);
        } else if (pred == null) {
            linkAfter(e, succ);
        } else if (succ == null) {
            linkBefore(e, pred);
        } else if (succ.next == pred) {
            // insert in between both
            linkBetween(e, pred, succ);
        } else {
            // found cycle: merge all
            merge(e, pred, succ);
        }
        return null;
    }

    /**
     * Find predecessor of e in the queue.
     */
    private Node<E> findPredecessor(E e) {
        Node<E> it = first;

        while (it != null) {
            if (e.before(it.item)) {
                return it;
            }
            it = it.next;
        }

        return null;
    }

    /**
     * Find successor of e in the queue.
     */
    private Node<E> findSuccessor(E e) {
        Node<E> it = last;
        while (it != null) {
            if (it.item.before(e)) {
                return it;
            }
            it = it.prev;
        }

        return null;
    }

    /**
     * Links e as first element.
     */
    private void linkFirst(E e) {
        final Node<E> f = first;
        final Node<E> newNode = new Node<>(null, e, f);
        first = newNode;
        if (f == null) {
            last = newNode;
        } else {
            f.prev = newNode;
        }
    }

    /**
     * Inserts element e before non-null Node succ.
     */
    void linkBefore(E e, Node<E> succ) {
        // assert n != null;
        final Node<E> pred = succ.prev;
        final Node<E> newNode = new Node<>(pred, e, succ);
        succ.prev = newNode;
        if (pred == null) {
            first = newNode;
        } else {
            pred.next = newNode;
        }
    }

    /**
     * Inserts element e after non-null Node pred.
     */
    void linkAfter(E e, Node<E> pred) {
        // assert n != null;
        final Node<E> succ = pred.next;
        final Node<E> newNode = new Node<>(pred, e, succ);
        pred.next = newNode;
        if (succ == null) {
            last = newNode;
        } else {
            succ.prev = newNode;
        }
    }

    /**
     * Inserts element e between Node pred and Node succ.
     */
    void linkBetween(E e, Node<E> pred, Node<E> succ) {
        final Node<E> newNode = new Node<>(pred, e, succ);
        if (pred == null) {
            first = newNode;
        } else {
            pred.next = newNode;
        }
        if (succ == null) {
            last = newNode;
        } else {
            succ.prev = newNode;
        }
    }

    /**
     * Merge e with all from Node a to Node b.
     */
    void merge(E e, Node<E> a, Node<E> b) {
        Node<E> pred = a.prev;
        Node<E> succ = b.next;

        // merge a with e
        e.merge(a.item);

        while (a.next != b.next) {
            // while not pointing to the same element, merge
            a = a.next;
            e.merge(a.item);
        }

        linkBetween(e, pred, succ);
    }

    private static class Node<E> {

        Node<E> prev;
        E item;
        Node<E> next;

        Node(Node<E> prev, E element, Node<E> next) {
            this.prev = prev;
            this.item = element;
            this.next = next;
        }
    }
}
