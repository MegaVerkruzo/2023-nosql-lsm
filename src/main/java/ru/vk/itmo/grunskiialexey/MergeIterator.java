package ru.vk.itmo.grunskiialexey;

import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.PriorityQueue;

public class MergeIterator<T> implements Iterator<T> {

    private final PriorityQueue<PeekIterator<T>> priorityQueue;
    private final Comparator<T> comparator;

    private static class PeekIterator<T> implements Iterator<T> {

        public final int id;
        private final Iterator<T> delegate;
        private T peek;

        private PeekIterator(int id, Iterator<T> delegate) {
            this.id = id;
            this.delegate = delegate;
        }

        @Override
        public boolean hasNext() {
            if (peek == null) {
                return delegate.hasNext();
            }
            return true;
        }

        @Override
        public T next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            T element = peek();
            this.peek = null;
            return element;
        }

        private T peek() {
            if (peek == null) {
                if (!delegate.hasNext()) {
                    return null;
                }
                peek = delegate.next();
            }
            return peek;
        }
    }

    PeekIterator<T> peek;

    public MergeIterator(Collection<Iterator<T>> iterators, Comparator<T> comparator) {
        this.comparator = comparator;
        Comparator<PeekIterator<T>> peekComp = (o1, o2) -> comparator.compare(o1.peek(), o2.peek());
        priorityQueue = new PriorityQueue<>(
                iterators.size(),
                peekComp.thenComparing(o -> -o.id)
        );

        int id = 0;
        for (Iterator<T> iterator : iterators) {
            if (iterator.hasNext()) {
                priorityQueue.add(new PeekIterator<>(id++, iterator));
            }
        }
    }

    private PeekIterator<T> peek() {
        // while exists take
        while (peek == null) {
            // Getting peek for one file or in-memory
            peek = priorityQueue.poll();
            if (peek == null) {
                return null;
            }

            while (true) {
                // getting first thing
                PeekIterator<T> next = priorityQueue.peek();
                if (next == null) {
                    break;
                }

                int compare = comparator.compare(peek.peek(), next.peek());
                if (compare == 0) {
                    skipEqualsElement();
                } else {
                    break;
                }
            }

            if (peek.peek() == null) {
                peek = null;
                continue;
            }

            peek = skipRemovedEntry(peek);
        }

        return peek;
    }

    private void skipEqualsElement() {
        PeekIterator<T> poll = priorityQueue.poll();
        if (poll != null) {
            poll.next();
            if (poll.hasNext()) {
                priorityQueue.add(poll);
            }
        }
    }

    private PeekIterator<T> skipRemovedEntry(PeekIterator<T> peek) {
        if (skip(peek.peek())) {
            peek.next();
            if (peek.hasNext()) {
                priorityQueue.add(peek);
            }
            return null;
        }
        return peek;
    }

    protected boolean skip(T t) {
        return false;
    }

    @Override
    public boolean hasNext() {
        return peek() != null;
    }

    @Override
    public T next() {
        PeekIterator<T> peek = peek();
        if (peek == null) {
            throw new NoSuchElementException();
        }
        T next = peek.next();
        this.peek = null;
        if (peek.hasNext()) {
            priorityQueue.add(peek);
        }
        return next;
    }
}
