package ru.vk.itmo.grunskiialexey;

import ru.vk.itmo.Config;
import ru.vk.itmo.Dao;
import ru.vk.itmo.Entry;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.io.UncheckedIOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.NavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static ru.vk.itmo.grunskiialexey.MemorySegmentDao.StorageState.comparator;

public class MemorySegmentDao implements Dao<MemorySegment, Entry<MemorySegment>> {
    private final ExecutorService bgExecutor = Executors.newSingleThreadExecutor();

    private final Arena arena;
    private final Compaction diskStorage;
    private final Path path;
    private final AtomicReference<StorageState> state = new AtomicReference<>(new StorageState());
    private final AtomicBoolean closed = new AtomicBoolean();
    private final ReadWriteLock upsertLock = new ReentrantReadWriteLock();

    public MemorySegmentDao(Config config) throws IOException {
        this.path = config.basePath().resolve("data");
        Files.createDirectories(path);

        arena = Arena.ofShared();

        List<MemorySegment> segments = DiskStorage.loadOrRecover(path, arena);

    }

    static int compare(MemorySegment memorySegment1, MemorySegment memorySegment2) {
        long mismatch = memorySegment1.mismatch(memorySegment2);
        if (mismatch == -1) {
            return 0;
        }

        if (mismatch == memorySegment1.byteSize()) {
            return -1;
        }

        if (mismatch == memorySegment2.byteSize()) {
            return 1;
        }
        byte b1 = memorySegment1.get(ValueLayout.JAVA_BYTE, mismatch);
        byte b2 = memorySegment2.get(ValueLayout.JAVA_BYTE, mismatch);
        return Byte.compare(b1, b2);
    }

    @Override
    public Iterator<Entry<MemorySegment>> get(MemorySegment from, MemorySegment to) {
        StorageState state = this.state.get();

        return Compaction.range(
                getInMemory(state.readStorage, from, to),
                getInMemory(state.writeStorage, from, to),
                state.diskSegmentList,
                from,
                to
        );
    }

    private Iterator<Entry<MemorySegment>> getInMemory(
            NavigableMap<MemorySegment, Entry<MemorySegment>> storage,
            MemorySegment from, MemorySegment to) {
        if (from == null && to == null) {
            return storage.values().iterator();
        }
        if (from == null) {
            return storage.headMap(to).values().iterator();
        }
        if (to == null) {
            return storage.tailMap(from).values().iterator();
        }
        return storage.subMap(from, to).values().iterator();
    }

    @Override
    public void upsert(Entry<MemorySegment> entry) {
        upsertLock.readLock().lock();
        try {
            state.get().writeStorage.put(entry.key(), entry);
        } finally {
            upsertLock.readLock().unlock();
        }
    }

    @Override
    public Entry<MemorySegment> get(MemorySegment key) {
        synchronized (this) {
            Entry<MemorySegment> entry = writeStorage.get(key);
            if (entry != null) {
                if (entry.value() == null) {
                    return null;
                }
                return entry;
            }

            Iterator<Entry<MemorySegment>> iterator = diskStorage.range(Collections.emptyIterator(), key, null);

            if (!iterator.hasNext()) {
                return null;
            }
            Entry<MemorySegment> next = iterator.next();
            if (compare(next.key(), key) == 0) {
                return next;
            }
            return null;
        }
    }

    @Override
    public void flush() {
        bgExecutor.execute(() -> {
            Collection<Entry<MemorySegment>> entries;
            ConcurrentSkipListMap<MemorySegment, Entry<MemorySegment>> writeStorage = state.get().writeStorage;
            if (writeStorage.isEmpty()) {
                return;
            }

            StorageState nextState = new StorageState(writeStorage, new ConcurrentSkipListMap<>(comparator));

            upsertLock.writeLock().lock();
            try {
                state.set(nextState);
            } finally {
                upsertLock.writeLock().unlock();
            }

            MemorySegment newPage;
            entries = writeStorage.values();
            try {
                newPage = DiskStorage.save(path, entries);
            } catch (IOException e) {
                // termination sequence
                throw new UncheckedIOException(e);
            }
            writeStorage = new ConcurrentSkipListMap<>(comparator);

            nextState = new StorageState(new ConcurrentSkipListMap<>(StorageState.comparator), nextState.writeStorage);
            upsertLock.writeLock().lock();
            try {
                state.set(nextState);
            } finally {
                upsertLock.writeLock().unlock();
            }
        });
    }

    @Override
    public void compact() {
        bgExecutor.execute(() -> {
            synchronized (this) {
                try {
                    StorageState state = this.state.get();
                    MemorySegment newPage = Compaction.compact(path, () -> Compaction.range(
                            Collections.emptyIterator(),
                            Collections.emptyIterator(),
                            state.diskSegmentList,
                            null,
                            null
                    ));
                    this.state.set(
                            new StorageState(state.readStorage,
                                    state.writeStorage,
                                    Collections.singletonList(newPage)
                            ));
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            }
        });
    }

    @Override
    public void close() throws IOException {
        if (closed.getAndSet(true)) {
            waitForClose();
            return;
        }

        flush();
        bgExecutor.execute(arena::close);
        bgExecutor.shutdown();
        waitForClose();
    }

    private void waitForClose() throws InterruptedIOException {
        try {
            if (!bgExecutor.awaitTermination(11, TimeUnit.MINUTES)) {
                throw new InterruptedException("Timeout");
            }
        } catch (InterruptedException e) {
            InterruptedIOException exception = new InterruptedIOException("Interrupted or timed out");
            exception.initCause(e);
            throw exception;
        }
    }

    private static class StorageState {
        private static final Comparator<MemorySegment> comparator = MemorySegmentDao::compare;
        private final NavigableMap<MemorySegment, Entry<MemorySegment>> readStorage;
        private final NavigableMap<MemorySegment, Entry<MemorySegment>> writeStorage;
        private final List<MemorySegment> diskSegmentList;
    }
}
