package ru.vk.itmo.grunskiialexey;

import ru.vk.itmo.Config;
import ru.vk.itmo.Dao;
import ru.vk.itmo.Entry;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.channels.InterruptedByTimeoutException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.NavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class MemorySegmentDao implements Dao<MemorySegment, Entry<MemorySegment>> {
    private final ExecutorService bgExecutor = Executors.newSingleThreadExecutor();

    private final Comparator<MemorySegment> comparator = MemorySegmentDao::compare;
    private final NavigableMap<MemorySegment, Entry<MemorySegment>> storage = new ConcurrentSkipListMap<>(comparator);
    private final Arena arena;
    private final Compaction diskStorage;
    private final Path path;
    private final AtomicBoolean closed = new AtomicBoolean();

    public MemorySegmentDao(Config config) throws IOException {
        this.path = config.basePath().resolve("data");
        Files.createDirectories(path);

        arena = Arena.ofShared();

        this.diskStorage = new Compaction(DiskStorage.loadOrRecover(path, arena));
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
        return diskStorage.range(getInMemory(from, to), from, to);
    }

    private Iterator<Entry<MemorySegment>> getInMemory(MemorySegment from, MemorySegment to) {
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
        storage.put(entry.key(), entry);
    }

    @Override
    public Entry<MemorySegment> get(MemorySegment key) {
        Entry<MemorySegment> entry = storage.get(key);
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

    @Override
    public void flush() throws IOException {
        bgExecutor.execute(() -> {
            synchronized (this) {
                if (!storage.isEmpty()) {
                    try {
                        DiskStorage.save(path, storage.values());
                    } catch (IOException e) {
                        throw new UncheckedIOException(e);
                    }
                }
            }

        });

    }

    @Override
    public void compact() throws IOException {
        bgExecutor.execute(() -> {
            synchronized (this) {
                try {
                    diskStorage.compact(path, storage);
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            }

        });
    }

    @Override
    public void close() throws IOException {
        if (!arena.scope().isAlive()) {
            return;
        }

        flush();
        bgExecutor.shutdown();
        try {
            if (!bgExecutor.awaitTermination(7, TimeUnit.MINUTES)) {
                throw new InterruptedByTimeoutException();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        arena.close();
    }
}
