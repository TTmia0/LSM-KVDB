package KvStore;

import org.junit.Test;
import service.KvStore;
import service.KvStoreImpl;

import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class KvStoreTest {
    @Test
    public void set() throws IOException {
        KvStore kvStore = new KvStoreImpl("db/", 4, 3);
        for (int i = 0; i < 11; i++) {
            kvStore.set(i + "", i + "");
        }
        for (int i = 0; i < 11; i++) {
            assertEquals(i + "", kvStore.get(i + ""));
        }
        assertNull(kvStore.get(20+""));
        for (int i = 0; i < 11; i++) {
            kvStore.rm(i + "");
        }
        for (int i = 0; i < 11; i++) {
            assertNull(kvStore.get(i + ""));
        }

        kvStore.close();
        kvStore = new KvStoreImpl("db/", 4, 3);
        for (int i = 0; i < 11; i++) {
            assertNull(kvStore.get(i + ""));
        }
        kvStore.close();
    }
}
