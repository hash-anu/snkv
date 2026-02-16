# SNKV + LiteFS Replication Demo (Single-Machine)

This document contains all steps to set up SNKV with LiteFS using a single machine with Consul for lease coordination, including a comprehensive 10 MB replication test.

---

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────────┐
│                      Single Machine Setup                        │
├─────────────────────────────────────────────────────────────────┤
│                                                                   │
│  ┌──────────────┐              ┌──────────────┐                 │
│  │ snkv_writer  │              │ snkv_reader  │                 │
│  │              │              │              │                 │
│  │ kvstore_put()│              │ kvstore_get()│                 │
│  └──────┬───────┘              └───────▲──────┘                 │
│         │                              │                        │
│         ▼                              │                        │
│  ┌─────────────────┐           ┌──────┴───────┐                │
│  │ LiteFS Primary  │           │ LiteFS Replica│                │
│  │ (litefs1.yml)   │───────────▶ (litefs2.yml) │                │
│  │                 │  HTTP      │               │                │
│  │ Port: 20202     │ Streaming  │ Port: 20203   │                │
│  │ FUSE: ./mnt1    │ LTX Frames │ FUSE: ./mnt2  │                │
│  └────────┬────────┘           └───────────────┘                │
│           │                                                      │
│           │ Leader Election                                     │
│           ▼                                                      │
│  ┌──────────────────────┐                                       │
│  │   Consul (port 8500) │                                       │
│  │  Lease Key:          │                                       │
│  │  "snkv-demo/primary" │                                       │
│  └──────────────────────┘                                       │
│                                                                   │
│  Data Flow:                                                      │
│  1. Writer writes to ./mnt1/snkv.db (Primary)                   │
│  2. LiteFS intercepts SQLite WAL writes                         │
│  3. Primary streams LTX frames via HTTP to Replica              │
│  4. Replica applies transactions to ./mnt2/snkv.db              │
│  5. Reader reads replicated data from ./mnt2/snkv.db            │
│                                                                   │
└─────────────────────────────────────────────────────────────────┘
```

---

## Replication Protocol

**LiteFS uses HTTP-based streaming replication:**

- **Protocol**: Custom HTTP/1.1 long-lived connections
- **Format**: LTX (LiteFS Transaction) frames containing SQLite page changes
- **Granularity**: Page-level (typically 4KB SQLite pages)
- **Consistency**: Strong consistency - exact transaction log replay
- **Discovery**: Consul manages leader election and advertise-url discovery
- **Streaming**: Continuous low-latency streaming (not polling)

```
Primary Node (Port 20202)
    │
    ├─ Intercepts SQLite WAL writes
    ├─ Converts to LTX frames
    │
    ▼
HTTP Stream (LTX Protocol)
    │
    ├─ Page-level changes
    ├─ Transaction boundaries
    │
    ▼
Replica Node (Port 20203)
    │
    ├─ Receives LTX frames
    ├─ Applies to local SQLite DB
    └─ Maintains read-only copy
```

---

## Prerequisites

```bash
sudo apt update
sudo apt install -y build-essential git golang-go fuse3 curl unzip
```

Check versions:

```bash
gcc --version
go version
```

---

## 1. Install LiteFS

```bash
git clone https://github.com/superfly/litefs.git
cd litefs
go build ./cmd/litefs
sudo mv litefs /usr/local/bin/
litefs version
```

---

## 2. Install Consul for Lease Coordination

```bash
curl -LO https://releases.hashicorp.com/consul/1.17.3/consul_1.17.3_linux_amd64.zip
unzip consul_1.17.3_linux_amd64.zip
chmod +x consul
sudo mv consul /usr/local/bin/
consul version
```

Start a local Consul agent (Terminal 1):

```bash
consul agent -dev -client=127.0.0.1
```

Leave this running in the background.

---

## 3. Prepare Directories

```bash
mkdir -p ~/litefs-demo/mnt1 ~/litefs-demo/litefs-data1
mkdir -p ~/litefs-demo/mnt2 ~/litefs-demo/litefs-data2
cd ~/litefs-demo
```

Directory structure:
- `mnt1` → Primary FUSE mount point (writer)
- `litefs-data1` → Primary internal storage
- `mnt2` → Replica FUSE mount point (reader)
- `litefs-data2` → Replica internal storage

---

## 4. LiteFS Configuration

### Primary (`litefs1.yml`)

```yaml
fuse:
  dir: ./mnt1

data:
  dir: ./litefs-data1

http:
  addr: ":20202"

lease:
  type: consul
  consul:
    url: "http://127.0.0.1:8500"
    key: "snkv-demo/primary"
  candidate: true
  advertise-url: "http://127.0.0.1:20202"
```

### Replica (`litefs2.yml`)

```yaml
fuse:
  dir: ./mnt2

data:
  dir: ./litefs-data2

http:
  addr: ":20203"

lease:
  type: consul
  consul:
    url: "http://127.0.0.1:8500"
    key: "snkv-demo/primary"
  candidate: false
  advertise-url: "http://127.0.0.1:20203"
```

**Key Configuration Parameters:**

- `http.addr`: Actual HTTP port LiteFS listens on locally
- `advertise-url`: URL advertised to other nodes for replication
- `candidate: true`: Node can become primary
- `candidate: false`: Node is always a replica

---

## 5. SNKV Writer - 10 MB Test (`snkv_writer.c`)

```c
#include "kvstore.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

int main(void) {
    KVStore *kv;
    kvstore_open("./mnt1/snkv.db",
                 &kv,
                 KVSTORE_JOURNAL_WAL);
    
    // Create a 1 MB buffer with repeating pattern
    size_t chunk_size = 1024 * 1024; // 1 MB
    char *data = malloc(chunk_size);
    if (!data) {
        fprintf(stderr, "Failed to allocate memory\n");
        kvstore_close(kv);
        return 1;
    }
    
    // Fill with A-Z repeating pattern for verification
    for (size_t i = 0; i < chunk_size; i++) {
        data[i] = 'A' + (i % 26);
    }
    
    // Write 10 chunks of 1 MB each = 10 MB total
    printf("Writing 10 MB of data...\n");
    for (int i = 0; i < 10; i++) {
        char key[32];
        snprintf(key, sizeof(key), "chunk_%d", i);
        
        kvstore_put(kv, key, strlen(key), data, chunk_size);
        printf("Wrote chunk %d (%zu bytes)\n", i, chunk_size);
    }
    
    printf("Total written: 10 MB\n");
    
    free(data);
    kvstore_close(kv);
    return 0;
}
```

## 6. SNKV Reader - 10 MB Verification (`snkv_reader.c`)

```c
#include "kvstore.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

int main(void) {
    KVStore *kv;
    void *val;
    int n;
    
    kvstore_open("./mnt2/snkv.db",
                 &kv,
                 KVSTORE_JOURNAL_WAL);
    
    printf("Reading 10 MB of data...\n");
    size_t total_read = 0;
    int all_valid = 1;
    size_t expected_size = 1024 * 1024; // 1 MB per chunk
    
    // Read and verify 10 chunks
    for (int i = 0; i < 10; i++) {
        char key[32];
        snprintf(key, sizeof(key), "chunk_%d", i);
        
        if (kvstore_get(kv, key, strlen(key), &val, &n) == 0) {
            total_read += n;
            
            // Check size
            if (n != expected_size) {
                printf("Chunk %d: SIZE MISMATCH (expected %zu, got %d)\n", 
                       i, expected_size, n);
                all_valid = 0;
                sqliteFree(val);
                continue;
            }
            
            // Verify entire pattern byte-by-byte
            char *data = (char *)val;
            int valid = 1;
            int first_error = -1;
            
            for (int j = 0; j < n; j++) {
                char expected = 'A' + (j % 26);
                if (data[j] != expected) {
                    if (first_error == -1) {
                        first_error = j;
                    }
                    valid = 0;
                }
            }
            
            if (valid) {
                printf("Chunk %d: %d bytes [✓ VALID]\n", i, n);
            } else {
                printf("Chunk %d: %d bytes [✗ CORRUPTED - first error at byte %d]\n", 
                       i, n, first_error);
                printf("  Expected: '%c', Got: '%c'\n", 
                       'A' + (first_error % 26), data[first_error]);
                all_valid = 0;
            }
            
            sqliteFree(val);
        } else {
            printf("Chunk %d: [✗ KEY NOT FOUND]\n", i);
            all_valid = 0;
        }
    }
    
    printf("\n========================================\n");
    printf("Total read: %zu bytes (%.2f MB)\n", 
           total_read, total_read / (1024.0 * 1024.0));
    printf("Expected:   %zu bytes (%.2f MB)\n",
           expected_size * 10, (expected_size * 10) / (1024.0 * 1024.0));
    printf("Data integrity: %s\n", all_valid ? "✓ PASS" : "✗ FAIL");
    printf("========================================\n");
    
    kvstore_close(kv);
    return all_valid ? 0 : 1;
}
```

---

## 7. Compile SNKV Programs

```bash
gcc snkv_writer.c -Iinclude libsnkv.a -o snkv_writer
gcc snkv_reader.c -Iinclude libsnkv.a -o snkv_reader
```

**Note**: Make sure `libsnkv.a` and the `include/` directory are in the correct paths.

---

## 8. Running the Demo

### Terminal 1: Start Consul (if not already running)

```bash
consul agent -dev -client=127.0.0.1
```

### Terminal 2: Start LiteFS Primary

```bash
cd ~/litefs-demo
litefs mount -config litefs1.yml
```

Expected output:
```
LiteFS development build
level=INFO msg="litefs mounted" addr=:20202
level=INFO msg="acquired primary lease"
```

### Terminal 3: Start LiteFS Replica

```bash
cd ~/litefs-demo
litefs mount -config litefs2.yml
```

Expected output:
```
LiteFS development build
level=INFO msg="litefs mounted" addr=:20203
level=INFO msg="connected to primary" url=http://127.0.0.1:20202
```

### Terminal 4: Run Writer

```bash
cd ~/litefs-demo
./snkv_writer
```

Expected output:
```
Writing 10 MB of data...
Wrote chunk 0 (1048576 bytes)
Wrote chunk 1 (1048576 bytes)
...
Wrote chunk 9 (1048576 bytes)
Total written: 10 MB
```

### Terminal 5: Run Reader (after a few seconds for replication)

```bash
cd ~/litefs-demo
sleep 2  # Wait for replication
./snkv_reader
```

Expected output:
```
Reading 10 MB of data...
Chunk 0: 1048576 bytes [✓ VALID]
Chunk 1: 1048576 bytes [✓ VALID]
...
Chunk 9: 1048576 bytes [✓ VALID]

========================================
Total read: 10485760 bytes (10.00 MB)
Expected:   10485760 bytes (10.00 MB)
Data integrity: ✓ PASS
========================================
```

---

## 9. Verify Replication

### Check Primary Status

```bash
curl http://127.0.0.1:20202/debug/vars | grep -E "(isPrimary|replica)"
```

### Check Replica Status

```bash
curl http://127.0.0.1:20203/debug/vars | grep -E "(isPrimary|replica)"
```

### Verify Database Files

```bash
# Primary
ls -lh mnt1/snkv.db*

# Replica
ls -lh mnt2/snkv.db*
```

Both should show:
- `snkv.db` - Main database file
- `snkv.db-wal` - Write-Ahead Log
- `snkv.db-shm` - Shared memory file

---

## 10. Test Replication Lag

```bash
time ./snkv_writer && time ./snkv_reader
```

This will show how quickly data replicates from primary to replica.

---

## Success Criteria

✅ **10 MB of data written** to primary (`./mnt1/snkv.db`)  
✅ **10 MB of data replicated** to replica (`./mnt2/snkv.db`)  
✅ **All 10,485,760 bytes verified** byte-by-byte with correct pattern  
✅ **Consul lease management** working (primary election)  
✅ **HTTP streaming replication** functioning between nodes  

---

## Troubleshooting

### Port Conflict Error

```
ERROR: cannot open http server: listen tcp :20202: bind: address already in use
```

**Solution**: Make sure `http.addr` is specified in both config files with different ports.

### FUSE Mount Failed

```
ERROR: cannot mount fuse: permission denied
```

**Solution**: 
```bash
sudo usermod -aG fuse $USER
# Log out and log back in
```

### Replica Not Connecting

**Check**:
1. Primary is running and healthy
2. Consul is running
3. Firewall allows ports 20202, 20203, 8500
4. Check logs for connection errors

---

## Architecture Summary

| Component | Purpose | Port/Path |
|-----------|---------|-----------|
| Consul | Leader election & service discovery | 8500 |
| LiteFS Primary | Write node, streams changes | 20202, ./mnt1 |
| LiteFS Replica | Read-only node, receives changes | 20203, ./mnt2 |
| SNKV Writer | Writes data via kvstore API | - |
| SNKV Reader | Reads data via kvstore API | - |

**Data Path:**
```
snkv_writer → SQLite (mnt1) → LiteFS Primary → HTTP Stream → 
LiteFS Replica → SQLite (mnt2) → snkv_reader
```

---

## Useful LiteFS Options

From `litefs.yml`:

- **`fuse.dir`** - FUSE mount point for application access
- **`data.dir`** - Internal LiteFS storage directory
- **`http.addr`** - HTTP server listening address
- **`lease.type`** - Lease mechanism: `consul`, `static`, or `none`
- **`lease.consul.url`** - Consul agent URL
- **`lease.consul.key`** - Consul key for lease coordination
- **`lease.candidate`** - Whether node can become primary
- **`lease.advertise-url`** - URL advertised to cluster for replication
- **`exec`** - Command to run after LiteFS is ready
- **`proxy`** - Optional HTTP proxy (for web applications)

---

## Next Steps

- **Failover Testing**: Stop primary, promote replica by changing `candidate: true`
- **Performance Testing**: Measure replication lag under load
- **Multi-Node Setup**: Deploy on separate machines/containers
- **Backup Strategy**: Configure LiteFS backup to S3/cloud storage
- **Monitoring**: Set up Prometheus metrics collection

---

## References

- [LiteFS Documentation](https://fly.io/docs/litefs/)
- [Consul Documentation](https://www.consul.io/docs)
- [SQLite WAL Mode](https://www.sqlite.org/wal.html)
