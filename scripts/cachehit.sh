#! /usr/bin/env stap
global total_bytes, disk_bytes, counter

probe vfs.read.return {
  if (bytes_read>0) {
    if (devname=="N/A") {
    } else {
      total_bytes += bytes_read
    }
  }
}
probe ioblock.request
{
    if (rw == 0 && size > 0)
    {
        if (devname=="N/A") { 
        } else {
          disk_bytes += size
        }
    }

}

# print VFS hits and misses every 5 second, plus the hit rate in %
probe timer.s(1) {
#    if (counter%15 == 0) {
#        printf ("\n%18s %18s %10s %10s\n", 
#            "Cache Reads (KB)", "Disk Reads (KB)", "Miss Rate", "Hit Rate")
#    }
    cache_bytes = total_bytes - disk_bytes
    if (cache_bytes < 1)
      cache_bytes = 1
    counter++
    hitrate =  10000 * cache_bytes / (cache_bytes+disk_bytes)
    missrate = 10000 * disk_bytes / (cache_bytes+disk_bytes)
    printf ("%18d %18d %6d.%02d%% %6d.%02d%%\n",
        cache_bytes/1024, disk_bytes/1024,
        missrate/100, missrate%100, hitrate/100, hitrate%100)
    total_bytes = 0
    disk_bytes = 0
}

