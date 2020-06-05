# ![HawtJournal](https://github.com/fusesource/hawtjournal/raw/master/src/site/resources/images/project-logo.png)

HawtJournal is a journal storage implementation based on append-only rotating logs and checksummed variable-length records, 
with fixed concurrent reads, full concurrent writes, dynamic batching and "dead" logs cleanup.

## Quickstart

HawtJournal APIs are very simple and intuitive.

First, create and configure the journal:

    Journal journal = new Journal();
    journal.setDirectory(JOURNAL_DIR);

Here, we're basically disabling cleaned-up logs archiving, enabling checksum, setting up a max log file length of 1 megabyte and a max batch size of 10 kilobytes.

Then, open the journal:

    journal.open();

And write some records:

    for (int i = 0; i < writes; i++) {
        boolean sync = i % 2 == 0 ? true : false;
        journal.write(new String("DATA" + i).getBytes("UTF-8"), sync);
    }

You can dynamically write either in async or sync mode: in async mode, writes are batched until either the max batch size is reached, 
the journal is manually synced or closed, or a sync write is executed.

Finally, replay the log by going through the Journal as an iterable object returning record locations:

    for (Location location : journal) {
        byte[] record = journal.read(location);
        // do something
    }

Eventually delete some record:

    journal.delete(location);

Optionally do a manual sync:

    journal.sync();

Compact logs:

    journal.compact();

And close it:

    journal.close();

That's all!

## About data durability

HawtJournal provides three levels of data durability: batch, sync and physical sync.

Batch durability provides the lowest durability guarantees but the greatest performance: data is collected in memory batches and then written on disk at "sync points",
either when a sync write is explicitly requested, a sync call is explicitly executed, or when the max batch size is reached.

Sync durability happens during sync points, providing higher durability guarantees with some performance cost: here, memory batches are written on disk.

Finally, physical sync durability provides the highest durability guarantees at the expense of a greater performance cost, flushing on disk hardware buffers at every sync point: 
disabled by default, it can be enabled by setting _Journal#setPhysicalSync_ true.

## License

Distributed under the [Apache Software License](http://www.apache.org/licenses/LICENSE-2.0.html).
