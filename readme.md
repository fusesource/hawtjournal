# HawtJournal

HawtJournal is a journal storage implementation based on append-only rotating logs and checksummed variable-length records, 
with fixed concurrent reads, full concurrent writes, dynamic batching and "dead" logs cleanup.

## Quickstart

HawtJournal APIs are very simple and intuitive.

First, create and configure the journal:

    Journal journal = new Journal();
    journal.setDirectory(JOURNAL_DIR);
    journal.setArchiveFiles(false);
    journal.setChecksum(true);
    journal.setMaxFileLength(1024 * 1024);
    journal.setMaxWriteBatchSize(1024 * 10);

Here, we're basically disabling cleaned-up logs archiving, enabling checksum, setting up a max log file length of 1 megabyte and a max batch size of 10 kilobytes.

Then, open the journal:

    journal.open();

And write some records:

    for (int i = 0; i < writes; i++) {
        boolean sync = i % 2 == 0 ? true : false;
        journal.write(ByteBuffer.wrap(new String("DATA" + i).getBytes("UTF-8")), sync);
    }

You can dynamically write either in async or sync mode: in async mode, writes are batched until either the max batch size is reached, 
the journal is manually synced or closed, or a sync write is executed.

Finally, replay the log by going through the Journal as an iterable object returning record locations:

    for (Location location : journal) {
        ByteBuffer record = journal.read(location);
        // do something
    }

Eventually delete some record:

    journal.delete(location);

Cleanup dead logs:

    journal.cleanup();

And close it:

    journal.close();

That's all!

## License

Distributed under the [Apache Software License](http://www.apache.org/licenses/LICENSE-2.0.html).
