package mayton.db.jfr;

import jdk.jfr.Event;
import jdk.jfr.Name;

@Name("mayton.db.jfr.AddRowBatchEvent")
public class AddRowBatchEvent extends Event {
    public long batchNumber;
}

