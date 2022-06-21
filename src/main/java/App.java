import io.delta.standalone.DeltaLog;
import io.delta.standalone.Operation;
import io.delta.standalone.OptimisticTransaction;
import io.delta.standalone.actions.Action;
import io.delta.standalone.actions.AddFile;
import io.delta.standalone.actions.Format;
import io.delta.standalone.actions.Metadata;
import io.delta.standalone.types.*;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.io.File;
import java.util.*;


public class App {

    public static void main(String[] args) {

        DateTime dateTime = new DateTime(2020, 6, 1, 0, 0, 0, 0, DateTimeZone.UTC);

        List<AddFile> addNewFiles = new ArrayList<>();
        String id = UUID.randomUUID().toString();
        String name = "myMeta";
        String desc = "The meta data so it doesn't error";
        Format format = new Format("parquet", new HashMap<>());
        List<String> partitionCols = new ArrayList<>();
        Map<String, String> conf = new HashMap<>();
        Optional<Long> created = Optional.of(System.currentTimeMillis());
        StructType schema = new StructType(new StructField[]{
            new StructField("myString", new StringType()),
            new StructField("myInteger", new IntegerType()),
            new StructField("myDateTime", new TimestampType()),
        });

        Metadata md = new Metadata(id, name, desc, format, partitionCols, conf, created, schema);

        for(int i = 0; i < 2; i++) {
            generateParquetFileFor(dateTime.plusDays(i), addNewFiles);
        }

        DeltaLog log = DeltaLog.forTable(new Configuration(), "data/");
        OptimisticTransaction txn = log.startTransaction();
        List<Action> totalCommitFiles = new ArrayList<>();
        totalCommitFiles.add(md);
        totalCommitFiles.addAll(addNewFiles);

        txn.commit(totalCommitFiles, new Operation(Operation.Name.UPDATE), "Zippy/1.0.0");
    }

    private static void generateParquetFileFor(DateTime dateTime, List<AddFile> addNewFiles) {
        try {
            Schema schema = parseSchema();
            DateTimeFormatter fmt = DateTimeFormat.forPattern("yyyyMMdd");
            Path path = new Path("data/data_" + dateTime.toString(fmt) + ".parquet");

            List<GenericData.Record> recordList = generateRecords(schema, dateTime);

            try (ParquetWriter<GenericData.Record> writer = AvroParquetWriter.<GenericData.Record>builder(path)
                    .withSchema(schema)
                    .withCompressionCodec(CompressionCodecName.SNAPPY)
                    .withRowGroupSize(ParquetWriter.DEFAULT_BLOCK_SIZE)
                    .withPageSize(ParquetWriter.DEFAULT_PAGE_SIZE)
                    .withConf(new Configuration())
                    .withValidation(false)
                    .withDictionaryEncoding(false)
                    .build()) {

                for (GenericData.Record record : recordList) {
                    writer.write(record);
                }
            }
            File file = new File(path.toUri().getPath());
            long len = file.length();
            addNewFiles.add(new AddFile(file.getAbsolutePath(), new HashMap<>(), len, System.currentTimeMillis(), true, null, null));
        } catch (Exception ex) {
            ex.printStackTrace(System.out);
        }
    }

    private static Schema parseSchema() {
        String schemaJson = "{\"namespace\": \"org.myorganization.mynamespace\"," //Not used in Parquet, can put anything
                + "\"type\": \"record\"," //Must be set as record
                + "\"name\": \"myrecordname\"," //Not used in Parquet, can put anything
                + "\"fields\": ["
                + " {\"name\": \"myString\",  \"type\": [\"string\", \"null\"]}"
                + ", {\"name\": \"myInteger\", \"type\": \"int\"}" //Required field
                + ", {\"name\": \"myDateTime\", \"type\": [{\"type\": \"long\", \"logicalType\" : \"timestamp-millis\"}, \"null\"]}"
                + " ]}";

        Schema.Parser parser = new Schema.Parser().setValidate(true);
        return parser.parse(schemaJson);
    }

    private static List<GenericData.Record> generateRecords(Schema schema, DateTime dateTime) {

        List<GenericData.Record> recordList = new ArrayList<>();

        long secondsOfDay = 24 * 60 * 60;

        for(int i = 1; i <= secondsOfDay; i++) {
            DateTime dateTimeTmp = dateTime.withTimeAtStartOfDay().plusSeconds(i-1);

            GenericData.Record record = new GenericData.Record(schema);
            record.put("myInteger", i);
            record.put("myString", i + " hi world of parquet!");
            record.put("myDateTime", dateTimeTmp.getMillis());

            recordList.add(record);
        }

        return recordList;
    }
}