package com.uber.athenax.backend.catalog;

import com.uber.athenax.vm.api.tables.AthenaXTableCatalog;
import com.uber.athenax.vm.api.tables.AthenaXTableCatalogProvider;
import org.apache.flink.table.api.*;
import org.apache.flink.table.catalog.ExternalCatalog;
import org.apache.flink.table.catalog.ExternalCatalogTable;
import org.apache.flink.table.descriptors.*;

import java.util.*;

public class OppoTableCatalogProvider implements AthenaXTableCatalogProvider {
    private AthenaXTableCatalog catalog = new OppoTableCatalog();

    @Override
    public Map<String, AthenaXTableCatalog> getInputCatalog(String cluster) {
        return Collections.singletonMap("dw", catalog);
    }

    @Override
    public AthenaXTableCatalog getOutputCatalog(String cluster, List<String> outputs) {
        return catalog;
    }

    static class OppoTableCatalog implements AthenaXTableCatalog {
        @Override
        public ExternalCatalogTable getTable(String tableName) throws TableNotExistException {
            if ("source".equals(tableName)) {
                ConnectorDescriptor connectorDescriptor = new FileSystem().path("/tmp/csv/test.csv");
                TableSchema schema = new TableSchemaBuilder()
                        .field("id", Types.INT())
                        .field("name", Types.STRING())
                        .build();
                FormatDescriptor formatDescriptor = new Csv()
                        .field("id", Types.INT())
                        .field("name", Types.STRING());
                ExternalCatalogTable source = ExternalCatalogTable.builder(connectorDescriptor)
                        .withFormat(formatDescriptor)
                        .withSchema(new Schema().schema(schema))
                        .inAppendMode()
                        .asTableSource();
                return source;
            } else if ("target".equals(tableName)) {
                ConnectorDescriptor connectorDescriptor1 = new FileSystem().path("/tmp/csv/output.csv");
                TableSchemaBuilder build1 = new TableSchemaBuilder();
                TableSchema schema1 = build1.field("id", Types.INT())
                        .field("name", Types.STRING())
                        .build();
                FormatDescriptor formatDescriptor1 = new Csv()
                        .field("id", Types.INT())
                        .field("name", Types.STRING());
                ExternalCatalogTable sink = ExternalCatalogTable.builder(connectorDescriptor1)
                        .withFormat(formatDescriptor1)
                        .withSchema(new Schema().schema(schema1))
                        .inAppendMode()
                        .asTableSink();
                return sink;
            }

            throw new TableNotExistException("dw", tableName);
        }

        @Override
        public List<String> listTables() {
            return Collections.singletonList("target");
        }

        @Override
        public ExternalCatalog getSubCatalog(String dbName) throws CatalogNotExistException {
            throw new CatalogNotExistException(dbName);
        }

        @Override
        public List<String> listSubCatalogs() {
            return new ArrayList<>();
        }
    }
}
