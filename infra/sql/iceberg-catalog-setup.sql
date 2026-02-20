CREATE TABLE iceberg_namespace_properties (
    catalog_name VARCHAR(255) NOT NULL,
    namespace VARCHAR(255) NOT NULL,
    property_key VARCHAR(500) NOT NULL,
    property_value VARCHAR(500),
    PRIMARY KEY (catalog_name, namespace, property_key)
);

CREATE TABLE iceberg_tables (
    catalog_name VARCHAR(255) NOT NULL,
    table_namespace VARCHAR(255) NOT NULL,
    table_name VARCHAR(255) NOT NULL,
    metadata_location VARCHAR(500),
    previous_metadata_location VARCHAR(500),
    PRIMARY KEY (catalog_name, table_namespace, table_name)
);