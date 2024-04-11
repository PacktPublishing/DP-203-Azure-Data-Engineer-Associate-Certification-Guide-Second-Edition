# Registering A Schema With Schema Registry

# Import the necessary libraries
from azure.schemaregistry import SchemaRegistryClient
from azure.identity import DefaultAzureCredential

# Define Your Schema
sampleSchema = 
"""{
    "namespace": "com.azure.sampleschema.avro",
    "type": "record",
    "name": "Trip",
    "fields":  [
                {"name": "tripId", "type": "string"},
                {"name": "startLocation", "type": "string"},
                {"name": "endLocation", "type": "string  "}
               ]
}"""

# Create the Schema Registry Client
azureCredential = DefaultAzureCredential()
schema_registry_client = SchemaRegistryClient
    (
        fully_qualified_namespace=<SCHEMA-NAMESPACE>.servicebus.windows.net,
        credential=azureCredential
    )

# Register the Schema
with schema_registry_client:
    schema_properties = schema_registry_client.register_schema
    (
    <SCHEMA_GROUPNAME>,
    <SCHEMA_NAME>,
    sampleSchema,
    "Avro  "
    )

# Get The Schema ID
schema_id = schema_properties.id
