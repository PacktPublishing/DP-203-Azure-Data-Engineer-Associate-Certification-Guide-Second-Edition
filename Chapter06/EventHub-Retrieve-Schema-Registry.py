# Retrieving A Schema From Schema Registry

# Import the necessary libraries
from azure.identity import DefaultAzureCredential
from azure.schemaregistry import SchemaRegistryClient  

# Create The Schema Registry Client
azureCredential = DefaultAzureCredential()
schema_registry_client = SchemaRegistryClient
(
fully_qualified_namespace=<SCHEMA-NAMESPACE>.servicebus.windows.net,
credential=azureCredential
)

#Retrieve The Schema
with schema_registry_client:
    schema = schema_registry_client.get_schema(schema_id)
    definition = schema.definition
    properties = schema.properties