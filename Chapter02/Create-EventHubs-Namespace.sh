# Create a resource group (if you haven't created already)
az group create --name StreamingResourceGroup --location UKSouth

# Create an Event Hubs namespace with 10 partitions
az eventhubs namespace create --name StreamingNamespace --resource-group StreamingResourceGroup --location UKSouth --sku Standard --capacity 10
