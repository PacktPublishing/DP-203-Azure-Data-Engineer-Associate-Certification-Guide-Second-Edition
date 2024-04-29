# Use this script to set your subscription
# Create a resource group in to Azure before running the other Powershell scripts in this chapter.

$subscriptionName="<INSERT SUBSCRIPTION NAME>"
$resourceGroup = "<INSERT RESOURCE GROUP NAME>"
$region = "<INSERT REGION NAME>"

az account set --subscription $subscriptionName

az group create --name $resourceGroup --location $region