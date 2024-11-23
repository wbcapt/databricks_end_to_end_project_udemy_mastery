# Databricks notebook source
# MAGIC %md
# MAGIC #Connecting to data
# MAGIC ##Without Unity Catalog
# MAGIC
# MAGIC Recommended approach is to use a service principal in Azure by registering an app in Microsoft Entra ID (formerly AAD) and then also create a key vault
# MAGIC
# MAGIC Pre-req:
# MAGIC 1. Ability to create a service princple app in Microsoft Entra ID
# MAGIC 2. Ability to store the secret value in a key vault you create in Azure as a secret
# MAGIC 3. ability to create a secret scope in Azure Databricks
# MAGIC 4. ADLS Storage account with Enable Soft Delete for Blobs and Containers disabled
# MAGIC
# MAGIC IMPORTANT: 
# MAGIC 1. remember to write down the secret value of your app service principle when you create it in step 2 as this functions as the password and you can only view it once
# MAGIC 2. remember to write down the secret scope (the name of the secret scope) you create in databricks per step 4
# MAGIC 3. Enable soft delete for blobs and containers should be disabled on your storage account. Go to Data management >> data protection on the blade (side menu) for the storage account
# MAGIC
# MAGIC Step by step:
# MAGIC 1. Go to Microsoft Entra ID
# MAGIC 2. Create an app registration
# MAGIC 	a. Note down the client ID and tenant ID (both found in overview tab)
# MAGIC 	b. Secret Key create one and note it down. The secret gets generated only once so it's important to remember this one
# MAGIC 		i. The secret value is the client's (application service principal ie what you registered) password for your application
# MAGIC 3. Store the client secret of your principal as a secret in a key vault. Remember to use vault access policy as opposed to RBAC because for some reason creating a secret may not be supported by RBAC in key vault
# MAGIC 4. Go to databricks https:/[databricks-instance]#secrets/createScope and fill in the databricks portion
# MAGIC 	a. For the DNS specified, this is the URI of your key vault which is found on the properties tab of your key vault
# MAGIC 	b. Your resource ID is the resource ID listed
# MAGIC     c. click create
# MAGIC
# MAGIC 5. Connect through databricks using the code below
# MAGIC
# MAGIC
# MAGIC general syntax can be borrowed from here: https://learn.microsoft.com/en-us/azure/databricks/connect/storage/tutorial-azure-storage#--step-3-grant-the-service-principal-access-to-azure-data-lake-storage-gen2 

# COMMAND ----------

# MAGIC %md
# MAGIC ##general syntax for connection per microsoft
# MAGIC
# MAGIC
# MAGIC service_credential = dbutils.secrets.get(scope="[scope]",key="[service-credential-key]")
# MAGIC
# MAGIC spark.conf.set("fs.azure.account.auth.type.[storage-account].dfs.core.windows.net", "OAuth")
# MAGIC spark.conf.set("fs.azure.account.oauth.provider.type.[storage-account].dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
# MAGIC spark.conf.set("fs.azure.account.oauth2.client.id.[storage-account].dfs.core.windows.net", "[application-id]")
# MAGIC spark.conf.set("fs.azure.account.oauth2.client.secret.[storage-account].dfs.core.windows.net", service_credential)
# MAGIC spark.conf.set("fs.azure.account.oauth2.client.endpoint.[storage-account].dfs.core.windows.net", "https://login.microsoftonline.com/[directory-id]/oauth2/token")
# MAGIC
# MAGIC where: 
# MAGIC
# MAGIC **scope** = Azure Key Vault backed secret scope
# MAGIC **service credential key** = name of key containing the client secret
# MAGIC **storage account** = name of azure storage account
# MAGIC **application id** = application client ID registered as service principle in Microsoft Entra ID
# MAGIC **directory id** = Directory (tenant) ID for the microsoft entra ID application/service principle. (the unique directory ID created for the service principle created within Entra)

# COMMAND ----------

# MAGIC %md
# MAGIC service_credential = dbutils.secrets.get(scope="azure-key-vault-secret-scope-new",key="databricks-free-trial-secret")
# MAGIC
# MAGIC spark.conf.set("fs.azure.account.auth.type.datbrudmymstryprjstg.dfs.core.windows.net", "OAuth")
# MAGIC spark.conf.set("fs.azure.account.oauth.provider.type.datbrudmymstryprjstg.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
# MAGIC spark.conf.set("fs.azure.account.oauth2.client.id.datbrudmymstryprjstg.dfs.core.windows.net", "638478f8-a4fc-41be-9e9d-b3abe0e988ff")
# MAGIC spark.conf.set("fs.azure.account.oauth2.client.secret.datbrudmymstryprjstg.dfs.core.windows.net", service_credential)
# MAGIC spark.conf.set("fs.azure.account.oauth2.client.endpoint.datbrudmymstryprjstg.dfs.core.windows.net", "https://login.microsoftonline.com/98898401-cb66-46bd-9820-017f5f98d1c6/oauth2/token")

# COMMAND ----------

#configuring connection
service_credential = dbutils.secrets.get(scope="azure-key-vault-secret-scope-new",key="databricks-free-trial-secret")
storage_account = 'datbrudmymstryprjstg'


spark.conf.set(f"fs.azure.account.auth.type.{storage_account}.dfs.core.windows.net", "OAuth")
spark.conf.set(f"fs.azure.account.oauth.provider.type.{storage_account}.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set(f"fs.azure.account.oauth2.client.id.{storage_account}.dfs.core.windows.net", "638478f8-a4fc-41be-9e9d-b3abe0e988ff")
spark.conf.set(f"fs.azure.account.oauth2.client.secret.{storage_account}.dfs.core.windows.net", service_credential)
spark.conf.set(f"fs.azure.account.oauth2.client.endpoint.{storage_account}.dfs.core.windows.net", "https://login.microsoftonline.com/98898401-cb66-46bd-9820-017f5f98d1c6/oauth2/token")

# COMMAND ----------

# MAGIC %md
# MAGIC #Test your connection
# MAGIC
# MAGIC Pre-req: 
# MAGIC 1. know the full path of your file in your blob storage container
# MAGIC 2. Ensure that enable soft delete for blobs and containers is not checked on your storage account. To check, go to your storage account >> data management >> data protection >> click save if you make changes
# MAGIC
# MAGIC
# MAGIC The general syntax for the below is:
# MAGIC df = (spark.read.format('csv')
# MAGIC     .option('header','true')
# MAGIC     .load("abfss://[your_container_name]@d[your_storage_account_name].dfs.core.windows.net/[full_file_path].[file extension]")
# MAGIC )
# MAGIC

# COMMAND ----------

storage_container = 'test-connection-container'
#dbutils.fs.ls('abfss://test-connection-container@datbrudmymstryprjstg.dfs.core.windows.net/')

dbutils.fs.ls(f'abfss://{storage_container}@{storage_account}.dfs.core.windows.net/')

# COMMAND ----------

#specify the dataframe
df = (spark.read.format('csv')
    .option('header','true')
    .load("abfss://test-connection-container@datbrudmymstryprjstg.dfs.core.windows.net/Online_Retail.csv")
)

# COMMAND ----------

display(df)
