using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using Microsoft.Azure; // Namespace for CloudConfigurationManager
using Microsoft.Azure.Storage; // Namespace for StorageAccounts
using Microsoft.Azure.CosmosDB.Table; // Namespace for Table storage types

namespace TableStorageSample
{
    class Program
    {
        // The connection string for the storage account.
        static CloudStorageAccount storageAccount;
        // The table client.
        static CloudTableClient tableClient;
        // Reference to the table.
        static CloudTable table;

        static void Main(string[] args)
        {
            // Parse the connection string and return a reference to the storage account.
            storageAccount = CloudStorageAccount.Parse(
                CloudConfigurationManager.GetSetting("StorageConnectionString"));

            // Create the table client.
            tableClient = storageAccount.CreateCloudTableClient();

            // Retrieve a reference to the table.
            table = tableClient.GetTableReference("people");

            // Create the table if it doesn't exist.
            table.CreateIfNotExists();

            CreateSingleEntity();
            //InsertBatch();
            //RetrieveEntities("Smith");
            //RetrieveSingleEntity("Ajay","Singala");
            //ReplaceEntity();
            //UpsertEntity();
            //DeleteEntity("Fred", "Jones");
            //DeleteTable();
        }

        private static void CreateSingleEntity()
        {
            // Add entity to table.
            var customer = new CustomerEntity()
            {
                FirstName = "Ajay",
                LastName = "Singala",
                Email = "ajay.singala@gmail.com",
                Phone = "55544-33322"
            };
            customer.Init();        // Set the Partition Key and Row Key values.

            TableOperation insertOperation = TableOperation.Insert(customer);
            table.Execute(insertOperation);
        }

        private static void InsertBatch()
        {
            // Insert a batch of entities.
            // All entities in a given batch must have the same Partition Key.
            var customer2 = new CustomerEntity()
            {
                FirstName = "John",
                LastName = "Smith",
                Email = "john.smith@gmail.com",
                Phone = "555-66-8976"
            };
            customer2.Init();        // Set the Partition Key and Row Key values.

            var customer3 = new CustomerEntity()
            {
                FirstName = "Mary",
                LastName = "Smith",
                Email = "mary.jane@gmail.com",
                Phone = "555-44-5678"
            };
            customer3.Init();        // Set the Partition Key and Row Key values.

            // Create the batch operation.
            TableBatchOperation batchOperation = new TableBatchOperation();

            // Add both customer entities to the batch insert operation.
            batchOperation.Insert(customer2);
            batchOperation.Insert(customer3);

            // Execute the batch operation.
            table.ExecuteBatch(batchOperation);
        }

        private static void RetrieveEntities(string lastname)
        {
            // Retrieve all entries in a Partition for Lastname = "Smith".
            // Construct the query operation for all customer entities where PartitionKey="Smith".
            TableQuery<CustomerEntity> query = new TableQuery<CustomerEntity>()
                .Where(TableQuery.GenerateFilterCondition(
                    "PartitionKey", QueryComparisons.Equal, lastname));

            // Print the fields for each customer.
            foreach (CustomerEntity entity in table.ExecuteQuery(query))
            {
                Console.WriteLine("{0}, {1}\t{2}\t{3}",
                    entity.PartitionKey, entity.RowKey,
                    entity.Email, entity.Phone);
            }
        }

        private static void RetrieveSingleEntity(string firstName, string lastName)
        {
            // Create a retrieve operation that takes a customer entity.
            TableOperation retrieveOperation = 
                TableOperation.Retrieve<CustomerEntity>(lastName, firstName);

            // Execute the retrieve operation.
            TableResult retrievedResult = table.Execute(retrieveOperation);

            // Print the phone number of the result.
            if (retrievedResult.Result != null)
            {
                Console.WriteLine("{0}\t{1}",
                    ((CustomerEntity)retrievedResult.Result).Email,
                    ((CustomerEntity)retrievedResult.Result).Phone);
            }
            else
            {
                Console.WriteLine("The phone number could not be retrieved.");
            }
        }

        private static void ReplaceEntity()
        {
            // Create a retrieve operation that takes a customer entity.
            TableOperation retrieveOperation = 
                TableOperation.Retrieve<CustomerEntity>("Smith", "John");

            // Execute the operation.
            TableResult retrievedResult = table.Execute(retrieveOperation);

            // Assign the result to a CustomerEntity object.
            CustomerEntity updateEntity = (CustomerEntity)retrievedResult.Result;

            if (updateEntity != null)
            {
                // Change the phone number.
                updateEntity.Phone = "425-555-1004";

                // Create the Replace TableOperation.
                TableOperation updateOperation = TableOperation.Replace(updateEntity);

                // Execute the operation.
                table.Execute(updateOperation);

                Console.WriteLine("Entity updated.");
                RetrieveSingleEntity("John", "Smith");
            }
            else
            {
                Console.WriteLine("Entity could not be retrieved.");
            }
        }

        private static void UpsertEntity()
        {
            // Create a customer entity.
            CustomerEntity customer3 = new CustomerEntity
            {
                LastName = "Jones",
                FirstName = "Fred",
                Email = "Fred@gmail.com",
                Phone = "425-555-0106"
            };
            customer3.Init();        // Set the Partition Key and Row Key values.

            // Create the TableOperation object that inserts the customer entity.
            TableOperation insertOperation = TableOperation.Insert(customer3);

            // Execute the operation.
            table.Execute(insertOperation);

            RetrieveEntities("Jones");

            // Create another customer entity with the same partition key and row key.
            // We've already created a 'Fred Jones' entity and saved it to the
            // 'people' table, but here we're specifying a different value for the
            // PhoneNumber property.
            CustomerEntity customer4 = new CustomerEntity
            {
                LastName = "Jones",
                FirstName = "Fred",
                Email = "Fred@yahoo.com",
                Phone = "425-555-0107"
            };
            customer4.Init();        // Set the Partition Key and Row Key values.

            // Create the InsertOrReplace TableOperation.
            TableOperation insertOrReplaceOperation = 
                TableOperation.InsertOrReplace(customer4);

            // Execute the operation. Because a 'Fred Jones' entity already exists in the
            // 'people' table, its property values will be overwritten by those in this
            // CustomerEntity. If 'Fred Jones' didn't already exist, the entity would be
            // added to the table.
            table.Execute(insertOrReplaceOperation);

            RetrieveEntities("Jones");
        }

        private static void DeleteEntity(string firstname, string lastname)
        {
            // Create a retrieve operation that expects a customer entity.
            TableOperation retrieveOperation = 
                TableOperation.Retrieve<CustomerEntity>(lastname, firstname);

            // Execute the operation.
            TableResult retrievedResult = table.Execute(retrieveOperation);

            // Assign the result to a CustomerEntity.
            CustomerEntity deleteEntity = (CustomerEntity)retrievedResult.Result;

            // Create the Delete TableOperation.
            if (deleteEntity != null)
            {
                TableOperation deleteOperation = TableOperation.Delete(deleteEntity);

                // Execute the operation.
                table.Execute(deleteOperation);

                Console.WriteLine("Entity {0} {1} deleted.",
                    firstname, lastname);

            }
            else
            {
                Console.WriteLine("Could not retrieve the entity.");
            }
        }

        private static void DeleteTable()
        {
            // Delete the table it if exists.
            table.DeleteIfExists();
            Console.WriteLine("Table Deleted");
        }
    }
}
