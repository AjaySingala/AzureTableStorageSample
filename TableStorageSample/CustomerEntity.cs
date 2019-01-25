using Microsoft.Azure.CosmosDB.Table;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace TableStorageSample
{
    public class CustomerEntity : TableEntity
    {
        public string FirstName { get; set; }
        public string LastName { get; set; }
        public string Email { get; set; }
        public string Phone { get; set; }

        public void Init()
        {
            this.PartitionKey = LastName;
            this.RowKey = FirstName;
        }
    }
}
