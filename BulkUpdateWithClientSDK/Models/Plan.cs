using System;
using System.Collections.Generic;
using System.IO;
using System.Text;

namespace BulkUpdateWithClientSDK.Models
{
	public class Plan
	{
		public string id { get; set; } = Guid.NewGuid().ToString();
		public string Name { get; set; }

		public string Status { get; set; }

		public string __partitionKey { get; set; }
	}
	public class BulkProcessData
	{
		public List<ItemData> ItemData { get; set; }
		public Operation Operation { get; set; }
	}
	public class ItemData
	{
		public MemoryStream Item { get; set; }
		public string PartitionKey { get; set; }
		public string ItemId { get; set; }
	}
	public enum Operation
	{
		IMPORT,DELETE
	}
}
