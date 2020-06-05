using System;
using System.IO;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using System.Collections.Generic;
using BulkUpdateWithClientSDK.Models;
using System.Collections.Concurrent;
using System.Text;
using System.Linq;

namespace BulkUpdateWithClientSDK
{
	public static class Function1
	{
		[FunctionName("InsertPlans")]
		public static async Task<IActionResult> Run(
			[HttpTrigger(AuthorizationLevel.Anonymous, "post", Route = null)] HttpRequest req,
			ILogger log)
		{
			CosmosRepo cosmosRepo = new CosmosRepo(log);

			List<Plan> plans = CreateItems();
			BulkProcessData dataToImport = new BulkProcessData();
			dataToImport.ItemData = new List<ItemData>();
			foreach (Plan plan in plans)
			{
				dataToImport.ItemData.Add(new ItemData
				{
					Item = new MemoryStream(Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(plan))),// JsonConvert.SerializeObject(s),
					ItemId = plan.id,
					PartitionKey = plan.__partitionKey

				});

			}
			dataToImport.Operation = Operation.IMPORT;
			await cosmosRepo.BulkProcessData(dataToImport);
			string responseMessage = $"Total Non available plans count, {plans.Where(m=>m.Status!="AVAILABLE").Count()}";
			log.LogInformation(responseMessage);
			return new OkObjectResult(responseMessage);
		}

		[FunctionName("UpdatePlans")]
		public static async Task<IActionResult> UpdatePlans(
			[HttpTrigger(AuthorizationLevel.Anonymous, "post", Route = null)] HttpRequest req,
			ILogger log)
		{
			CosmosRepo cosmosRepo = new CosmosRepo(log);

			IEnumerable<Plan> plans = await cosmosRepo.GetItemsAsync<Plan>("select * from c", null);
			log.LogInformation($"Total Non available plans count before update, {plans.Where(m => m.Status != "AVAILABLE").Count()}");


			//Update status to Available
			BulkProcessData dataToUpdate = new BulkProcessData();
			dataToUpdate.ItemData = new List<ItemData>();
			foreach (Plan plan in plans)
			{
				plan.Status = "AVAILABLE";
				dataToUpdate.ItemData.Add(new ItemData
				{
					Item = new MemoryStream(Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(plan))),// JsonConvert.SerializeObject(s),
					ItemId = plan.id,
					PartitionKey = plan.__partitionKey

				});

			}
			dataToUpdate.Operation = Operation.IMPORT;
			await cosmosRepo.BulkProcessData(dataToUpdate);
			string responseMessage = $"Total Non available plans count after update, {plans.Where(m => m.Status != "AVAILABLE").Count()}";
			log.LogInformation(responseMessage);
			return new OkObjectResult(responseMessage);
		}

		[FunctionName("DeletePlans")]
		public static async Task<IActionResult> DeletePlans(
			[HttpTrigger(AuthorizationLevel.Anonymous, "delete", Route = null)] HttpRequest req,
			ILogger log)
		{
			CosmosRepo cosmosRepo = new CosmosRepo(log);

			IEnumerable<Plan> plans = await cosmosRepo.GetItemsAsync<Plan>("select * from c", null);
			log.LogInformation($"Total plans to delete, {plans.Count()}");


			//Update status to Available
			BulkProcessData dataToDelete = new BulkProcessData();
			dataToDelete.ItemData = new List<ItemData>();
			foreach (Plan plan in plans)
			{
				dataToDelete.ItemData.Add(new ItemData
				{
					Item = new MemoryStream(Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(plan))),// JsonConvert.SerializeObject(s),
					ItemId = plan.id,
					PartitionKey = plan.__partitionKey

				});

			}
			dataToDelete.Operation = Operation.DELETE;
			await cosmosRepo.BulkProcessData(dataToDelete);
			string responseMessage = $"Total plans to delete, {plans.Count()}";
			log.LogInformation(responseMessage);
			return new OkObjectResult(responseMessage);
		}

		private static List<Plan> CreateItems()
		{
			List<Plan> plans = new List<Plan>();
			var random = new Random();
			for (int i = 0; i < 1000; i++)
			{
				string status = i > 500 ? "BOOKED" : "AVAILABLE";
				plans.Add(new Plan
				{
					id = Guid.NewGuid().ToString(),
					Name = "Name" + i,
					Status = status,
					__partitionKey=random.Next(20).ToString()

				});
			}
			return plans;
		}
	}
}
