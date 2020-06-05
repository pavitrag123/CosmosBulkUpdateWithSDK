using BulkUpdateWithClientSDK.Models;
using Microsoft.Azure.Cosmos;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace BulkUpdateWithClientSDK
{
	public class CosmosRepo
	{
		private Container _container;
		private static CosmosClient dbClient;
		private ILogger logger;
		public CosmosRepo(ILogger logger)
		{
			this.logger = logger;
			if (dbClient == null)
			{
				CosmosClientOptions clientOptions = new CosmosClientOptions()
				{
					AllowBulkExecution = true,
					ConnectionMode = ConnectionMode.Gateway,
					GatewayModeMaxConnectionLimit = 300
				};
				dbClient = new CosmosClient("https://localhost:8081/", "C2y6yDjf5/R+ob0N8A7Cgv30VRDJIWEHLM+4QDU5DE2nQ9nDuVTqobD4b8mGGyPMbIZnqyMsEcaGQy67XIw/Jw==", clientOptions);


			}
			this._container = GetDBContainerAsync().GetAwaiter().GetResult();
		}
		private async Task<Container> GetDBContainerAsync()
		{
			var _container = dbClient.GetContainer("db1", "test");
			await _container.ReadContainerAsync();
			return _container;
		}
		public async Task<IEnumerable<T>> GetItemsAsync<T>(string queryString, QueryRequestOptions requestOptions)
		{
			var query = this._container.GetItemQueryIterator<T>(new QueryDefinition(queryString), null, requestOptions);
			List<T> results = new List<T>();
			double Rus = 0;
			while (query.HasMoreResults)
			{
				var response = await query.ReadNextAsync();
				Rus = Rus + response.RequestCharge;
				results.AddRange(response.ToList());
			}
			return results;
		}
		public async Task BulkProcessData(BulkProcessData dataToImport)
		{
			CancellationTokenSource cancellationTokenSource = new CancellationTokenSource();
			//cancellationTokenSource.CancelAfter(runtimeInSeconds * 1000);
			CancellationToken cancellationToken = cancellationTokenSource.Token;
			try
			{
				ConcurrentBag<Task> concurrentTasks = new ConcurrentBag<Task>();
				ConcurrentBag<string> completedTasks = new ConcurrentBag<string>();

				foreach (var itemData in dataToImport.ItemData)
				{
					if (dataToImport.Operation == Operation.IMPORT)
					{
						var tsk = this._container.UpsertItemStreamAsync(itemData.Item, new PartitionKey(itemData.PartitionKey), null, cancellationToken)
						.ContinueWith((Task<ResponseMessage> task) =>
						{
							logger.LogInformation("ReplaceItemStreamAsync itemId" + itemData.ItemId);
							logger.LogInformation("ReplaceItemStreamAsync  task IsCompleted status " + task.IsCompleted);
							logger.LogInformation("ReplaceItemStreamAsync  task IsCompletedSuccessfully status " + task.IsCompletedSuccessfully);
							logger.LogInformation("ReplaceItemStreamAsync  task IsCanceled status " + task.IsCanceled);
							logger.LogInformation("ReplaceItemStreamAsync  task IsFaulted status " + task.IsFaulted);
							if (task.IsCompletedSuccessfully)
							{
								completedTasks.Add(itemData.ItemId);
								if (itemData.Item != null) { itemData.Item.Dispose(); }
								if (task.Result != null) { task.Result.Dispose(); }
							}
							else
							{
								logger.LogError("Error while updating document.", JsonConvert.SerializeObject(task.Result.Diagnostics));
							}

							task.Dispose();
						});
						concurrentTasks.Add(tsk);
					}
					else if (dataToImport.Operation == Operation.DELETE)
					{
						var tsk = this._container.DeleteItemStreamAsync(itemData.ItemId, new PartitionKey(itemData.PartitionKey), null, cancellationToken)
							.ContinueWith((Task<ResponseMessage> task) =>
							{
								if (task.IsCompletedSuccessfully)
								{
									completedTasks.Add(itemData.ItemId);
									if (itemData.Item != null) { itemData.Item.Dispose(); }
									if (task.Result != null) { task.Result.Dispose(); }
								}
								else
								{
									logger.LogError("Error while deleting document.", JsonConvert.SerializeObject(task.Result.Diagnostics));
								}

								task.Dispose();
							});
						concurrentTasks.Add(tsk);
					}

				}
				await Task.WhenAll(concurrentTasks);
				//logger?.LogInformation(string.Format("Processed ItemIds {0}", JsonConvert.SerializeObject(completedTasks)));

				logger?.LogInformation(string.Format("Bulk processed {0}  out of {2} items for database {1}", completedTasks.Count, this._container.Database.Id, concurrentTasks.Count));
			}
			catch (Exception ex)
			{
				logger?.LogError(JsonConvert.SerializeObject(ex));

			}

		}

	}
}
