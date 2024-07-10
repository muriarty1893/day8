    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Globalization;
    using System.IO;
    using System.Linq;
    using System.Threading.Tasks;
    using CsvHelper;
    using CsvHelper.Configuration;
    using Nest;
    using Microsoft.Extensions.Logging;
    using Microsoft.Extensions.Logging.Console;
    using Microsoft.Extensions.Logging.Debug;

    public class ElectricVehicle
    {
        public string? VIN { get; set; }
        public string? County { get; set; }
        public string? City { get; set; }
        public string? State { get; set; }
        public string? PostalCode { get; set; }
        public int? ModelYear { get; set; }
        public string? Make { get; set; }
        public string? Model { get; set; }
        public string? ElectricVehicleType { get; set; }
        public string? CAFVEligibility { get; set; }
        public int? ElectricRange { get; set; }
        public double? BaseMSRP { get; set; }
        public int? LegislativeDistrict { get; set; }
        public long? DOLVehicleID { get; set; }
        public string? VehicleLocation { get; set; }
        public string? ElectricUtility { get; set; }
        public long? CensusTract { get; set; }
    }

    public class ElectricVehicleMap : ClassMap<ElectricVehicle>
    {
        public ElectricVehicleMap()
        {
            Map(m => m.VIN).Name("VIN (1-10)");
            Map(m => m.County).Name("County");
            Map(m => m.City).Name("City");
            Map(m => m.State).Name("State");
            Map(m => m.PostalCode).Name("Postal Code");
            Map(m => m.ModelYear).Name("Model Year");
            Map(m => m.Make).Name("Make");
            Map(m => m.Model).Name("Model");
            Map(m => m.ElectricVehicleType).Name("Electric Vehicle Type");
            Map(m => m.CAFVEligibility).Name("Clean Alternative Fuel Vehicle (CAFV) Eligibility");
            Map(m => m.ElectricRange).Name("Electric Range");
            Map(m => m.BaseMSRP).Name("Base MSRP");
            Map(m => m.LegislativeDistrict).Name("Legislative District");
            Map(m => m.DOLVehicleID).Name("DOL Vehicle ID");
            Map(m => m.VehicleLocation).Name("Vehicle Location");
            Map(m => m.ElectricUtility).Name("Electric Utility");
            Map(m => m.CensusTract).Name("2020 Census Tract");
        }
    }

    public class Program
    {
        private static ElasticClient CreateElasticClient()
        {
            var settings = new ConnectionSettings(new Uri("http://localhost:9200"))
                .DefaultIndex("electric_vehicles");
            return new ElasticClient(settings);
        }

        private static List<ElectricVehicle> ReadCsv(string filePath)
        {
            using (var reader = new StreamReader(filePath))
            using (var csv = new CsvReader(reader, new CsvConfiguration(CultureInfo.InvariantCulture)))
            {
                csv.Context.RegisterClassMap<ElectricVehicleMap>();
                return new List<ElectricVehicle>(csv.GetRecords<ElectricVehicle>());
            }
        }

        private static void IndexElectricVehiclesBulk(ElasticClient client, List<ElectricVehicle> vehicles, ILogger logger)
        {
            const int batchSize = 1000; // Her bir bulk isteğinde gönderilecek belge sayısı
            var totalBatches = (vehicles.Count + batchSize - 1) / batchSize;

            Parallel.For(0, totalBatches, i =>
            {
                try
                {
                    var batch = vehicles.Skip(i * batchSize).Take(batchSize).ToList();
                    if (batch == null || !batch.Any())
                    {
                        logger.LogError($"Batch {i + 1}/{totalBatches} is null or empty");
                        return;
                    }

                    foreach (var vehicle in batch)
                    {
                        if (vehicle == null)
                        {
                            logger.LogError($"Vehicle in batch {i + 1}/{totalBatches} is null");
                            return;
                        }
                    }

                    if (client == null)
                    {
                        logger.LogError("ElasticClient is null");
                        return;
                    }

                    var bulkIndexResponse = client.Bulk(b => b.IndexMany(batch));

                    if (!bulkIndexResponse.IsValid)
                    {
                        logger.LogError($"Error indexing batch {i + 1}/{totalBatches}, Reason: {bulkIndexResponse.ServerError}");
                        foreach (var itemWithError in bulkIndexResponse.ItemsWithErrors)
                        {
                            logger.LogError($"Failed item: {itemWithError.Error.Reason}");
                        }
                    }
                }
                catch (Exception ex)
                {
                    logger.LogError($"Exception in batch {i + 1}/{totalBatches}: {ex.Message}");
                }
            });
        }

        private static void DeleteElectricVehicles(ElasticClient client, ILogger logger)
        {
            var deleteResponse = client.DeleteByQuery<ElectricVehicle>(q => q
                .Query(rq => rq
                    .MatchAll()
                )
            );

            if (!deleteResponse.IsValid)
            {
                logger.LogError("Error deleting vehicles: {Reason}", deleteResponse.ServerError);
            }
        }

        private static void SearchElectricVehicles(ElasticClient client, string searchText, ILogger logger)
        {
            var searchResponse = client.Search<ElectricVehicle>(s => s
                .Query(q => q
                    .MultiMatch(mm => mm
                        .Query(searchText)
                        .Fields(f => f
                            .Field(p => p.Model)     // Modele göre arar.
                        )
                        .Fuzziness(Fuzziness.Auto) // Otomatik bulanıklık ayarı.
                    )
                )
                .Sort(srt => srt
                    .Descending(SortSpecialField.Score) // Sonuçları puan sırasına göre sıralar.
                )
            );

            if (!searchResponse.IsValid)
            {
                logger.LogError("Error searching vehicles: {Reason}", searchResponse.ServerError);
                return;
            }
            Console.WriteLine("Results:\n--------------------------------------------");
            int counter = 0;
            foreach (var vehicle in searchResponse.Documents)
            {
                if (counter >= 3) { break; } // En fazla 5 aracı yazdırır.
                Console.WriteLine($"Make: {vehicle.Make} | Model: {vehicle.Model} | Range: {vehicle.ElectricRange}\n--------------------------------------------");
                counter++;
            }
            Console.WriteLine(searchResponse.Documents.Count + " match(es) found");
        }

        public static void Main(string[] args)
        {
        using var loggerFactory = LoggerFactory.Create(builder =>
        {
            builder.AddConsole();
            builder.AddDebug();
        });
        var logger = loggerFactory.CreateLogger<Program>();

        var filePath = "Electric_Vehicle_Population_Data.csv"; // CSV dosyasının yolu
        Stopwatch stopwatch2 = new Stopwatch();
        Stopwatch stopwatch1 = new Stopwatch();
        stopwatch2.Start();

        var vehicles = ReadCsv(filePath); // CSV dosyasını okur  
        if (vehicles == null || !vehicles.Any())
        {
            logger.LogError("No vehicles read from CSV");
            return;
        }

        var client = CreateElasticClient(); // Elasticsearch istemcisini oluşturur

        DeleteElectricVehicles(client, logger); // Elasticsearch'ten mevcut tüm araçları siler
        IndexElectricVehiclesBulk(client, vehicles, logger); // CSV'den okunan araçları Elasticsearch'e bulk indeksler
        
        stopwatch1.Start();
        SearchElectricVehicles(client, "330E", logger); // Elasticsearch'te girilen kelimeyi arar
        stopwatch1.Stop();
        stopwatch2.Stop();

        Console.WriteLine($"Search completed in {stopwatch1.ElapsedMilliseconds} ms");
        Console.WriteLine($"Whole operation completed in {stopwatch2.ElapsedMilliseconds} ms");
        }
    }