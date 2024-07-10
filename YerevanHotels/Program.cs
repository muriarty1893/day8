using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Linq;
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
        var records = csv.GetRecords<ElectricVehicle>().ToList();
        // Verilerin doğru okunup okunmadığını kontrol etmek için birkaç satırı yazdırın
        foreach (var record in records.Take(5))
        {
            Console.WriteLine($"VIN: {record.VIN}, Make: {record.Make}, Model: {record.Model}");
        }
        return records;
    }
}


    private static void IndexElectricVehicles(ElasticClient client, List<ElectricVehicle> vehicles, ILogger logger)
    {
        var bulkIndexResponse = client.Bulk(b => b.IndexMany(vehicles));
        if (!bulkIndexResponse.IsValid)
        {
            logger.LogError("Failed to index some documents: {Errors}", bulkIndexResponse.ItemsWithErrors.Select(e => e.Error));
        }
    }

    private static void SearchElectricVehicles(ElasticClient client, string searchText, ILogger logger)
    {
        var searchResponse = client.Search<ElectricVehicle>(s => s
            .Query(q => q
                .MultiMatch(mm => mm
                    .Query(searchText)
                    .Fields(f => f
                        .Field(p => p.Model)
                    )
                    .Fuzziness(Fuzziness.Auto)
                )
            )
            .Sort(srt => srt
                .Descending(SortSpecialField.Score)
            )
        );

        if (!searchResponse.IsValid)
        {
            logger.LogError("Search failed: {Errors}", searchResponse.OriginalException.Message);
            return;
        }

        Console.WriteLine("Results:\n--------------------------------------------");
        int counter = 0;
        int maxResults = 3;
        foreach (var vehicle in searchResponse.Documents)
        {
            if (counter >= maxResults) break;
            Console.WriteLine($"Make: {vehicle.Make} | Model: {vehicle.Model} | Range: {vehicle.ElectricRange}\n--------------------------------------------");
            counter++;
        }
        Console.WriteLine($"{searchResponse.Documents.Count} match(es) found");
    }

    public static void Main(string[] args)
    {
        Stopwatch stoppwatch1 = new Stopwatch();
        Stopwatch stoppwatch2 = new Stopwatch();
        stoppwatch1.Start();
        using var loggerFactory = LoggerFactory.Create(builder =>
        {
            builder.AddConsole();
            builder.AddDebug();
        });
        var logger = loggerFactory.CreateLogger<Program>();

        var filePath = "Electric_Vehicle_Population_Data.csv";
        var vehicles = ReadCsv(filePath);

        if (vehicles == null || !vehicles.Any())
        {
            logger.LogError("No vehicles found in CSV file");
            return;
        }

        var client = CreateElasticClient();

        // Indexing without deleting existing data
        IndexElectricVehicles(client, vehicles, logger);
        stoppwatch1.Start();
        // Searching for a specific model
        SearchElectricVehicles(client, "330E", logger);
        stoppwatch1.Stop();
        stoppwatch2.Stop();
        Console.WriteLine("");
    }
}