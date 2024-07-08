using System;
using System.Collections.Generic;
using System.IO;
using CsvHelper;
using CsvHelper.Configuration;
using Nest;

public class Hotel
{
    public string? HotelName { get; set; }
    public string? FreeParking { get; set; }
    public decimal PricePerDay { get; set; }
}

public class Program
{
    private static ElasticClient CreateElasticClient()
    {
        var settings = new ConnectionSettings(new Uri("http://localhost:9200"))
            .DefaultIndex("hotels");
        return new ElasticClient(settings);
    }

    private static List<Hotel> ReadCsv(string filePath)
    {
        using (var reader = new StreamReader(filePath))
        using (var csv = new CsvReader(reader, new CsvConfiguration(System.Globalization.CultureInfo.InvariantCulture)))
        {
            return new List<Hotel>(csv.GetRecords<Hotel>());
        }
    }

    private static void IndexHotels(ElasticClient client, List<Hotel> hotels)
    {
        foreach (var hotel in hotels)
        {
            client.IndexDocument(hotel);
        }
    }

    private static void SearchHotels(ElasticClient client, string searchText)
    {
        var searchResponse = client.Search<Hotel>(s => s
            .Query(q => q
                .Match(m => m
                    .Field(f => f.HotelName)
                    .Query(searchText)
                )
            )
        );

        foreach (var hotel in searchResponse.Documents)
        {
            Console.WriteLine($"Hotel: {hotel.HotelName}, Price: {hotel.PricePerDay}, Free Parking: {hotel.FreeParking}");
        }
    }

    public static void Main(string[] args)
    {
        var filePath = "oteller.csv";
        var hotels = ReadCsv(filePath);

        var client = CreateElasticClient();

        // Index hotels
        IndexHotels(client, hotels);

        // Search hotels
        SearchHotels(client, "Kantar");
    }
}
