using System;
using System.Collections.Generic;
using System.IO;
using Nest;
using CsvHelper;
using CsvHelper.Configuration;
using CsvHelper.Configuration.Attributes;
using System.Globalization;
using System.Linq;

namespace ElasticsearchExample2
{
    // FinancialData sınıfı, CSV dosyasındaki verilerin yapısını temsil eder
    public class FinancialData
    {
        [Name("Company Name")]
        public string CompanyName { get; set; }

        [Name("Revenue")]
        public double Revenue { get; set; }

        [Name("Profit")]
        public double Profit { get; set; }

        [Name("Year")]
        public int Year { get; set; }
    }

    class Program
    {
        static void Main(string[] args)
        {
            try
            {
                // Elasticsearch bağlantı ayarlarını yapılandır
                var settings = new ConnectionSettings(new Uri("http://localhost:9200"))
                    .DefaultIndex("financial_data") // Varsayılan indeks adı
                    .DisableDirectStreaming(); // Hata ayıklama bilgilerini görmek için

                var client = new ElasticClient(settings);

                // İndeks var mı kontrol et
                if (!client.Indices.Exists("financial_data").Exists)
                {
                    // İndeks oluştur ve FinancialData sınıfını haritalandır
                    client.Indices.Create("financial_data", c => c
                        .Map<FinancialData>(m => m
                            .AutoMap()
                            .Properties(ps => ps
                                .Text(t => t
                                    .Name(fd => fd.CompanyName)
                                    .Analyzer("standard")
                                )
                                .Number(t => t
                                    .Name(fd => fd.Revenue)
                                )
                                .Number(t => t
                                    .Name(fd => fd.Profit)
                                )
                                .Number(t => t
                                    .Name(fd => fd.Year)
                                )
                            )
                        )
                    );
                }

                // CSV dosyasını oku ve boş satırları kaldır
                using (var reader = new StreamReader(@"C:\Users\Murat Eker\Desktop\day8\business-financial-data-march-2024-csv.csv"))
                using (var csv = new CsvReader(reader, new CsvConfiguration(CultureInfo.InvariantCulture)
                {
                    HeaderValidated = null, // Başlık doğrulamasını devre dışı bırak
                    MissingFieldFound = null, // Eksik alan bulunursa hata vermemek için
                }))
                {
                    var records = csv.GetRecords<FinancialData>().Where(record =>
                        !string.IsNullOrWhiteSpace(record.CompanyName) &&
                        record.Revenue != 0 &&
                        record.Profit != 0 &&
                        record.Year != 0
                    ).ToList();

                    // Verileri Elasticsearch'e yükle
                    var indexResponse = client.IndexMany(records);

                    if (!indexResponse.IsValid)
                    {
                        Console.WriteLine("Veriler Elasticsearch'e yüklenirken hata oluştu.");
                        Console.WriteLine(indexResponse.DebugInformation);  // Detaylı hata bilgilerini yazdırır
                        foreach (var item in indexResponse.ItemsWithErrors)
                        {
                            Console.WriteLine($"Hata: {item.Error}");
                        }
                        return;
                    }
                }

                // Basit bir arama yap
                var searchResponse = client.Search<FinancialData>(s => s
                    .Query(q => q
                        .QueryString(qs => qs
                            .Query("Business") // Aranacak kelimeyi buraya yazın
                        )
                    )
                );

                // Arama Sorgusunun Doğru Olduğunu Kontrol Etme
                if (!searchResponse.IsValid)
                {
                    Console.WriteLine("Arama sorgusu başarısız oldu.");
                    Console.WriteLine(searchResponse.DebugInformation);  // Hata bilgilerini yazdırır
                    return;
                }

                if (searchResponse.Hits.Count == 0)
                {
                    Console.WriteLine("Arama sorgusuyla eşleşen sonuç bulunamadı.");
                }
                else
                {
                    foreach (var hit in searchResponse.Hits)
                    {
                        Console.WriteLine($"{hit.Source.CompanyName} - {hit.Source.Revenue} - {hit.Source.Profit} - {hit.Source.Year}");
                    }
                }

                // Anında cmd den çıkmasın diye
                Console.WriteLine("Bir tuş  a basın çıkmak için...");
                Console.ReadLine(); // Programın kapanmasını engellemek için eklenen satır
            }
            catch (Exception ex)
            {
                // Hata ayıklama
                Console.WriteLine($"Beklenmeyen bir hata oluştu: {ex.Message}");
                Console.WriteLine(ex.StackTrace);
            }
        }
    }
}
