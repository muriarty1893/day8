using System;
using System.Collections.Generic;
using System.IO;
using Nest;
using CsvHelper;
using CsvHelper.Configuration;
using CsvHelper.Configuration.Attributes;
using System.Globalization;

namespace ElasticsearchExample2
{
    // FinancialData sınıfı, CSV dosyasındaki verilerin yapısını temsil eder
    public class FinancialData
    {
        [Name("Series_reference")]
        public string SeriesReference { get; set; }

        [Name("Period")]
        public string Period { get; set; }

        [Name("Data_value")]
        public decimal DataValue { get; set; }

        [Name("Suppressed")]
        public string Suppressed { get; set; }

        [Name("STATUS")]
        public string Status { get; set; }

        [Name("UNITS")]
        public string Units { get; set; }

        [Name("Magnitude")]
        public int Magnitude { get; set; }

        [Name("Subject")]
        public string Subject { get; set; }

        [Name("Group")]
        public string Group { get; set; }

        [Name("Series_title_1")]
        public string SeriesTitle1 { get; set; }

        [Name("Series_title_2")]
        public string SeriesTitle2 { get; set; }

        [Name("Series_title_3")]
        public string SeriesTitle3 { get; set; }

        [Name("Series_title_4")]
        public string SeriesTitle4 { get; set; }

        [Name("Series_title_5")]
        public string SeriesTitle5 { get; set; }
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
                                    .Name(n => n.SeriesReference)
                                    .Analyzer("standard")
                                )
                                .Text(t => t
                                    .Name(n => n.Period)
                                    .Analyzer("standard")
                                )
                                .Number(n => n
                                    .Name(n => n.DataValue)
                                    .Type(NumberType.Double)
                                )
                                .Text(t => t
                                    .Name(n => n.Suppressed)
                                    .Analyzer("standard")
                                )
                                .Text(t => t
                                    .Name(n => n.Status)
                                    .Analyzer("standard")
                                )
                                .Text(t => t
                                    .Name(n => n.Units)
                                    .Analyzer("standard")
                                )
                                .Number(n => n
                                    .Name(n => n.Magnitude)
                                    .Type(NumberType.Integer)
                                )
                                .Text(t => t
                                    .Name(n => n.Subject)
                                    .Analyzer("standard")
                                )
                                .Text(t => t
                                    .Name(n => n.Group)
                                    .Analyzer("standard")
                                )
                                .Text(t => t
                                    .Name(n => n.SeriesTitle1)
                                    .Analyzer("standard")
                                )
                                .Text(t => t
                                    .Name(n => n.SeriesTitle2)
                                    .Analyzer("standard")
                                )
                                .Text(t => t
                                    .Name(n => n.SeriesTitle3)
                                    .Analyzer("standard")
                                )
                                .Text(t => t
                                    .Name(n => n.SeriesTitle4)
                                    .Analyzer("standard")
                                )
                                .Text(t => t
                                    .Name(n => n.SeriesTitle5)
                                    .Analyzer("standard")
                                )
                            )
                        )
                    );
                }

                // CSV dosyasını oku
                using (var reader = new StreamReader(@"C:\Users\Murat Eker\Desktop\day8\business-financial-data-march-2024-csv.csv"))
                using (var csv = new CsvReader(reader, new CsvConfiguration(CultureInfo.InvariantCulture)
                {
                    HeaderValidated = null, // Başlık doğrulamasını devre dışı bırak
                    MissingFieldFound = null, // Eksik alan bulunursa hata vermemek için
                }))
                {
                    // CSV verilerini FinancialData nesnelerine dönüştür
                    var records = csv.GetRecords<FinancialData>();
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
                            .Query("reap") // Aranacak kelimeyi buraya yazın
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
                        Console.WriteLine($"{hit.Source.SeriesReference} - {hit.Source.Period} - {hit.Source.DataValue}");
                    }
                }

                // Anında cmd den çıkmasın diye
                Console.WriteLine("Bir tuşa basın çıkmak için...");
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
