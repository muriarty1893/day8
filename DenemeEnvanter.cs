using System;
using System.Collections.Generic;
using System.IO;
using System.Net.Http;
using System.Threading.Tasks;
using System.Web.Script.Serialization;
using System.Web.UI;
using System.Web.UI.HtmlControls;
using HtmlAgilityPack;
using Newtonsoft.Json;

namespace DenemeEnvanter
{
    public partial class Anasayfa : Page
    {
        protected HtmlGenericControl price;

        protected void Page_Load(object sender, EventArgs e)
        {
            if (!IsPostBack)
            {
                string appDataPath = Server.MapPath("~/App_Data");
                if (!Directory.Exists(appDataPath))
                {
                    Directory.CreateDirectory(appDataPath);
                }

                Task.Run(() => ScrapeData()).Wait();
                LoadJsonData();
            }
        }

        private async Task ScrapeData()
        {
            string url = "https://www.trendyol.com/sr?q=gta%205%20ps4";
            HttpClient client = new HttpClient();
            HttpResponseMessage response = await client.GetAsync(url);
            response.EnsureSuccessStatusCode();
            string htmlContent = await response.Content.ReadAsStringAsync();

            HtmlDocument doc = new HtmlDocument();
            doc.LoadHtml(htmlContent);

            var isimler = doc.DocumentNode.SelectNodes("//span[contains(@class, 'prdct-desc-cntnr-name hasRatings')]");
            var fiyatlar = doc.DocumentNode.SelectNodes("//div[contains(@class, 'prc-box-dscntd')]");

            List<(string Isim, string Fiyat)> urunler = new List<(string, string)>();

            for (int i = 0; i < isimler.Count; i++)
            {
                string isim = isimler[i].InnerText.Trim();
                string fiyat = fiyatlar[i].InnerText.Trim();
                urunler.Add((isim, fiyat));
            }

            string sonFiyat = urunler[0].Fiyat;
            string sonIsim = urunler[0].Isim;
            var data = new { price = sonFiyat , name = sonIsim};

            string json = JsonConvert.SerializeObject(data, Formatting.Indented);
            File.WriteAllText(Server.MapPath("~/App_Data/data.json"), json);
        }

        private void LoadJsonData()
        {
            string jsonFilePath = Server.MapPath("~/App_Data/data.json");

            if (File.Exists(jsonFilePath))
            {
                string jsonData = File.ReadAllText(jsonFilePath);

                JavaScriptSerializer serializer = new JavaScriptSerializer();
                dynamic data = serializer.Deserialize<dynamic>(jsonData);

                price.InnerText = data["price"];
                gameName.InnerText = data["name"];
            }
        }
    }
}