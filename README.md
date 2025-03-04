# Iowa PySpark Project

Beskrivning
Detta projekt använder PySpark och Maskininlärning för att analysera sambandet mellan spritförsäljning och trafikolyckor i Iowa under åren 2014–2016. Genom att bearbeta och analysera data från trafikolyckor och spritförsäljning försöker vi upptäcka eventuella samband mellan de två variablerna.

Projektet består av följande steg:
- Dataförberedelse och rensning med hjälp av PySpark.
- SQL-frågor för att sammanfoga spritförsäljnings- och olycksdata.
- Visualisering av de sammanfogade data för att jämföra spritförsäljning och trafikolyckor per månad.
- Regressionsanalys för att undersöka om det finns ett samband mellan spritförsäljning och trafikolyckor.

📊 Funktioner
- Laddar och rensar trafikolycksdata och spritförsäljningsdata.
- Använder SQL-frågor för att sammanställa och analysera data.
- Skapar grafer för att visualisera sambandet mellan spritförsäljning och trafikolyckor.
- Utför en linjär regressionsanalys för att förutsäga trafikolyckor baserat på spritförsäljning.

🚀 Så här kör du projektet
För att köra detta projekt lokalt, följ dessa steg:

1. Kloning av repository:
   ```bash
   git clone https://github.com/Benereta88/Iowa_PySpark_Project.git
   cd Iowa_PySpark_Project

   
Data
Iowa_Vehicle_Crashes_sample (1).csv: Data om trafikolyckor i Iowa.
Iowa_Liquor_Sales_sample.csv: Data om spritförsäljning i Iowa.
Båda datasetten används för att analysera sambandet mellan trafikolyckor och spritförsäljning.


🛠 Tekniker och verktyg
Apache Spark / PySpark – För att bearbeta och analysera stora mängder data.
Pandas – För datahantering i Python.
Matplotlib – För visualisering av data och resultat.
SQL – För att slå samman och analysera data med hjälp av Spark SQL.

📈 Visualisering
Resultaten från analysen presenteras i följande grafer:

Spritförsäljning vs Trafikolyckor per Månad (2014-2016) – Linjediagram som visar hur spritförsäljningen och trafikolyckorna förändras över tid.
Faktiska vs Predikterade Olyckor – Scatterplot som jämför faktiska olyckor med de predikterade olyckorna från regressionsmodellen.


🔧 Förutsättningar
Python 3.x
Apache Spark (PySpark)
Matplotlib
Pandas

🧑‍🤝‍🧑 Bidrag
Om du vill bidra till projektet, vänligen skapa en Pull Request eller skicka en Issue om du hittar något som kan förbättras.
