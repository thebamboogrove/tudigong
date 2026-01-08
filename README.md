<p align="center"><img width="192" height="192" alt="Tudigong" src="https://github.com/user-attachments/assets/162c6cab-dce7-4806-ab67-24f419f7962e" /></p>

# Tudigong
### China county-level map with 2020 Chinese Census [^1] & GHSL [^2] [^3] [^4] data

Spatial map consisting of 2844 + 4 + 5 + 2 + 135 boundary divisions based on 2021 boundary data [^5] [^6] and de-facto divisions included in census tabulation, of which:

- 2844 standard county-sized divisions
- 4 prefecture-level divisions without any county-sized sub-divisions [^7]
- 5 non-formal county-level administrative zones [^8]
- 2 previously used and named county-level divisions [^9]
- 135 special areas [^10] included in county-sized tabulated data

With additions affecting roughly 190 standard county-level divisions.

> [!WARNING]
> Boundaries are broadly in accordance with the P.R.C.'s national legislation and standards, and any internationally disputed boundary is not defined as such.

> [!WARNING]
> The names of county-level divisions might not correspond temporally to census data.

> [!NOTE]
> Geospatial additions and all other features are sometimes approximate and inaccurate as a result of simplified boundary data.

> [!NOTE]
> Map features are pre-projected to fit an AEA China projection instead of the standard WGS84 (EPSG:4326) projection for display purposes.
> $`{\mathrm{\phi}}_0 = 36 00'00"{\mathrm{N}}\qquad 
\varphi_1 = 24\degree 00'00"{\mathrm{N}}\qquad
\varphi_2 = 48\degree 00'00"{\mathrm{N}}\qquad
{\mathrm{\lambda}}_0 = 104\degree 30'00"{\mathrm{E}}`$

> GHSL data for 2990 county-level divisions R<sup>2</sup>≈0.8761, y=0.9769x5652, MAE=84258 & RMSE=161217

> Un-official administrative and statistical code format:
> | X | X | Y | Y | 7 | Z |
> | - | - | - | - | - | - |
> 
> XX = Provincial-level code. YY = Prefecture-level. 7Z = County-level code beginning with digit 7, with Z being 1–9 e.g., 130171. These codes have been assigned for statistical purposes, and as such are not authoritative nor to be trusted to be accurate.

## Attribution
### D3
Copyright (c), Mike Bostock\
Released under the [ISC License](attr/d3)

### deck.gl
Copyright (c), Vis.gl contributors\
Released under the [MIT License](attr/deckgl)

### TopoJSON Client
Copyright (c), Michael Bostock\
Released under the [ISC License](attr/topojson-client)

### Simple Statistics
Copyright (c), Tom MacWright\
Released under the [ISC License](attr/simple-statistics)

[^1]: National Bureau of Statistics of China.
Tabulation on the 2020 China Population Census by County.
Beijing: China Statistics Press, 2022.
ISBN 978-7-5037-9772-9.
[^2]: Schiavina M., Melchiorri M., Pesaresi M.
GHS-SMOD R2023A: GHS Settlement Layers, Application of the Degree of Urbanisation Methodology (Stage I) to GHS-POP R2023A and GHS-BUILT-S R2023A, Multitemporal (1975–2030).
European Commission, Joint Research Centre (JRC), 2023.
PID: https://data.europa.eu/89h/a0df7a6f-49de-46ea-9bde-563437a6e2ba
https://doi.org/10.2905/A0DF7A6F-49DE-46EA-9BDE-563437A6E2BA2
[^3]: Schiavina M., Freire S., Carioli A., MacManus K.
GHS-POP R2023A: GHS Population Grid Multitemporal (1975–2030).
European Commission, Joint Research Centre (JRC), 2023.
PID: https://data.europa.eu/89h/2ff68a52-5b5b-4a22-8f40-c41da8332cfe
https://doi.org/10.2905/2FF68A52-5B5B-4A22-8F40-C41DA8332CFE
[^4]: Pesaresi M., Schiavina M., Politis P., Freire S., Krasnodębska K., Uhl J. H., Carioli A., Corbane C., Dijkstra L., Florio P., Friedrich H. K., Gao J., Leyk S., Lu L., Maffenini L., Mari-Rivero I., Melchiorri M., Syrris V., Van Den Hoek J., Kemper T.
"Advances on the Global Human Settlement Layer by Joint Assessment of Earth Observation and Population Survey Data." International Journal of Digital Earth 17, no. 1 (2024).
https://doi.org/10.1080/17538947.2024.2390454
[^5]: Rui C., Zhang H., Chen B.
"China Temporal Administrative Map: A Multitemporal Database for Chinese Historical Administrative Divisions (2009–2023)." In Proceedings of the Third International Conference on Environmental Remote Sensing and Geographic Information Technology (ERSGIT 2024), 497–507. SPIE, 2025.
https://doi.org/10.1117/12.3059430 
[^6]: Rui Cheng.
CTAmap: A Long-term Series of Provincial, Municipal and County Administrative Division Dataset (2000–2024). Version 1.30. Dataset, 2025.
https://www.shengshixian.com 
[^7]: 儋州市, 东莞市, 嘉峪关市, and 中山市.
[^8]: 大柴旦行政委员会 assigned to 海西蒙古族藏族自治州.
呼中区, 新林区, 松岭区, and 加格达奇区 assigned to 大兴安岭地区. 
[^9]: 江苏省南通市港闸区, and 安徽省芜湖市三山区.
[^10]: Development zones, administrative districts, and various other non-formal administrative regions.
