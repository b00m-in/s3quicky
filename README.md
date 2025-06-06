# [Contents](#Contents)
- [Overturemaps](#overturemaps)
- [pmtiles](#pmtiles)
  - [go-pmtiles](#go-pmtiles)
- [geoparquet](#geoparquet)
  - [s3quicky](s3quicky)
- [Milestones](#milestones)
- [Questions](#questions)
- [References](#references)

## Overturemaps

A project to aggregate map data from various sources and distribute that data normalized to one schema. The json schema is best avoided to begin with as the data is provided in geoparquet format and it's easier to query the data using the parquet column names (`id, geometry, bbox, theme, type, version, sources`). The `bbox` and `theme` columns stand out as columns to filter on to limit size of data requests/downloads.   

The duckdb example is also best avoided to begin with because the query in the example takes a long time to run, converts the geoparquet data to geojson and then uses another tool [tippecanoe](#references) to convert to a pmtile, when in fact the pmtiles are directly available for download - [Overturemaps pmtiles](#references)

## pmtiles

### go-pmtiles

Handy cli to query/extract the pmtile objects in overturemaps s3 bucket. The `show` command:  

`./go-pmtiles show https://overturemaps-tiles-us-west-2-beta.s3.amazonaws.com/2025-05-21/base.pmtiles`

outputs that there are 90m addressed tiles, 30m tile entries, 22.5m tile contents - not sure to what each of those numbers corresponds. 

```
pmtiles spec version: 3    
tile type: mvt
bounds: (long: -180.000000, lat: -85.051129) (long: 180.000000, lat: 85.051129)
min zoom: 0
max zoom: 13
center: (long: 0.000000, lat: 0.000000)
center zoom: 0
addressed tiles count: 89478485
tile entries count: 29970401
tile contents count: 22500299
clustered: true
internal compression: gzip
tile compression: gzip
vector_layers <object...>
attribution <a href="https://www.openstreetmap.org/copyright" target="_blank">&copy; OpenStreetMap</a> <a href="https://docs.overturemaps.org/attribution" target="_blank">&copy; Overture Maps Foundation</a>
description A tileset generated from Overture data
name Overture base
planetiler:buildtime 2024-06-17T09:47:21.506Z
planetiler:githash 437afe62c7dfaf8a45edbae5590c7d868ec2ef25
planetiler:version 0.8.0
type overlay
web viewer: https://pmtiles.io/#url=https%3A%2F%2Foverturemaps-tiles-us-west-2-beta.s3.amazonaws.com%2F2025-05-21%2Fbase.pmtiles
```
The `show` command works by only requesting headers and metadata using a small 'Range' request of a few bytes of data.  

The `extract` command can use the `bbox` flag to limit the data requested:

```
./go-pmtiles extract https://overturemaps-tiles-us-west-2-beta.s3.amazonaws.com/2025-05-21/buildings.pmtiles madrid_buildings.pmtiles --bbox=-3.71,40.41,-3.69,40.43
./go-pmtiles extract https://overturemaps-tiles-us-west-2-beta.s3.amazonaws.com/2025-05-21/transport.pmtiles madrid_xport.pmtiles --bbox=-3.71,40.41,-3.69,40.43
```
These pmtiles are <10MB and can be viewed using QGIS/GDAL or by uploading [here](https://pmtiles.io). If no `zoom` flag is provided, go-pmtiles will download tiles for all the zoom levels specified in the metadata (0-14 in the case above). 

### geoparquet

To query the `parquet` data instead of the `pmtile` data, the `go-pmtiles` cannot be used as it only permits pmtiles so instead of hacking at it to over-ride the file format check, we resort to `aws-sdk-go` to develop a quick cli to list the keys in the `overturemaps-us-west-2` s3 bucket and download the parquet data with the same filters (`bbox` etc.)

```
./s3quicky ls overturemaps-us-west-2
```
lists the keys in the bucket with a limit of 1000.

```
Name: bridgefiles/2025-04-23.0/dataset=Esri Community Maps/theme=buildings/type=building/part-00200-dde0f909-37de-437a-b25a-02cc821e2cba.c000.zstd.parquet             
Name: bridgefiles/2025-04-23.0/dataset=Esri Community Maps/theme=buildings/type=building/part-00201-dde0f909-37de-437a-b25a-02cc821e2cba.c000.zstd.parquet             
Name: bridgefiles/2025-04-23.0/dataset=Esri Community Maps/theme=buildings/type=building/part-00202-dde0f909-37de-437a-b25a-02cc821e2cba.c000.zstd.parquet

...

Name: bridgefiles/2025-04-23.0/dataset=Instituto Geográfico Nacional (España)s/theme=buildings/type=building/part-00228-dde0f909-37de-437a-b25a-02cc821e2cba.c000.zstd.parquet
Name: bridgefiles/2025-04-23.0/dataset=Instituto Geográfico Nacional (España)s/theme=buildings/type=building/part-00230-dde0f909-37de-437a-b25a-02cc821e2cba.c000.zstd.parquet
```

Use the `prefix` flag to narrow in on a desired list of keys:

```
./s3quicky ls --prefix release/2025-05-21 overturemaps-us-west-2
```

Individual keys / files can then downloaded. Obviously this involves knowing the version, the provider the part etc. so is not very useful from a user perspective, but it provides some insight into how the data is actually stored.

```
./s3quicky get overturemaps-us-west-2 bridgefiles/2025-04-23.0/dataset=Esri Community Maps/theme=buildings/type=building/part-00200-dde0f909-37de-437a-b25a-02cc821e2cba.c000.zstd.parquet             
```

TODO: develop a way to get all the geoparquet data from all the keys/datasets/providers/parts within a `bbox` like the `go-pmtiles` tool does.

The [parquet file format](https://parquet.apache.org/docs/file-format/) has the length of the metadata in the penultimate 4 bytes (the last 4 bytes are `PAR1` or `PARE`) and the actual metadata in the bytesimmediatele prior to the size.

So one possible way to get at the metadata is as follows:
1. Get the file size with a Head request and check ContentLength
```
./s3quicky head overturemaps-us-west-2 release/2025-05-21.0/theme=buildings/type=building/part-00085-0df994ca-3323-4d7c-a374-68c653f78289-c000.zstd.parquet 
{
  AcceptRanges: "bytes",
  ContentLength: 991368715,
  ContentType: "application/octet-stream",
  ETag: "\"7e564dc6a8d67bce8eb6734529fc53ba-190\"",
  LastModified: 2025-05-21 16:13:25 +0000 UTC,
  Metadata: {
    Mtime: "1747668333"
  },
  ServerSideEncryption: "AES256",
  StorageClass: "INTELLIGENT_TIERING"
} 
```
2. Use the `ContentLength` from `1` above and the range header to `peek` at the last 8 bytes of the file 
```
./s3quicky head -p overturemaps-us-west-2 release/2025-05-21.0/theme=buildings/type=building/part-00085-0df994ca-3323-4d7c-a374-68c653f78289-c000.zstd.parquet
299215 PAR1
```
3. Now that we can calculate the metadata footer offset from the end, use the range header to download only the metadata (using `end=-1` will download the entire object)

```
> 991368715-299215=991069492
./s3quicky get -start=991069492 -end=991368715 overturemaps-us-west-2 release/2025-05-21.0/theme=buildings/type=building/part-00085-0df994ca-3323-4d7c-a374-68c653f78289-c000.zstd.parquet
```
This will save to a file called `part-00085-0df994ca-3323-4d7c-a374-68c653f78289-c000-bytes-991069492-991368715.zstd.parquet`

4. Then something like `parquet_reader` from `arrow-go` can be used to read the metadata and convert it to text.

```
./parquet_reader part-00085-0df994ca-3323-4d7c-a374-68c653f78289-c000-bytes-991069492-991368715.zstd.parquet > part-00085-0df994ca-3323-4d7c-a374-68c653f78289-c000.metadata

```

All the 4 steps above steps are combined into a single step using the `show` command:

```
./s3quicky show overturemaps-us-west-2 release/2025-05-21.0/theme=buildings/type=building/part-00085-0df994ca-3323-4d7c-a374-68c653f78289-c000.zstd.parquet > 00085-0df994ca.metadata

File name: part-00085-0df994ca-3323-4d7c-a374-68c653f78289-c000.zstd.parquet
Version: v1.0
Created By: parquet-mr version 1.12.3-databricks-0002 (build 2484a95dbe16a0023e3eb29c201f99ff9ea771ee)
Num Rows: 11747508
Number of RowGroups: 67
Number of Real Columns: 24
Number of Columns: 38
Number of Selected Columns: 38
Column 0: id (BYTE_ARRAY/UTF8)
Column 1: geometry (BYTE_ARRAY)
Column 2: bbox.xmin (FLOAT)
Column 3: bbox.xmax (FLOAT)
Column 4: bbox.ymin (FLOAT)
Column 5: bbox.ymax (FLOAT)
Column 6: version (INT32)
Column 7: sources.list.element.property (BYTE_ARRAY/UTF8)
...

```
TODO: Now we can use columns 2-5 (`bbox`) to check whether the data contained in the parquet is in a zone of interest.

## Milestones

What's the average height of all the buildings in Madrid for which user-contributed height data exists?

## Questions

Do openstreetmap users provide building height data? If so, how do they measure the eight of the building and how is the contribution verified?

## References

[Overturemaps docs](https://docs.overturemaps.org/getting-data/)  
[Overturemaps schema](https://docs.overturemaps.org/schema/)  
[Overturemaps pmtiles](https://docs.overturemaps.org/examples/overture-tiles/#13/47.6/-122.33/0/45)  
[Overturemaps github](https://github.com/OvertureMaps/data)  
[Geoparquet format](https://guide.cloudnativegeo.org/geoparquet/)  
[tippecanoe](https://github.com/felt/tippecanoe)  
[arrow-go](https://gitub.com/apache/arrow-go)  
[s3quicky](https://github.com/b00m-in/s3quicky)  