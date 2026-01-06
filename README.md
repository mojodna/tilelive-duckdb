# tilelive-duckdb

A [tilelive](https://github.com/mapbox/tilelive) source for serving [Mapbox Vector Tiles](https://docs.mapbox.com/data/tilesets/guides/vector-tiles-introduction/) from [DuckDB](https://duckdb.org/) tables with spatial data.

## Installation

```bash
npm install tilelive-duckdb
```

## Usage

### Register the protocol

```javascript
const tilelive = require("@mapbox/tilelive");
require("tilelive-duckdb")(tilelive);
```

### Load a source

```javascript
tilelive.load("duckdb:///path/to/data.db?table=buildings", (err, source) => {
  if (err) throw err;

  source.getTile(14, 8192, 8192, (err, tile, headers) => {
    // tile is a Buffer containing MVT data
  });
});
```

### URI Format

```text
duckdb:///path/to/database.db?table=<table>&geometry=<column>&layer=<name>
```

| Parameter  | Required | Default    | Description              |
| ---------- | -------- | ---------- | ------------------------ |
| `table`    | Yes      | -          | Source table name        |
| `geometry` | No       | `geometry` | Geometry column name     |
| `layer`    | No       | table name | Layer name in output MVT |

### With Tessera

```bash
tessera "duckdb:///data/buildings.db?table=buildings"
```

## Requirements

- Geometry must be in EPSG:3857 (Web Mercator) projection
- DuckDB spatial extension must be installed (`INSTALL spatial`)
- Table must exist and contain a geometry column

## Preparing Data

Tile serving requires geometries in EPSG:3857 (Web Mercator) projection and benefits from an R-tree spatial index for efficient tile queries. The index allows DuckDB to quickly filter geometries intersecting each tile's bounding box.

This example creates a tiled database from the latest Overture Maps building data for New York City:

```sql
install spatial;
load spatial;

-- attach to latest Overture Maps data (read-only)
attach 'https://labs.overturemaps.org/data/latest.ddb' as overture (read_only);

-- create local database for tiling
attach 'tiles.db';
use tiles;

create or replace table buildings as (
  select
    -- transform into Web Mercator for tile serving
    st_transform(geometry, 'EPSG:4326', 'EPSG:3857', always_xy := true) as geometry,
    subtype,
    class,
    height,
  from
    overture.building
  where
    -- extent of New York City
    bbox.xmin between -74.2 and -73.6 and bbox.ymin between 40.5 and 40.9
    and bbox.xmax between -74.2 and -73.6
    and bbox.ymax between 40.5 and 40.9
);

-- create spatial index for efficient tile queries
create index buildings_idx on buildings using rtree (geometry);

use memory.main;
detach tiles;
```

The R-tree index is critical for performance—without it, each tile request would scan the entire table.

## API

### `source.getTile(z, x, y, callback)`

Returns a Mapbox Vector Tile for the specified tile coordinates.

- `z` - Zoom level
- `x` - Tile column
- `y` - Tile row
- `callback(err, data, headers)` - Called with MVT buffer and headers

### `source.getInfo(callback)`

Returns metadata about the tile source.

- `callback(err, info)` - Called with info object containing:
  - `bounds` - `[minx, miny, maxx, maxy]` in EPSG:4326
  - `minzoom` - Minimum zoom level (0)
  - `maxzoom` - Maximum zoom level (14)
  - `format` - Tile format (`"pbf"`)

### `source.close(callback)`

Closes the database connection.

## License

ISC
