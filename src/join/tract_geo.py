import httpx
import geopandas as gpd
import pandas as pd
from shapely.geometry import Point
from pathlib import Path
from tqdm import tqdm

from src.config import GEO_DIR, STATE_FIPS

TIGER_BASE = "https://www2.census.gov/geo/tiger/TIGER2022/TRACT"


class TractGeocoder:
    def __init__(self, states: list[str] | None = None):
        self.states = states or list(STATE_FIPS.keys())
        self.tracts_gdf: gpd.GeoDataFrame | None = None
        self.sindex = None

    def _download_state_shapefile(self, state_fips: str) -> Path:
        out_dir = GEO_DIR / "tracts" / state_fips
        shp_path = out_dir / f"tl_2022_{state_fips}_tract.shp"
        if shp_path.exists():
            return shp_path

        out_dir.mkdir(parents=True, exist_ok=True)
        zip_url = f"{TIGER_BASE}/tl_2022_{state_fips}_tract.zip"
        resp = httpx.get(zip_url, timeout=120, follow_redirects=True)
        resp.raise_for_status()

        import zipfile
        from io import BytesIO
        with zipfile.ZipFile(BytesIO(resp.content)) as zf:
            zf.extractall(out_dir)
        return shp_path

    def load(self):
        gdfs = []
        for fips in tqdm(self.states, desc="Loading tract shapefiles"):
            shp_path = self._download_state_shapefile(fips)
            gdf = gpd.read_file(shp_path)
            gdf["tract_fips"] = gdf["GEOID"]
            gdfs.append(gdf[["tract_fips", "geometry"]])

        self.tracts_gdf = pd.concat(gdfs, ignore_index=True)
        self.tracts_gdf = gpd.GeoDataFrame(self.tracts_gdf, geometry="geometry")
        self.tracts_gdf = self.tracts_gdf.set_crs("EPSG:4326", allow_override=True)
        self.sindex = self.tracts_gdf.sindex
        print(f"Loaded {len(self.tracts_gdf)} tract polygons")

    def assign_tract(self, lat: float, lon: float) -> str | None:
        point = Point(lon, lat)
        candidates = list(self.sindex.query(point, predicate="intersects"))
        if candidates:
            return self.tracts_gdf.iloc[candidates[0]]["tract_fips"]
        return None

    def bulk_assign(self, df: pd.DataFrame, lat_col: str, lon_col: str) -> pd.DataFrame:
        valid = df[[lat_col, lon_col]].dropna()
        if valid.empty:
            df["tract_fips"] = None
            return df

        points = gpd.GeoDataFrame(
            df,
            geometry=gpd.points_from_xy(df[lon_col], df[lat_col]),
            crs="EPSG:4326",
        )
        joined = gpd.sjoin(points, self.tracts_gdf, how="left", predicate="within")
        df = df.copy()
        df["tract_fips"] = joined["tract_fips_right"].values
        return df
