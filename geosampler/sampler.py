__all__ = ["Sampler"]

from ast import expr_context
import json
import time
import urllib.parse

import folium
import pandas as pd
import requests
from folium import plugins
from tqdm import tqdm
from .utils import basemaps


class Sampler:
    def __init__(
        self,
        api_key: str,
        language: str = "en",
        keyword: str = "",
        maxprice: str = "",
        minprice: str = "",
        opennow: str = "",
        radius: str = "5000",
        type_: str = "",
        rankby: str = "prominence",
    ):
        self.api_key = api_key
        self.language = language
        self.keyword = keyword
        self.maxprice = maxprice
        self.minprice = minprice
        self.opennow = opennow
        self.radius = radius
        self.type_ = type_
        self.rankby = rankby

    def _get_place_details(self, place_id: str) -> pd.Series:
        payload = {}
        headers = {}
        url = f"https://maps.googleapis.com/maps/api/place/details/json?place_id={place_id}&key={self.api_key}"
        response = requests.request("GET", url, headers=headers, data=payload)
        response_json = json.loads(response.text)
        if response_json["result"]:
            df = pd.json_normalize(response_json["result"], max_level=2)
            keys = [
                "place_id",
                "international_phone_number",
                "website",
                "address_components",
                "price_level",
            ]
            keys = [key for key in keys if key in df.columns]
            df = df[keys]
            address_components = df["address_components"][0]
            address_components = pd.json_normalize(df["address_components"][0])
            address_components["types"] = address_components["types"].apply(
                lambda x: str(x[0])
            )
            try:
                df["postal_code"] = address_components["long_name"][
                    address_components["types"].loc[lambda x: x == "postal_code"].index
                ].item()
            except: 
                df["postal_code"] = ""
            try: 
                df["locality"] = address_components["long_name"][
                    address_components["types"].loc[lambda x: x == "locality"].index
                ].item()
            except: 
                df["locality"] = ""
            try:
                df["country"] = address_components["long_name"][
                    address_components["types"].loc[lambda x: x == "country"].index
                ].item()
            except: 
                df["country"] = ""
            try: 
                df["country_code"] = address_components["short_name"][
                    address_components["types"].loc[lambda x: x == "country"].index
                ].item()
            except: 
                df["country_code"] = ""
            df.drop(labels="address_components", axis=1, inplace=True)
        return df

    def nearby_search(
        self, locations: list = [], extra_details: bool = False
    ) -> pd.DataFrame:
        payload = {}
        headers = {}
        results = []
        for location in tqdm(locations):
            print(f"Retrieving population at location: {location}")
            pagetoken = ""
            location = urllib.parse.quote(location)
            page = 1
            while pagetoken is not None and page <= 3:
                print(f"Retrieving results from page {page}")
                url = f"https://maps.googleapis.com/maps/api/place/nearbysearch/json?location={location}&radius={self.radius}&type={self.type_}&keyword={self.keyword}&rankby={self.rankby}&pagetoken={pagetoken}&key={self.api_key}"
                response = requests.request("GET", url, headers=headers, data=payload)
                time.sleep(2)
                response_json = json.loads(response.text)
                pagetoken = response_json.get("next_page_token", None)
                if response_json["results"]:
                    df = pd.json_normalize(response_json["results"], max_level=2)
                    df = df.query("business_status == 'OPERATIONAL'")
                    df = df[
                        [
                            "place_id",
                            "name",
                            "vicinity",
                            "geometry.location.lat",
                            "geometry.location.lng",
                            "types",
                            "rating",
                            "user_ratings_total",
                        ]
                    ]
                    results.append(df)
                    page += 1
        df = pd.concat(results, ignore_index=True)
        df = df.loc[df.astype(str).drop_duplicates(subset="place_id").index]
        df.reset_index(drop=True, inplace=True)
        if extra_details:
            temp = []
            for index, row in df.iterrows():
                s = self._get_place_details(row["place_id"])
                temp.append(s)
            temp_df = pd.concat(temp)
            population = df.merge(temp_df, how="inner", on="place_id")
            return population
        else:
            return df

    def random_sample(self, population: pd.DataFrame, n: int = None, *args, **kwargs) -> pd.DataFrame:
        return population.sample(n, *args, **kwargs)


    def stratified_sample(self, population: pd.DataFrame, n:int, columns: list, *args, **kwargs) -> pd.DataFrame:
        ss = population.groupby(
            by=columns, group_keys=True
        ).apply(lambda x: x.sample(n, *args, **kwargs))
        ss.reset_index(drop=True)
        return ss 

    def map(
        self,
        datasets: list,
        colors: list,
        icons: list,
        location: list = [41.38, 2.16],
        map_tiles: list = ["Google Maps"],
        zoom_start: int = 15,
        *args,
        **kwargs,
    ):
        m = folium.Map(location=location, zoom_start=zoom_start, *args, **kwargs)
        for tile_layer in map_tiles:
            basemaps[tile_layer].add_to(m)
        m.add_child(folium.LayerControl())
        plugins.Fullscreen().add_to(m)
        for df, c, icon in zip(datasets, colors, icons):
            for i in range(len(df)):
                folium.Marker(
                    location=[
                        df.iloc[i]["geometry.location.lat"],
                        df.iloc[i]["geometry.location.lng"],
                    ],
                    popup=df.iloc[i]["name"],
                    icon=folium.Icon(color=c, prefix="fa", icon=icon),
                ).add_to(m)
        self.m = m
        return m
