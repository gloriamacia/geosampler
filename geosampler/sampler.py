__all__ = ['Sampler']

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
        radius: str = "",
        region: str = "",
        type_: str = "",
        rankby: str = "distance"
    ):
        self.api_key = api_key
        self.language = language
        self.keyword = keyword
        self.maxprice = maxprice
        self.minprice = minprice
        self.opennow = opennow
        self.radius = radius
        self.region = region
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
            df["postal_code"] = address_components["long_name"][
                address_components["types"].loc[lambda x: x == "postal_code"].index
            ].item()
            df["locality"] = address_components["long_name"][
                address_components["types"].loc[lambda x: x == "locality"].index
            ].item()
            df["country"] = address_components["long_name"][
                address_components["types"].loc[lambda x: x == "country"].index
            ].item()
            df["country_code"] = address_components["short_name"][
                address_components["types"].loc[lambda x: x == "country"].index
            ].item()
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
            while pagetoken is not None and page <=3:
                print(f"page {page}")
                url = f"https://maps.googleapis.com/maps/api/place/nearbysearch/json?location={location}&type={self.type_}&rankby=distance&pagetoken={pagetoken}&key={self.api_key}"
                response = requests.request("GET", url, headers=headers, data=payload)
                time.sleep(3)
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
        df = df.loc[df.astype(str).drop_duplicates(subset='place_id').index]
        df.reset_index(drop=True, inplace=True)
        if extra_details:
            temp = []
            for index, row in df.iterrows():
                s = self._get_place_details(row["place_id"])
                temp.append(s)
            temp_df = pd.concat(temp)
            population = df.merge(temp_df, how="inner", on="place_id")
            self.population = population
            return population
        else:
            self.population = df
            return df

    def random_sample(self, *args, **kwargs) -> pd.DataFrame:
        self.random_sample = self.population.sample(*args, **kwargs)
        return self.random_sample

    def stratified_sample(self, columns: list = [], *args, **kwargs) -> pd.DataFrame:
        self.stratified_sample = self.population.groupby(
           by=columns, group_keys=True
        ).apply(lambda x: x.sample(*args, **kwargs))
        return self.stratified_sample

    def map(
        self,
        datasets: list, 
        colors: list,
        icons: list,
        location: list = [41.38, 2.16],
        map_tiles: list = ['Google Maps'],
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
                    location=[df.iloc[i]["geometry.location.lat"], df.iloc[i]["geometry.location.lng"]],
                    popup=df.iloc[i]["name"],
                    icon=folium.Icon(color=c, prefix='fa', icon=icon),
                ).add_to(m)
        self.m = m
        return m
