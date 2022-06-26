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
        language: str = "en",
        location: str = "",
        maxprice: str = "",
        minprice: str = "",
        opennow: str = "",
        radius: str = "",
        region: str = "",
        type_: str = "",
    ):
        self.language = language
        self.location = location
        self.maxprice = maxprice
        self.minprice = minprice
        self.opennow = opennow
        self.radius = radius
        self.region = region
        self.type_ = type_

    def _get_place_details(self, place_id: str) -> pd.Series:
        payload = {}
        headers = {}
        url = f"https://maps.googleapis.com/maps/api/place/details/json?place_id={place_id}&key={API_KEY}"
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

    def text_search(
        self, queries: list = [], extra_details: bool = False
    ) -> pd.DataFrame:
        results = []
        payload = {}
        headers = {}
        for query in tqdm(queries):
            print(f"Retrieving population for query: {query}")
            pagetoken = ""
            query = urllib.parse.quote(query)
            while pagetoken is not None:
                url = f"https://maps.googleapis.com/maps/api/place/textsearch/json?query={query}&key={API_KEY}&language={self.language}&location={self.location}&maxprice={self.maxprice}&minprice={self.minprice}&opennow={self.opennow}&radius={self.radius}&region={self.region}&type={self.type_}&pagetoken={pagetoken}"
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
                            "formatted_address",
                            "geometry.location.lat",
                            "geometry.location.lng",
                            "types",
                            "rating",
                            "user_ratings_total",
                        ]
                    ]
                    results.append(df)
        df = pd.concat(results, ignore_index=True)
        df = df.loc[df.astype(str).drop_duplicates().index]
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
        location: list = [41.38, 2.16],
        data: pd.DataFrame = None, 
        map_tiles: list = ['Google Maps'],
        *args,
        **kwargs,
    ):
        m = folium.Map(location=location, *args, **kwargs)
        for tile_layer in map_tiles:
            basemaps[tile_layer].add_to(m)
        m.add_child(folium.LayerControl())
        plugins.Fullscreen().add_to(m)
        for i in range(len(data)):
            folium.Marker(
                location=[data.iloc[i]["geometry.location.lat"], data.iloc[i]["geometry.location.lng"]],
                popup=data.iloc[i]["name"],
                icon=folium.Icon(color="red"),
            ).add_to(m)
        self.m = m
        return m
