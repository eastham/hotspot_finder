"""Input a CSV of the form:
airport,time,dist, f1tail, f1alt, f1lat, f1long, f1track, f2tail, f2alt, f2lat, f2long, f2track
build a dataframe and render it with a folium heatmap
"""

import numpy as np
from folium.plugins import HeatMap
import folium
import pandas as pd
import sys

def get_flight_str(df, index, f2=False):
    f_num = "f1" if not f2 else "f2"
    f_str = f"{df.at[index, f_num + 'tail']}: {df.at[index, f_num + 'alt']} MSL {df.at[index, f_num + 'track']} deg"
    
    return f_str

def get_link(df, index):
    # return link with url of the form https://globe.adsbexchange.com/?replay=2024-06-06-18:32&lat=36.228&lon=-121.123&zoom=10.8

    f1lat = df.at[index, "f1lat"]
    f1lon = df.at[index, "f1lon"]
    ts = df.at[index, "time"]
    # convert ts in epoch seconds to string of the form 2024-06-06-18:32
    ts_str = pd.to_datetime(ts, unit='s').strftime('%Y-%m-%d-%H:%M')
    url = f"https://globe.adsbexchange.com/?replay={ts_str}&lat={f1lat}&lon={f1lon}&zoom=14"
    link = f'<a target="adsx" href="{url}">link</a>'
    return link

def main():
    #     map_airport = ["wvi", 36.9357325, -121.7896375]
    map_airport = ["o69", 38.2577933, -122.6053236]

    if len(sys.argv) != 2:
        print(f"Usage: {sys.argv[0]} <input-file>")
        sys.exit(1)

    input_file = sys.argv[1]

    df = pd.read_csv(input_file)
    df = df[df["airport"] == map_airport[0]]
    print(df)
    #print(df.T)
    folium_df = df[["f1lat", "f1lon", "f1alt"]]
    folium_df.columns = ["lat", "lon", "alt"]
    print(folium_df)

    # filter out rows where f1alt is < 4000
    folium_df = folium_df[folium_df["alt"] < 4000]

    # filter out rows where altitude difference between f1alt and f2alt is < 800
    folium_df = folium_df[abs(df["f1alt"] - df["f2alt"]) <= 400]

    # filter out rows where distance between f1 and f2 is < .3 nm
    folium_df = folium_df[df["dist"] <= .3]

    #print how many rows in folium_df
    print(f"** Number of rows after prox : {len(folium_df)}")
    # filter out rows with tail starting with N0000 or N/A
    folium_df = folium_df.dropna()

    print(f"** Number of rows after n/a filter : {len(folium_df)}")

    print(folium_df)

    m = folium.Map(location=[map_airport[1], map_airport[2]], zoom_start=14)
    HeatMap(folium_df).add_to(m)

    markers_fg = folium.FeatureGroup(name='markers')

    # add points for each row
    for index, row in folium_df.iterrows():
        marker = folium.Marker([row["lat"], row["lon"]], radius=300)
        marker.add_to(markers_fg)
        # add tooltip with altitude
        link_str = get_link(df, index)
        f1str = get_flight_str(df, index)
        f2str = get_flight_str(df, index, f2=True)
        # make marker clickable to the link
        marker.add_child(folium.Popup(f"{link_str} {f1str} === {f2str}"))

#        marker.add_child(folium.Tooltip(f"{link_str} {f1str} === {f2str}"))
#        marker.add_to(m)

    markers_fg.add_to(m)

    # do another heatmap for only points where heading differs by more than 45 degrees
    heading_df = pd.DataFrame(
        None, columns=["lat", "lon"])

    for index, row in folium_df.iterrows():
        # this heading math is not right:
        if abs(df.at[index, "f1track"] - df.at[index, "f2track"]) > 45:
            new_df = pd.DataFrame([[row["lat"], row["lon"]]], columns=["lat", "lon"])
            heading_df = pd.concat([heading_df, new_df], ignore_index=True)
    HeatMap(heading_df).add_to(m)

    # add layer control
    folium.LayerControl().add_to(m)

    m.save(f"map_{map_airport[0]}.html")

main()