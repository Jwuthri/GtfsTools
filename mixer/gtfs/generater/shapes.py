"""Generate the file shapes.txt."""
import networkx as nx
import multiprocessing as mp
from hashlib import sha1
from tqdm import tqdm

import pandas as pd

from mixer.gtfs.reader.controller import Controller


class EstimateShapes(object):
    """Gen or fill shapes."""

    def __init__(self, gtfs_path):
        """Constructor.

        :params gtfs_path: path of the gtfs zip
        """
        self.gtfs_path = gtfs_path
        self.dict_gtfs = Controller(gtfs_path).main()
        self.routes = self.dict_gtfs["routes.txt"]
        self.stoptimes = self.dict_gtfs["stop_times.txt"]
        self.stoptimes["trip_id"] = self.stoptimes["trip_id"].str.lower()
        self.stops = self.dict_gtfs["stops.txt"]

    def explore_digraph_from(self, G, source, path):
        """Depth graph."""
        if len(path) > 1:
            for np in path[:-1]:
                if source in G.successors(np):
                    G.remove_edge(np, source)
        path.append(source)
        for ns in G.successors(source):
            if ns not in path:
                self.explore_digraph_from(G, ns, path[:])

    def simplify_digraph(self, G):
        """Remove shortpath."""
        starts = []
        for node in G.nodes():
            if not G.predecessors(node):
                starts.append(node)
        if not starts:
            for node in G.nodes():
                if 1 == len(G.predecessors(node)):
                    starts.append(node)
        for s in starts:
            self.explore_digraph_from(G, s, [])

    def select_trip(self, route_id, direction):
        """Select trips for a route/direction."""
        query = "route_id == '{}' & direction_id == {}"
        cquery = query.format(route_id, direction)

        return self.trips.query(cquery)

    def select_stimes(self, trip):
        """Select stimes for a trip."""
        stoptimes = self.stoptimes[
            self.stoptimes["trip_id"] == trip["trip_id"]]

        return stoptimes.sort_values(by="stop_sequence")

    def build_digraph(self, route_id, direction):
        """Gen the Graph of a route/direction."""
        G = nx.DiGraph()
        trips = self.select_trip(route_id, direction)
        for idx, trip in trips.iterrows():
            previous = None
            stoptimes = self.select_stimes(trip)
            for idx, stoptime in stoptimes.iterrows():
                current = stoptime["stop_id"]
                if current not in G.nodes():
                    G.add_node(current)
                if previous:
                    if (previous, current) not in G.edges():
                        G.add_edge(previous, current)
                previous = current

        return G

    def get_shortest_path(self, path, G, stop):
        """Gen the shortest path btw 2 pts."""
        try:
            if len(path) == 1:
                shortest_path = nx.shortest_path(G, path[0], stop)
            else:
                shortest_path = nx.shortest_path(G, path[-1][-1], stop)
        except:
            shortest_path = [path[-1][-1], stop]

        return shortest_path

    def gen_path(self, dshapes, seq, G, key):
        """Gen the path for a trip."""
        path = list()
        path.append([seq[0]])
        for stop in seq[1:]:
            shortest_path = self.get_shortest_path(path, G, stop)
            path.append(shortest_path)
            dshapes[key] = path

        return dshapes

    def gen_trip_shape(self, G, dshapes, dtrips, trip, stoptimes):
        """Gen a shapes for a trip."""
        stoptimes = stoptimes[stoptimes["trip_id"] == trip]
        seq = list(stoptimes["stop_id"])
        key = sha1(str(seq).encode("utf8")).hexdigest()
        dtrips[trip] = key
        if key not in dshapes.keys():
            dshapes = self.gen_path(dshapes, seq, G, key)

        return dshapes, dtrips

    def gen_trips_shapes(self, G, lst_trips, stoptimes):
        """Gen all shapes for all trips."""
        dict_shapes = dict()
        dict_trips = dict()
        for trip in lst_trips:
            dict_shapes, dict_trips = self.gen_trip_shape(
                G, dict_shapes, dict_trips, trip, stoptimes)

        return dict_shapes, dict_trips

    def gen_shapes_df(self, key, val):
        """Gen the df from value of dict."""
        shapes = sum(val, [])
        shapes = pd.DataFrame({"stop_id": shapes})
        cols = ["stop_lat", "stop_lon"]
        shapes = pd.merge(shapes, self.stops, on="stop_id")[cols]
        shapes = shapes.rename(columns={
            "stop_lat": "shape_pt_lat", "stop_lon": "shape_pt_lon"
        })
        shapes["shape_id"] = key
        shapes["shape_pt_sequence"] = shapes.index

        return shapes

    def prepare_shapes(self, dict_shapes, dict_trips):
        """Transform dict to df."""
        shapes_df = pd.DataFrame()
        for key, val in dict_shapes.items():
            shapes = self.gen_shapes_df(key, val)
            shapes_df = pd.concat([shapes_df, shapes])
        trips_df = pd.DataFrame.from_dict([dict_trips]).T
        trips_df["trip_id"] = trips_df.index
        trips_df.columns = ["shape_id", "trip_id"]
        trips_df = trips_df.reset_index(drop=True)

        return shapes_df, trips_df

    def gen_route_dir(self, route_id, direction):
        """Gen the shapes for a route/direction."""
        sub_col = ["route_id", "trip_id", "direction_id"]
        stoptimes = pd.merge(self.stoptimes, self.trips[sub_col], on="trip_id")
        G = self.build_digraph(route_id, direction)
        self.simplify_digraph(G)
        query = "route_id == '{}' & direction_id == {}"
        cquery = query.format(route_id, direction)
        trip_route_dir = stoptimes.query(cquery)
        lst_trips = list(trip_route_dir['trip_id'].unique())
        dict_shapes, dict_trips = self.gen_trips_shapes(
            G, lst_trips, stoptimes)

        return self.prepare_shapes(dict_shapes, dict_trips)

    def mp_r_d(self, args):
        """Multiprocess the gen_route_dir."""
        return self.gen_route_dir(*args)

    def gen_data(self):
        """Gen the l_args for mp."""
        lst_routes = list(self.trips["route_id"].unique())
        lst_dir = [0, 1]

        return [
            (route, direction)
            for route in lst_routes
            for direction in lst_dir
        ]

    def remove_shape_id(self, df):
        """Remove the shape_id in trips if doesn t exist."""
        try:
            df = df.drop("shape_id", 1)
        except:
            pass

        return df

    def export_shapes(self, res):
        """Create the shapes.txt and add the new shapes id to trips."""
        shapes_df = pd.DataFrame()
        trips_df = pd.DataFrame()
        for df in res:
            shapes_df = pd.concat([shapes_df, df[0]])
            trips_df = pd.concat([trips_df, df[1]])
        shapes_df["shape_dist_traveled"] = None
        trips_df = trips_df[["trip_id", "shape_id"]]
        trips = self.remove_shape_id(self.trips)
        trips = pd.merge(trips, trips_df, on="trip_id")

        return shapes_df, trips

    def main(self, trips):
        """Build all digraph."""
        self.trips = trips
        l_args = self.gen_data()
        res = []
        pool = mp.Pool()
        ln = len(l_args)
        # res = pool.map(self.mp_r_d, l_args)
        for shape in tqdm(pool.imap_unordered(self.mp_r_d, l_args), total=ln):
            res.append(shape)
        pool.close()

        return self.export_shapes(res)
