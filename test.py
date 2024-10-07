import folium
import openrouteservice as ors
#from geopy.distance import geodesic

# Define coordinates
Qalyubia_coordinates = (30.41, 31.21)
Cairo_coordinates = (30.04388051, 31.23528921)
API_key = "5b3ce3597851110001cf62484567d5264cf74c029b1f8aef089393bc"

# Initialize the ORS client
client = ors.Client(key=API_key)

# Create a map centered around the midpoint of the two points
midpoint_coords = [
    (Qalyubia_coordinates[0] + Cairo_coordinates[0]) / 2,
    (Qalyubia_coordinates[1] + Cairo_coordinates[1]) / 2,
]
map_route = folium.Map(location=midpoint_coords, zoom_start=12)

# Add a marker for the starting point
folium.Marker(Qalyubia_coordinates, popup="Start Point").add_to(map_route)

# Add a marker for the ending point
folium.Marker(Cairo_coordinates, popup="End Point").add_to(map_route)

# Get the route coordinates
directions_coordinates = [
    tuple(reversed(Qalyubia_coordinates)),
    tuple(reversed(Cairo_coordinates)),
]
route = client.directions(
    coordinates=directions_coordinates, profile="driving-car", format="geojson"
)

# Extracting coordinates from the route
route_coords = route["features"][0]["geometry"]["coordinates"]

# Convert coordinates to Folium format (latitude, longitude)
route_coords = [tuple(reversed(coord)) for coord in route_coords]

# Draw the route path on the map
folium.PolyLine(
    locations=route_coords,
    color="red",
    weight=4,
    tooltip="Route Path",
).add_to(map_route)

# Save the map as an HTML file
map_route.save("route_map111.html")

# Access and print specific coordinates along the route
for idx, coord in enumerate(route_coords):
    if idx % 10 == 0:  # Example: get every 10th coordinate
        print(f"Point {idx}: {coord}")