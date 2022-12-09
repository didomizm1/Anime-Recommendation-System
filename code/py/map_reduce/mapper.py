#!/usr/bin/env python
# Map implementation with K-Means clustering

# Import Hadoop Streaming API
import sys

from math import sqrt


# Import centroids, either initial centroids on first iteration or centroids calculated by reducer on further iterations
def import_centroids(filepath):
    # Open file and create list of centroids
    centroids_list = []
    with open(filepath, "r") as file:
        # Skip first line
        next(file)

        # Iterate through each line of centroids file
        for line in file:

            centroid = []  # Centroid to be added to centroids list
            coordinates = line.split(',')  # Centroid coordinates before processing

            # Iterate through coordinates of a centroid
            for x in range(42):
                # If coordinate is not an NaN value, add to centroid as a float, otherwise add NaN value itself
                current_coordinate = coordinates[x]
                try:
                    centroid.append(float(current_coordinate))
                except (Exception,):
                    centroid.append(current_coordinate)

            # Add processed centroid to centroids list
            centroids_list.append(centroid)

    # Close file, then return list of centroids
    file.close()

    return centroids_list


# Generate clusters based on centroids
def generate_clusters(centroids_list):
    
    index = 0

    # Keeps track of the centroid number for calculating the distances
    centroid_number = 0
    for centroid in centroids_list:
        try:
            for x in range(42):
                coordinate[x] = float(coordinate[x])
        except ValueError:
            continue

        # Euclidian distance, calculates the distance between the point and each centroid
        distance = 0
        for x in range(42):
            if np.isnan(coordinate[x]):  # Checking if coordinate is a NaN value
                distance = distance
            else:
                distance += (pow(coordinate[x]-centroid[centroid_number], 2))

        current_distance = sqrt(distance)
        # Find the centroid closer to the point
        if current_distance <= min_distance:
            min_distance = current_distance
            index = centroids_list.index(centroid)
        centroid_number += 1

    print(index)

    for x in range(42):
        print(", " + coordinate[x])


# Call functions in order to print data for reducer
if __name__ == '__main__':
    centroids = import_centroids('../../../data/centroids/centroids.csv')
    generate_clusters(centroids)
