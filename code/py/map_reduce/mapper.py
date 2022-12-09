#!/usr/bin/env python
# Map implementation with K-Means clustering

# Import Hadoop Streaming API as well as square root function for distance
import sys
from math import sqrt


def import_datapoints_file(filepath, mode):
    # Open file and create list of centroids
    datapoints_list = []
    with open(filepath, "r") as file:
        # Skip first line
        next(file)

        # Iterate through each line of datapoints file
        for line in file:

            datapoint = []  # Datapoint to be added to datapoints list
            coordinates = line.split(',')  # Datapoint coordinates before processing

            # Switch statement to determine the functionality, depending on whether centroids or ratings are imported
            match mode:
                case 0:  # Import centroids; initial on first iteration, ones calculated by reducer on other iterations
                    # Iterate through coordinates of a centroid
                    for x in range(42):
                        # If coordinate is not an NaN value, add to centroid as a float, otherwise add NaN value itself
                        current_coordinate = coordinates[x]
                        try:
                            datapoint.append(float(current_coordinate))
                        except (Exception,):
                            datapoint.append('')

                case 1:  # Import and process average user ratings file
                    # Iterate through coordinates of a datapoint
                    for x in range(43):
                        user_id = current_coordinate = coordinates[x]
                        if x == 0:  # User ID is being examined
                            datapoint.append(int(user_id))
                        else:  # Coordinate is being examined
                            # If coordinate isn't NaN value, add to datapoint as a float, otherwise add NaN value itself
                            try:
                                datapoint.append(float(current_coordinate))
                            except (Exception,):
                                datapoint.append('')

            # Add processed datapoint to datapoints list
            datapoints_list.append(datapoint)

    # Close file, then return list of datapoints
    file.close()

    return datapoints_list


# Generate clusters based on centroids
def generate_clusters(centroids_list):
    # Path to average user ratings
    filepath = '../../../data/average_ratings/average_ratings.csv'

    # Save each line of average ratings as a string for later use when sending to reducer
    original_average_ratings = []
    with open(filepath, "r") as file:
        # Skip first line
        next(file)

        # Add line to list
        for line in file:
            original_average_ratings.append(line)
    file.close()

    # Get average user ratings as a processed list
    average_ratings = import_datapoints_file(filepath, 1)

    for user_rating in average_ratings:

        cluster_number = -1  # Keeps track of the cluster number for a user rating to be added to
        least_distance = -1  # Smallest distance so far found between a centroid and user rating datapoint

        # Euclidian distance, calculates the distance between the point and each centroid
        for centroid in centroids_list:

            distance_sum = 0  # Sum portion of Euclidian distance which goes inside the square root

            # Add to the sum only for dimensions (genres) that the centroid and datapoint have in common
            # i.e., ignore NaN values for both the centroid and datapoint when finding a distance
            for x in range(42):
                try:
                    distance_sum += pow(centroid[x] - user_rating[x+1], 2)
                except (Exception,):
                    continue

            # Get final distance by taking the square root of the sum
            current_distance = sqrt(distance_sum)

            # Set closest centroid on first iteration or if lesser distance was found
            if current_distance < least_distance or least_distance == -1:
                least_distance = current_distance
                cluster_number += 1  # Clusters will be numbered starting with 0
    
        # Print cluster number followed by user rating as a string from original ratings file
        rating_index = average_ratings.index(user_rating)
        print(str(cluster_number) + "," + original_average_ratings[rating_index], end="")


# Call functions in order to print data for reducer
if __name__ == '__main__':
    centroids = import_datapoints_file('../../../data/centroids/centroids.csv', 0)
    generate_clusters(centroids)
