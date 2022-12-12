# Takes test user input and gives series recommendations based upon final clusters and centroids
import pandas as pd
import sys
import random
from math import sqrt


# Recommendations function
def find_recommendations():
    # Holds parsed test user input values
    test_user_info = []

    # Open and parse input file
    filepath = '../../../data/recommendation/test_user_average_ratings.csv'
    with open(filepath, "r") as file:
        # Skip first line
        next(file)
        for line in file:
            coordinates = line.split(",")  # Coordinates of user data
            # Iterate through coordinates of test user input data
            for x in range(43):
                user_id = current_coordinate = coordinates[x]
                if x == 0:  # User ID is being examined
                    test_user_info.append(int(user_id))
                else:  # Coordinate is being examined
                    # If coordinate isn't NaN value, add to user info as a float, otherwise add NaN value itself
                    try:
                        test_user_info.append(float(current_coordinate))
                    except (Exception,):
                        test_user_info.append('')
    file.close()

    # Holds parsed centroid values
    centroids_list = []

    # Open and parse centroids file
    filepath = '../../../data/centroids/centroids.csv'
    with open(filepath, "r") as file:
        # Skip first line
        next(file)
        for line in file:
            centroid = []  # Holds values for a single centroid
            coordinates = line.split(",")  # Coordinates of centroid

            # Iterate through coordinates of a centroid
            for x in range(42):
                # If coordinate is not an NaN value, add to centroid as a float, otherwise add NaN value itself
                current_coordinate = coordinates[x]
                try:
                    centroid.append(float(current_coordinate))
                except (Exception,):
                    centroid.append('')

            # Add centroid to centroids list
            centroids_list.append(centroid)
    file.close()

    # Find which centroid is the closest to the user point
    cluster_number = -1  # Keeps track of the cluster number the user is found to be part of
    least_distance = -1  # Smallest distance so far found between a centroid and user rating datapoint

    # Euclidian distance, calculates the distance between the point and each centroid
    for centroid in centroids_list:

        distance_sum = 0  # Sum portion of Euclidian distance which goes inside the square root

        # Add to the sum only for dimensions (genres) that the centroid and datapoint have in common
        # i.e., ignore NaN values for both the centroid and datapoint when finding a distance
        for x in range(42):
            try:
                distance_sum += pow(centroid[x] - test_user_info[x+1], 2)
            except (Exception,):
                continue

        # Get final distance by taking the square root of the sum
        current_distance = sqrt(distance_sum)

        # Set closest centroid on first iteration or if lesser distance was found
        if current_distance < least_distance or least_distance == -1:
            least_distance = current_distance
            cluster_number += 1  # Clusters will be numbered starting with 0

    # List to hold user ID's of users who are in the same cluster as the test user
    users_in_cluster = []

    # Open and parse clusters file
    filepath = '../../../data/clusters/clusters.csv'
    with open(filepath, "r") as file:
        # Skip first line
        next(file)

        # Iterate through each user in the clusters file
        for line in file:
            # Split line and extract cluster number and user ID
            line_components = line.split(",")
            file_user_cluster_number = line_components[0]
            user_id = line_components[1]

            # User ID saved only if they are in the same cluster as the test user
            if file_user_cluster_number == str(cluster_number):
                users_in_cluster.append(user_id)

    file.close()

    # Now that other users in the cluster have been found, pick a random user from the cluster
    chosen_user_id = random.choice(users_in_cluster)

    # List that holds ID's of series to recommend
    series_to_recommend = []

    # Open and parse user ratings file
    filepath = '../../../data/sample/user_ratings_sample.csv'
    with open(filepath, "r") as file:
        # Skip first line
        next(file)

        for line in file:
            # Split line and extract user ID, anime ID, and rating
            line_components = line.split(",")
            current_line_user_id = line_components[0]
            current_line_anime_id = line_components[1]
            current_line_rating = line_components[2]

            # Add series ID to list if user ID matches and if rating is high enough
            if current_line_user_id == chosen_user_id and int(current_line_rating) >= 7:
                series_to_recommend.append(current_line_anime_id)

    file.close()

    # Open output file to write to
    filepath = '../../../data/recommendation/recommendations.txt'
    with open(filepath, "w") as file:
        # Print info about test user and chosen similar user
        sys.stdout = file
        print("Input user ID: " + str(test_user_info[0]))
        print("Input user cluster number: " + str(cluster_number))
        print("Chosen similar user ID: " + chosen_user_id)
        print("\nRecommendations:")

        # Create DataFrame of anime series
        anime_list_df = pd.read_csv('../../../data/cleaned/anime_list.csv')

        # Find and print names of series associated with the series ID's in the series ID's list
        for series_id in series_to_recommend:
            series_id_int = int(series_id)
            current_name = anime_list_df.loc[anime_list_df['MAL_ID'] == series_id_int]['English name']
            print(current_name.values[0])

    file.close()


if __name__ == '__main__':
    find_recommendations()
