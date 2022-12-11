#!/usr/bin/env python
# Reduce implementation with K-means clustering

# Import Hadoop Streaming API
import sys


# Generate new centroids
def new_centroids():
    # Holds information which will be used to calculate the new centroid for each cluster;
    # Each key is a cluster's number and each value is a dictionary of lists of that cluster's info
    clusters_info_list = {}

    # Labels to be printed to csv file later
    labels = ""

    # Iterate through lines received from mapper
    for line in sys.stdin:
        # Parse output from mapper
        line_components = line.split(",")

        # Line is first line with labels
        if line_components[0] == "cluster_number":
            # Remove unneeded column labels and skip to next iteration
            labels = line.replace("cluster_number,user_id,", "").strip('\n')
            continue

        cluster_number = line_components[0]

        # If cluster number wasn't already encountered, add data structures for it
        if cluster_number not in clusters_info_list.keys():
            # Holds sum of ratings for each genre in a cluster
            genre_rating_sums = [0] * 42
            # Holds the count of ratings for each genre in a cluster
            genre_rating_counts = [0] * 42
            # Adds new entry for this newly found cluster
            clusters_info_list[cluster_number] = {
                "genre_rating_sums": genre_rating_sums,
                "genre_rating_counts": genre_rating_counts
            }

        # Iterate through each piece of a line to find & operate on genre rating values
        for x in range(44):
            # Ignore cluster number (index 0) and user ID (index 1)
            if x > 1:
                # Skip over incrementation for NaN values
                try:
                    # Add current user's genre rating to cluster's current total
                    clusters_info_list[cluster_number]['genre_rating_sums'][x-2] += float(line_components[x])
                    # Increment count of users in a cluster encountered who have a rating for current genre
                    clusters_info_list[cluster_number]['genre_rating_counts'][x-2] += 1
                except (Exception,):
                    continue

    # Name of csv file to write centroids to for further iterations (output is also printed to HDFS)
    initial_stdout = sys.stdout
    filepath = "C:/Users/alpgr/Desktop/Anime-Recommendation-System/data/centroids/centroids.csv"
    with open(filepath, "w") as new_centroids_output:
        # Print labels
        print(labels)

        # Print labels to csv file
        sys.stdout = new_centroids_output
        print(labels)
        sys.stdout = initial_stdout

        # Total number of clusters
        num_of_clusters = len(clusters_info_list)

        # Iterate through info gathered for each cluster in order to calculate mean point (new centroid)
        for cluster_num in range(num_of_clusters):
            # Holds the new coordinates of the current cluster's new centroid
            new_centroid_coordinates = []

            # Iterate through each genre to calculate the new mean coordinate
            for i in range(42):
                # Append NaN value if genre ratings count is 0 (divide by 0 error triggers this)
                try:
                    # Calculating cluster means to get new coordinates for the new centroid
                    current_genre_sum = clusters_info_list[str(cluster_num)]['genre_rating_sums'][i]
                    current_genre_count = clusters_info_list[str(cluster_num)]['genre_rating_counts'][i]
                    new_centroid_coordinates.append(current_genre_sum/current_genre_count)
                except (Exception,):
                    new_centroid_coordinates.append('')

            # Iterate through coordinates of new centroid in order to print the full centroid
            string_to_print = ""
            for j in range(42):
                # On last iteration, don't print comma
                if j == 41:
                    string_to_print += str(new_centroid_coordinates[j])
                else:
                    string_to_print += str(new_centroid_coordinates[j]) + ","

            # Print new centroids (output of reducer to HDFS)
            print(string_to_print)

            # Writing to csv file
            sys.stdout = new_centroids_output
            print(string_to_print)
            sys.stdout = initial_stdout

    new_centroids_output.close()


# Call functions in order to print final output
if __name__ == "__main__":
    new_centroids()
