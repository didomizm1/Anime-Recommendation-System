#!/usr/bin/env python
# Reduce implementation with K-means clustering

# Import Hadoop Streaming API
import sys
# Import csv writer class
import csv
# Generate new centroids
def new_centroids():
    index_sums=[]
    count=0

    for line in sys.stdin:
        #Parse output from mapper
        for x in range(43):
            cluster_number, user_ratings[x+1] = line.split(",")

        try:
            for x in range(42):
                user_ratings=float(user_ratings[x])
            
        except(Exception,):
            continue

            for x in range(42):
                sum_index+=user_ratings[x]
                count+=1
            #Calculating new means to get the new centroid
            new_center=(sum_index/count)

        #Name of csv file
        filepath = "../../../data/centroids/centroids.csv"
        #Writing to csv file
        with open(filepath, 'w') as csvfile: 
            # Creating a csv writer object 
            csvwriter = csv.writer(csvfile) 
            # Writing the new centroid
            csvwriter.writerows( )

# Call functions in order to print final output
if __name__ == "__main__":
    new_centroids()
