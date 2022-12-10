#!/usr/bin/env python
# Reduce implementation with K-means clustering

# Import Hadoop Streaming API
import sys
# Import csv writer class
import csv
# Generate new centroids
def new_centroids():
    #Holds sum of ratings for each genre in a cluster
    index_sums=[]
    #Holds the count of ratings for each genre in a cluster
    index_count=[]
    #Holds the new data points of the new centroids
    new_points=[]

    for line in sys.stdin:
        #Parse output from mapper
        for x in range(43):
            cluster_number, user_ratings[x+1] = line.split(",")
        for x in range(42):
            try:
                user_ratings=float(user_ratings[x]) 
            except(Exception,):
            continue
        #Looping through clusters
        for cluster in cluster_number:
            for x in range(42):
                index_sum[x]+=user_ratings[x]
                index_count[x]+=1
        for x in range(42):
            #Calculating cluster means to get new data points for the new centroid
            new_points[x]=(sum_index[x]/count[x])

             #Name of csv file
            filepath = "../../../data/centroids/centroids.csv"
             #Writing to csv file
            with open(filepath, 'w') as csvfile: 
                # Creating a csv writer object 
                 csvwriter = csv.writer(csvfile) 
                # Writing the new centroid
                csvwriter.writerows(new_points[x]+ ",")
            #Print new centroids
            print(new_points[x]+ ",")
# Call functions in order to print final output
if __name__ == "__main__":
    new_centroids()
