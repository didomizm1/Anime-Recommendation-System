#!/usr/bin/env python
# Map implementation with K-Means clustering

# Import Hadoop Streaming API
import sys

from math import sqrt

# Choose initial centroids
def initial_centroids(filepath):
    centroids=[]
    with open(filepath) as fp:
        line=fp.readline()
        while line:
            if line:
                try:
                    line=line.strip()
                    coordinate=line.split(',')
                    for x in range(42):
                        centroids.append([float(coordinate[x)])
                except:
                    break
            else:
                break
            line=fp.readline()
    fp.close()
    return centroids

# Generate clusters based on chosen centroids
def generate_clusters(centroids):
    for line in sys.stdin:
        line=line.strip()
        coordinate=line.split(',')
        min_distance=10000000000
        index=0
        #Keeps track of the centroid number for calculating the distances
        centroid_number=0
        for centroid in centroids:
            try:
                for x in range(42):
                    coordinate[x]=float(coordinate[x])
            except ValueError:
                continue
            # Euclidian distance, calculates the distance between the point and each centroid
            distance=0
            for x in range(42):
                if np.isnan(coordinate[x]) #Checking if coordinate is a NaN value
                    distance=distance
                else
                    distance+=(pow(coordinate[x]-centroid[centroid_number],2))

            current_distance=sqrt(distance)
            # Find the centroid closer to the point
            if current_distance <= min_distance:
                min_distance=current_distance
                index=centroids.index(centroid)
            centroid_number+=1
        print(index)
        for x in range(42):
            print(", " + coordinate[x])
            
# Call functions in order to print data for reducer
if __name__ == '__main__':
    centroids=initial_centroids('../../../data/centroids/initial_centroids.csv') 
    generate_clusters(centroids)
