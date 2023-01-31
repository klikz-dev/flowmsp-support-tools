#!/usr/bin/env python3

import math
import logging

logging.basicConfig()
logger = logging.getLogger("geo_location")


"""
 * <p>Represents a point on the surface of a sphere. (The Earth is almost
 * spherical.)</p>
 *
 * <p>To create an instance, call one of the static methods fromDegrees() or
 * fromRadians().</p>
 *
 * <p>This code was originally published at
 * <a href="http://JanMatuschek.de/LatitudeLongitudeBoundingCoordinates#Java">
 * http://JanMatuschek.de/LatitudeLongitudeBoundingCoordinates#Java</a>.</p>
 *
 * @author Jan Philip Matuschek
 * @version 22 September 2010
"""
class GeoLocation:
    radLat = None # latitude in radians
    radLon = None # longitude in radians
    degLat = None # latitude in degrees
    degLon = None # longitude in degrees

    MIN_LAT = math.radians(-90)  # -PI/2
    MAX_LAT = math.radians(90)   #  PI/2
    MIN_LON = math.radians(-180) # -PI
    MAX_LON = math.radians(180)  #  PI

    def fromDegrees(self, latitude, longitude):
        """
        * @param latitude the latitude, in degrees.
        * @param longitude the longitude, in degrees.
        """
        result = GeoLocation()

        result.radLat = math.radians(latitude)
        result.radLon = math.radians(longitude)
        result.degLat = latitude
        result.degLon = longitude

        result.checkBounds()
        return result

    def fromRadians(self, latitude, longitude):
        """
        * @param latitude the latitude, in radians.
        * @param longitude the longitude, in radians.
        """
        result = GeoLocation()
        result.radLat = latitude
        result.radLon = longitude
        result.degLat = math.degrees(latitude)
        result.degLon = math.degrees(longitude)
        result.checkBounds()

        return result;


    def checkBounds(self):
        if (self.radLat < self.MIN_LAT or self.radLat > self.MAX_LAT or
            self.radLon < self.MIN_LON or self.radLon > self.MAX_LON):
            raise "out of bounds"

    def getLatitudeInDegrees(self):
        "return the latitude, in degrees"
        return self.degLat

    def getLongitudeInDegrees(self):
        "return the longitude, in degrees."
        return self.degLon

    def getLatitudeInRadians(self):
        "return the latitude, in radians."
        return self.radLat

    def getLongitudeInRadians(self):
        "return the longitude, in radians."
        return self.radLon

    def __repr__(self):
        return "(%s\u00b0, %s\u00b0) = (%s rad, %s rad)" % \
            (self.degLat, self.degLon, self.radLat, self.radLon)

    def distanceTo (location, radius):
        """
        * Computes the great circle distance between this GeoLocation instance
        * and the location argument.
        * @param radius the radius of the sphere, e.g. the average radius for a
        * spherical approximation of the figure of the Earth is approximately
        * 6371.01 kilometers.
        * @return the distance, measured in the same unit as the radius
        * argument.
        """
        return (math.acos(math.sin(radLat) * math.sin(location.radLat) +
                          math.cos(radLat) * math.cos(location.radLat) *
                          math.cos(radLon - location.radLon)) * radius)


    def boundingCoordinates(self, distance, radius):
        """
        /**
        * <p>Computes the bounding coordinates of all points on the surface
        * of a sphere that have a great circle distance to the point represented
        * by this GeoLocation instance that is less or equal to the distance
        * argument.</p>
        * <p>For more information about the formulae used in this method visit
        * <a href="http://JanMatuschek.de/LatitudeLongitudeBoundingCoordinates">
        * http://JanMatuschek.de/LatitudeLongitudeBoundingCoordinates</a>.</p>
        * @param distance the distance from the point represented by this
        * GeoLocation instance. Must me measured in the same unit as the radius
        * argument.
        * @param radius the radius of the sphere, e.g. the average radius for a
        * spherical approximation of the figure of the Earth is approximately
        * 6371.01 kilometers.
        * @return an array of two GeoLocation objects such that:<ul>
        * <li>The latitude of any point within the specified distance is greater
        * or equal to the latitude of the first array element and smaller or
        * equal to the latitude of the second array element.</li>
        * <li>If the longitude of the first array element is smaller or equal to
        * the longitude of the second element, then
        * the longitude of any point within the specified distance is greater
        * or equal to the longitude of the first array element and smaller or
        * equal to the longitude of the second array element.</li>
        * <li>If the longitude of the first array element is greater than the
        * longitude of the second element (this is the case if the 180th
        * meridian is within the distance), then
        * the longitude of any point within the specified distance is greater
        * or equal to the longitude of the first array element
        * <strong>or</strong> smaller or equal to the longitude of the second
        * array element.</li>
        * </ul>
        */
        """
        if (radius < 0 or distance < 0):
            raise "error"

        # angular distance in radians on a great circle
        radDist = distance / radius

        minLat = self.radLat - radDist
        maxLat = self.radLat + radDist

        if (minLat > self.MIN_LAT and maxLat < self.MAX_LAT):
            deltaLon = math.asin(math.sin(radDist) / math.cos(self.radLat))
                
            minLon = self.radLon - deltaLon

            if (minLon < self.MIN_LON):
                minLon += 2 * math.PI

            maxLon = self.radLon + deltaLon
                    
            if (maxLon > self.MAX_LON):
                maxLon -= 2 * math.PI
                        
        else:
            # a pole is within the distance
            minLat = max(minLat, self.MIN_LAT)
            maxLat = min(maxLat, self.MAX_LAT)
            minLon = self.MIN_LON
            maxLon = self.MAX_LON

        mn = self.fromRadians(minLat, minLon)
        mx = self.fromRadians(maxLat, maxLon)

        return mn, mx


if __name__ == "__main__":
    print("hello")
